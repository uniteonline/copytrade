"""
策略服务：打印初始持仓 + 实时消费 queue_ops
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
需在 config.yaml 中预先配置 okx 与 rabbitmq 凭证。
"""

import asyncio, json, logging, yaml, aio_pika
from functools import partial
from okx.Trade import TradeAPI
from trade import OKXTradingClient
from collections import deque
from typing import Optional
from concurrent.futures import ThreadPoolExecutor
# ------------------------------------------------------------------ #
# 读取配置
# ------------------------------------------------------------------ #
with open("config.yaml", encoding="utf-8") as f:
    cfg = yaml.safe_load(f)

MQ_URL        = cfg["rabbitmq"]["url"]
MQ_QUEUE_OPS  = cfg["rabbitmq"]["queue_ops"]
MQ_QUEUE_INFO = cfg["rabbitmq"]["queue_info"]
MQ_QUEUE_SNAP_REQ = cfg["rabbitmq"]["queue_snap_req"]   # NEW
MQ_QUEUE_STATUS = cfg["rabbitmq"].get("status_events", "status.events")  # NEW
## 新增：用于向 Indicator 请求 MONITOR_STATUS 的专用队列（默认 q.snapshot.req）
MQ_QUEUE_STATUS_REQ = cfg["rabbitmq"].get("volume_profile", "q.snapshot.req")
PREFETCH   = cfg["rabbitmq"].get("prefetch", 10)
RECENT_SEEN = int(cfg["rabbitmq"].get("recent_seen", 3000))  # 去重窗口大小

OKX_APIKEY     = cfg["okx"]["api_key"]
OKX_SECRETKEY  = cfg["okx"]["secret_key"]
OKX_PASSPHRASE = cfg["okx"]["passphrase"]
# 并发上限（可在 config.yaml 的 rabbitmq 下可选配置）
MAX_CONCURRENCY_OPS  = cfg["rabbitmq"].get("max_concurrency_ops", 64)
MAX_CONCURRENCY_INFO = cfg["rabbitmq"].get("max_concurrency_info", 16)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

# ------------------------------------------------------------------ #
#  策略类：打印持仓 + 消费 MQ
# ------------------------------------------------------------------ #
class StrategyService:
    def __init__(
        self,
        okx_client: OKXTradingClient,
        mq_url: str,
        queue_ops: str,
        queue_info: str,
        queue_snap_req: str,           # 钱包快照请求（原有）
        queue_status: str,             # MONITOR_STATUS 事件消费（原有）
        queue_status_req: str          # NEW: 向 Indicator 主动请求 MONITOR_STATUS 的专用队列
    ):
        self.okx = okx_client
        self.mq_url = mq_url
        self.queue_ops = queue_ops
        self.queue_info = queue_info
        self.queue_snap_req = queue_snap_req    # NEW
        self.queue_status = queue_status        # NEW
        self.queue_status_req = queue_status_req
        # 并发控制（ops/info 独立限流）
        self._sem_ops  = asyncio.Semaphore(MAX_CONCURRENCY_OPS)
        self._sem_info = asyncio.Semaphore(MAX_CONCURRENCY_INFO)

    async def _print_initial_positions(self):
        """启动时拉取一次【持仓】+【未成交委托】"""
        # ---------- 1) 当前持仓 ----------
        try:
            # 需要在 OKXTradingClient 中实现 get_positions()
            positions = self.okx.get_positions()
        except Exception as e:
            logging.error("Fetch positions failed: %s", e, exc_info=True)
            positions = []

        if positions:
            logging.info("=== OKX 当前持仓 ===")
            for p in positions:
                logging.info(
                    "[POS] %s  size=%s  avgPx=%s  PnL=%s  lev=%s",
                    p["instId"],
                    p.get("pos") or p.get("sz"),   # 兼容字段
                    p.get("avgPx", "-"),
                    p.get("upl",    "-"),
                    p.get("lever",  "-"),
                )
        else:
            logging.info("No active positions on OKX.")

        # ---------- 2) 未成交委托 ----------
        try:
            open_orders = self.okx.get_pending_orders()
        except Exception as e:
            logging.error("Fetch open orders failed: %s", e, exc_info=True)
            open_orders = []

        if open_orders:
            logging.info("=== OKX 未成交委托 ===")
            for o in open_orders:
                logging.info(
                    "[ORDER] %s %s  size=%s  px=%s  status=%s",
                    o["instId"],
                    o.get("side", "-"),
                    o.get("sz") or o.get("pos"),
                    o.get("px", "-"),
                    o.get("state", "-"),
                )
        else:
            logging.info("No pending orders on OKX.")

    # -----------------------------------------------
    # 去重缓存：最多保存 recent_seen 条 msg_id
    # -----------------------------------------------
    # O(1) 去重：deque + set 组合；手动驱逐最旧元素
    _seen: deque[str] = deque()
    _seen_set: set[str] = set()
    _lock = asyncio.Lock()          # NEW

    async def _handle_one(
        self,
        message: aio_pika.IncomingMessage,
        log_flag: bool,
        sem: asyncio.Semaphore
    ) -> None:
        """
        手动 ack/nack；阻塞业务丢线程池；Semaphore 控制并发。
        """
        try:
            evt = json.loads(message.body)
        except Exception:
            logging.error("Malformed message: %s", message.body)
            # 数据不可恢复，直接丢弃
            await message.reject(requeue=False)
            return

        # ---------- ① 去重：同一 msg_id 只处理一次 ----------
        msg_id = evt.get("msg_id") or str(hash(message.body))
        async with self._lock:                 # 🔒 并发安全
            if msg_id in self._seen_set:
                logging.debug("Duplicate msg_id %s skipped", msg_id)
                await message.ack()            # 去重命中也要 ack
                return
            # 驱逐策略：窗口达到上限时弹出最早的一条
            if len(self._seen) >= RECENT_SEEN:
                old = self._seen.popleft()
                self._seen_set.discard(old)
            self._seen.append(msg_id)
            self._seen_set.add(msg_id)

        # ---------- ② 真正处理：放到线程池，不阻塞事件循环 ----------
        async with sem:
            try:
                # 如果 parse_wallet_event 内部是同步阻塞，这里不会卡 event-loop
                await asyncio.to_thread(self.okx.parse_wallet_event, evt)
                await message.ack()
            except Exception as e:
                logging.error(
                    "Failed to parse wallet event: %s, error: %s",
                    evt, e, exc_info=True
                )
                # 视幂等性决定是否重回队列；通常事件具备幂等可重试，这里保守丢弃以免卡队列
                await message.nack(requeue=False)

    async def _consume(
        self,
        queue: aio_pika.abc.AbstractQueue,
        sem: asyncio.Semaphore,
        log_flag: bool
    ) -> None:
        """
        用 iterator 拉流；每条消息用 task 处理，保证拉取不断流；
        处理并发由 Semaphore 控制。
        """
        async with queue.iterator() as it:
            async for message in it:
                # 立刻创建任务去处理，iterator 继续往下拉
                asyncio.create_task(self._handle_one(message, log_flag, sem))

    async def run(self):
        # Step-1 打印一次性持仓
        await self._print_initial_positions()

        # Step-2 接入 RabbitMQ（自动重连）
        # 🔧 调大默认线程池，让 to_thread 真正铺开（建议 >= 总并发上限）
        loop = asyncio.get_running_loop()
        loop.set_default_executor(ThreadPoolExecutor(
            max_workers=MAX_CONCURRENCY_OPS + MAX_CONCURRENCY_INFO
        ))
        # 🔧 ops / info 分离连接 + 分离 Channel + 各自 QoS
        conn_ops = await aio_pika.connect_robust(
            self.mq_url, heartbeat=30,
            client_properties={"connection_name": "strategy_ops"}
        )
        conn_info = await aio_pika.connect_robust(
            self.mq_url, heartbeat=30,
            client_properties={"connection_name": "strategy_info"}
        )
        chan_ops  = await conn_ops.channel()
        chan_info = await conn_info.channel()
        # 建议：ops 预取大、info 小些（也可配到 config.yaml）
        await chan_ops.set_qos(prefetch_count=max(PREFETCH, MAX_CONCURRENCY_OPS * 2))
        await chan_info.set_qos(prefetch_count=max(16, MAX_CONCURRENCY_INFO * 2))
        # Indicator 状态请求（与钱包快照解耦）
        await chan_info.declare_queue(self.queue_status_req, durable=True)
        # NEW: 钱包快照请求（用于冷启动回放 → 触发 FILL/OPEN）
        await chan_ops.declare_queue(self.queue_snap_req, durable=True)

        # ---------- 启动即请求一次 MONITOR_STATUS（只发到 q.snapshot.req / volume_profile） ----------

        await chan_info.default_exchange.publish(
            aio_pika.Message(
                body=json.dumps({"type": "MONITOR_STATUS_REQ"}).encode("utf-8"),
                content_type="application/json",
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            ),
            routing_key=self.queue_status_req,
        )
        # 声明两个队列：wallet_ops 与 wallet_info
        ops_queue  = await chan_ops.declare_queue(self.queue_ops,  durable=True)
        info_queue = await chan_info.declare_queue(self.queue_info, durable=True)
        # 额外声明并消费 status.events（或配置指定的队列名）
        status_queue = await chan_info.declare_queue(self.queue_status, durable=True)
        # ✅ 改为 iterator + 手动 ack 的并发消费模式（各自用不同的并发上限）
        asyncio.create_task(self._consume(ops_queue,  self._sem_ops,  True))
        asyncio.create_task(self._consume(info_queue, self._sem_info, False))
        asyncio.create_task(self._consume(status_queue, self._sem_info, False))
        # ---------- NEW: 启动后立刻请求“钱包快照回放”，由钱包端回放到其配置的 QUEUE_OPS ----------
        await chan_ops.default_exchange.publish(
            aio_pika.Message(
                body=json.dumps({"type": "SNAPSHOT_REQ"}).encode("utf-8"),
                content_type="application/json",
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            ),
            routing_key=self.queue_snap_req,
        )
        logging.info("[BOOTSTRAP] SNAPSHOT_REQ sent → %s",
                     self.queue_snap_req)
        # 阻塞协程
        await asyncio.Future()

# ------------------------------------------------------------------ #
#  启动脚本
# ------------------------------------------------------------------ #
if __name__ == "__main__":
    okx_cli = OKXTradingClient(
        flag        = "0",       # 实盘账号；如果要跑“模拟盘”填 "1"
        margin_mode = "cross",   # 全仓
        leverage    = "100",     # 默认先给一个大一点的目标杠杆
        pos_mode    = "long_short", 
        leverage_mode ="double"     
    )
    service = StrategyService(
        okx_cli,
        MQ_URL,
        MQ_QUEUE_OPS,
        MQ_QUEUE_INFO,
        MQ_QUEUE_SNAP_REQ,          # 钱包：queue_snap_req
        MQ_QUEUE_STATUS,            # 消费：status.events
        MQ_QUEUE_STATUS_REQ         # 新增：向 Indicator 请求 MONITOR_STATUS 的队列（q.snapshot.req）
    )
    try:
        asyncio.run(service.run())
    except KeyboardInterrupt:
        logging.info("StrategyService stopped by user")
