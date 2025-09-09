"""
HLWebSocketManager.py
~~~~~~~~~~~~~~~~~~~~~
高可用 Hyperliquid WebSocket 管理器
"""

from __future__ import annotations
import sys, os
import json, logging, threading, time, random, itertools
from typing import Callable, Dict, List, Optional

import websocket                 
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
if project_root not in sys.path:
    sys.path.insert(0, project_root)
from utils.alert import send_email

WS_URL = "wss://api.hyperliquid.xyz/ws"
PING_INTERVAL = 20        # 秒
PING_TIMEOUT  = 10        # 秒
SUB_TIMEOUT   = 5         # 秒，收不到 ack 视为失败
MAX_SUB_RETRY = 2         # 第 1 次失败立即重试，第 2 次失败告警
BACKOFF_BASE  = 1         # 首次重连等待 1 s
BACKOFF_FACTOR= 2         # 指数退避因子
BACKOFF_MAX   = 60        # 最大退避


class HLWebSocketManager:
    """
    :param users: 需要订阅的地址列表
    :param on_position: 回调函数 (user, payload_dict) -> None
    """
    def __init__(
        self,
        users: List[str],
        on_position: Callable[[str, dict], None],
        on_fill: Optional[Callable[[str, dict], None]] = None,
        on_mids: Optional[Callable[[dict], None]] = None,   # ← 新增
        extra_subs: Optional[List[str]] = None,
    ):
        self.users = users
        self.on_position = on_position
        self.on_fill     = on_fill
        self._extra_subs = extra_subs or []
        self.on_mids     = on_mids    

        self._ws: Optional[websocket.WebSocketApp] = None
        self._stop_evt  = threading.Event()
        self._ack_lock  = threading.Lock()
        self._ack_map: Dict[tuple, bool] = {}         # (user,subType) -> ack
        self._retry_map: Dict[tuple, int] = {}        # (user,subType) -> retries
        self._connect_thread: Optional[threading.Thread] = None

    # ------------------------------------------------------------------ #
    # 公开方法
    # ------------------------------------------------------------------ #
    def start(self) -> None:
        """启动后台线程，保持长连"""
        self._connect_thread = threading.Thread(target=self._run, daemon=True)
        self._connect_thread.start()
        logging.info("HLWebSocketManager started with %d users", len(self.users))

    def stop(self) -> None:
        """优雅关闭"""
        self._stop_evt.set()
        if self._ws:
            self._ws.close()

    # ------------------------------------------------------------------ #
    # 核心循环：掉线后指数退避重连
    # ------------------------------------------------------------------ #
    def _run(self):
        backoff_iter = (BACKOFF_BASE * BACKOFF_FACTOR ** i for i in itertools.count())
        while not self._stop_evt.is_set():
            wait = min(next(backoff_iter), BACKOFF_MAX)
            if self._connect():
                # 连接成功后重置退避
                backoff_iter = (BACKOFF_BASE * BACKOFF_FACTOR ** i for i in itertools.count())
            else:
                logging.warning("ws connect failed, retry in %s s", wait)
                time.sleep(wait)

    # ------------------------------------------------------------------ #
    # 单次连接生命周期
    # ------------------------------------------------------------------ #
    def _connect(self) -> bool:
        try:
            self._ws = websocket.WebSocketApp(
                WS_URL,
                on_open   = self._on_open,
                on_message= self._on_message,
                on_error  = self._on_error,
                on_close  = self._on_close,
            )
            # run_forever 自带 ping_keepalive 与 reconnect 钩子，但为保持更细粒度控制，
            # 我们在外层循环自己重连。:contentReference[oaicite:3]{index=3}
            self._ws.run_forever(
                ping_interval=PING_INTERVAL,
                ping_timeout =PING_TIMEOUT,
                ping_payload ="keepalive"
            )
            return False   # run_forever 返回表示连接掉线
        except Exception as e:
            logging.exception("ws connection raised: %s", e)
            return False

    # ------------------------------------------------------------------ #
    # WebSocket 事件回调
    # ------------------------------------------------------------------ #
    def _on_open(self, _):

        # ──────────────────────────────────────────────────────────────
        # ① 断线重连时，需要把跨模块缓存全部重置
        #    • _first_done  —— 首帧到达屏障，防止 Ticker 过早启动
        #    • _snapshots   —— 上一条连接遗留的持仓快照
        #      若不清空，新首帧会被误判为增量，导致 OPEN 事件漏发
        # ──────────────────────────────────────────────────────────────
        try:
            import __main__ as _w              # 保证运行在同一解释器内
            if hasattr(_w, "_first_done"):
                _w._first_done.clear()
            if hasattr(_w, "_snapshots"):
                _w._snapshots.clear()
        except Exception as e:                 # 兜底防止循环引用等异常
            logging.debug("reset shared state fail: %s", e)
        with self._ack_lock:
            self._ack_map   = {(u, "webData2"): False for u in self.users}
            if "userEvents" in self._extra_subs:
                self._ack_map.update({(u, "userEvents"): False for u in self.users})
            self._retry_map = {k: 0 for k in self._ack_map}
        # ① 先订全市场 mid 价
        self._ws.send(json.dumps({"method":"subscribe",
                                  "subscription":{"type":"mids"}}))
        # ② 再订 user-level 数据
        for u in self.users:
            self._send_sub(u, "webData2")
            if "userEvents" in self._extra_subs:     # 成交流（可选）
                self._send_sub(u, "userEvents")
        # ③ 订阅全市场 mid 价（一次即可，不需要 user）
        try:
            self._ws.send(json.dumps({
                "method": "subscribe",
                "subscription": {"type": "allMids"}
            }))
        except Exception as e:
            logging.error("send mids subscription failed: %s", e)
        # 启动后台定时器检查订阅 ack
        threading.Thread(target=self._watch_sub_ack, daemon=True).start()

    def _on_close(self, *_):
        logging.warning("ws closed")

    def _on_error(self, _, err):
        logging.error("ws error: %s", err)

    def _on_message(self, _, msg: str):
        try:
            data = json.loads(msg)
            ch   = data.get("channel")
            # # 临时调试：把 webData2 全量打印出来
            # if ch == "webData2":
            #     logging.info("🟢 webData2 raw: %s", json.dumps(data)[:1500])   # 只打印前 500 字符

            if ch == "subscriptionResponse":
                sub  = data["data"]["subscription"]
                sub_type = sub.get("type")
                user      = sub.get("user")      # allMids 没有 user

                if user is None:
                    # 全局频道（如 allMids）的 ACK，只做日志
                    logging.info("subscription ack for global channel %s", sub_type)
                else:
                    key = (user, sub_type)
                    with self._ack_lock:
                        self._ack_map[key]   = True
                        self._retry_map[key] = 0
                    logging.info("subscription ack for %s (%s)", user, sub_type)

            elif ch == "webData2":
                if self.on_position:
                    self.on_position(data["data"]["user"], data["data"])
            elif ch == "userEvents":
                # DEBUG：先把完整结构打一行，方便确定 events/fills 层级
                logging.info("userEvents raw: %s", json.dumps(data, ensure_ascii=False))
                if self.on_fill:
                    self.on_fill(data["data"]["user"], data["data"])
            elif ch == "allMids": 
      
                if self.on_mids:
                    self.on_mids(data["data"]["mids"])
        except Exception:
            logging.exception("failed to process message")

    # ------------------------------------------------------------------ #
    # 订阅 & 重试
    # ------------------------------------------------------------------ #
    def _send_sub(self, user: str, sub_type: str):
        # ② 防止 YAML 把 0x 前缀解析为 int
        user_str = user if isinstance(user, str) else hex(user)
        sub = {"method": "subscribe",
               "subscription": {"type": sub_type, "user": user_str}}
        try:
            self._ws.send(json.dumps(sub))
        except Exception as e:
            logging.error("send subscription failed for %s: %s", user, e)

    def _watch_sub_ack(self):
        """监控 ack，最多重试一次；第二次失败则告警"""
        while self._ws and self._ws.sock and self._ws.sock.connected:
            time.sleep(SUB_TIMEOUT)           # 先等待，再检查最新 ack
            with self._ack_lock:
                pending = [u for u, ok in self._ack_map.items() if not ok]
            if not pending:
                return  # 全部 ack
            for key in pending:
                user, typ = key               # 拆包
                with self._ack_lock:
                    retries = self._retry_map[key] + 1
                    self._retry_map[key] = retries
                if retries <= MAX_SUB_RETRY:
                    logging.warning("retrying subscription for %s (%d/%d)",
                                    user, retries, MAX_SUB_RETRY)
                    self._send_sub(user, typ)
                else:
                    subj = f"[HL WS] Subscription failed for {user} ({typ})"
                    body = f"Hyperliquid websocket订阅连续{retries}次失败，已退出重试。"
                    send_email(subj, body)
                    logging.error("subscription failed twice for %s, alert sent", user)
                    # 避免无限告警，只记录一次
                    with self._ack_lock:
                        self._ack_map[key] = True   # 标记为已处理


# ---------------------------------------------------------------------- #
# 示例用法：直接运行可测试
# ---------------------------------------------------------------------- #
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    import yaml, os

    cfg_file = os.path.join(os.path.dirname(__file__), "config.yaml")
    if os.path.exists(cfg_file):
        with open(cfg_file, "r", encoding="utf-8") as f:
            USERS = yaml.safe_load(f)["users"]
    else:
        USERS = ["0x916ea2a9f3ba1ddd006c52babd0216e2ac54ed32"]

    def on_pos(user, payload):
        pos = payload["clearinghouseState"]["assetPositions"]
        logging.info("★ %s 持仓数=%d", user, len(pos))

    mgr = HLWebSocketManager(USERS, on_pos)
    mgr.start()

    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        mgr.stop()
