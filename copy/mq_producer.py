# mq_producer.py
import json, asyncio, logging, threading
import aio_pika
from aio_pika import Message, DeliveryMode, connect_robust
from aio_pika.exceptions import AMQPException, MessageProcessError
class MQProducer:
    """
    · 采用 aio-pika RobustConnection：自动重连、自动恢复 channel
    · 对外仍暴露同步接口 `publish(body)`，调用方 **无需改动**
    """

    def __init__(
        self,
        amqp_url: str,
        queue: str = "queue_ops",
        *,
        heartbeat: int = 30,
    ):
        self.url, self.queue_name, self.heartbeat = amqp_url, queue, heartbeat

        # 尝试复用主线程 loop；若不存在则新建后台 loop
        try:
            self.loop = asyncio.get_running_loop()
        except RuntimeError:
            self.loop = asyncio.new_event_loop()
            threading.Thread(target=self.loop.run_forever, daemon=True).start()

        self._ready_evt = asyncio.Event()
        self.loop.create_task(self._init())

    # ---------- 建立连接 ----------
    async def _init(self):
        self.conn: aio_pika.RobustConnection = await connect_robust(
            self.url,
            loop=self.loop,
            heartbeat=self.heartbeat,
        )
        self.chan: aio_pika.RobustChannel = await self.conn.channel(
            publisher_confirms=True
        )
        await self.chan.declare_queue(self.queue_name, durable=True)
        self.exch = self.chan.default_exchange
        self._ready_evt.set()

    # ---------- 真正的异步发送 ----------
    async def _publish_async(self, body: dict):
        await self._ready_evt.wait()
        payload = json.dumps(body, separators=(",", ":")).encode()
        await self.exch.publish(
            Message(payload, delivery_mode=DeliveryMode.PERSISTENT),
            routing_key=self.queue_name,
            mandatory=True,
        )

    # ---------- 同步封装 ----------
    def publish(self, body: dict):
        """
        · 同步线程环境：阻塞直到 confirm 收到 / 抛异常
        · 若已在同一个事件循环中：返回 asyncio.Task（调用方可自行 await）
        """
        try:
            cur_loop = asyncio.get_running_loop()
            if cur_loop is self.loop:
                return cur_loop.create_task(self._publish_async(body))
        except RuntimeError:
            pass

        fut = asyncio.run_coroutine_threadsafe(
            self._publish_async(body), self.loop
        )
        return fut.result()