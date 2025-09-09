# streamer.py
# 指标快照广播：聚合 250–1000ms 推一次到 ex.features；Publisher Confirms；TTL/溢出策略可配
from __future__ import annotations

import json
import logging
import queue
import random
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

import pika
import yaml


@dataclass
class StreamerConfig:
    amqp_url: str
    exchange: str
    exchange_type: str = "fanout"        # topic/fanout/direct，按需
    routing_key_fmt: str = "features.{instId}"
    interval_ms: int = 500               # 250–1000ms 之间
    ttl_ms: int = 1000                   # 每条消息的过期（毫秒）
    publisher_confirms: bool = True
    delivery_mode_persistent: bool = False  # 指标快照多为瞬时，可设 False
    queue_maxsize: int = 10000
    overflow_policy: str = "coalesce"    # coalesce | drop_oldest | drop_new
    batch_max: int = 500                 # 单批最多发布 N 条（合并后）
    reconnect_backoff_min: float = 0.8
    reconnect_backoff_max: float = 5.0
    heartbeat: int = 30
    blocked_connection_timeout: int = 15
    declare_exchange: bool = True        # 若交换机由运维预建，可置 False
    # 使用默认交换机("")且 routing_key_fmt 为固定队列名时，自动声明该队列（防止首批消息 unroutable）
    declare_queue_on_default_exchange: bool = True


class FeatureStreamer:
    """
    线程安全：
      • 生产者线程通过 put(snapshot) 写入内部有界队列
      • 后台线程定时聚合 + 批量发布（Publisher Confirms）
    溢出策略：
      • coalesce   : 队列满时直接合并到 latest_map，丢弃入队
      • drop_oldest: 弹出最老元素，再入队
      • drop_new   : 丢弃新元素
    """

    def __init__(self, cfg: StreamerConfig, service_id: str = "indicator-svc"):
        self.cfg = cfg
        self.service_id = service_id
        self._in_q: "queue.Queue[dict]" = queue.Queue(maxsize=cfg.queue_maxsize)
        self._latest_by_inst: Dict[str, dict] = {}
        self._stop_evt = threading.Event()
        self._thr: Optional[threading.Thread] = None

        # MQ 句柄
        self._conn: Optional[pika.BlockingConnection] = None
        self._ch: Optional[pika.adapters.blocking_connection.BlockingChannel] = None

        # Basic.Return 回调日志（mandatory=True 时）
        self._returns = 0

    # ---------------- 公共 API ---------------- #

    def start(self) -> None:
        if self._thr and self._thr.is_alive():
            return
        self._stop_evt.clear()
        self._thr = threading.Thread(target=self._run_loop, name="FeatureStreamer", daemon=True)
        self._thr.start()
        logging.info("[streamer] started")

    def stop(self) -> None:
        self._stop_evt.set()
        if self._thr:
            self._thr.join(timeout=5)
        self._close()

    def put(self, snapshot: dict) -> None:
        """
        snapshot 约定：
          必含 'instId'，其余自由键：如 ofi/microprice/qimb/spread/cvd/profile/state...
        """
        inst_id = str(snapshot.get("instId") or snapshot.get("inst_id") or "").strip()
        if not inst_id:
            return
        # 尽量小对象：序列化交给发布批次再做
        try:
            self._in_q.put_nowait(snapshot)
        except queue.Full:
            pol = self.cfg.overflow_policy.lower()
            if pol == "drop_oldest":
                try:
                    self._in_q.get_nowait()
                    self._in_q.put_nowait(snapshot)
                except Exception:
                    pass
            elif pol == "coalesce":
                # 直接合并，不入队
                self._latest_by_inst[inst_id] = snapshot
            else:
                # drop_new
                pass

    # ---------------- 内部：主循环 ---------------- #

    def _run_loop(self) -> None:
        self._ensure_mq()
        interval = max(250, min(1000, int(self.cfg.interval_ms))) / 1000.0

        next_flush = time.monotonic() + interval
        pending: Dict[str, dict] = {}

        while not self._stop_evt.is_set():
            # 1) 尽可能多地取数据（非阻塞），与 latest_by_inst 合并去重
            drained = 0
            while drained < self.cfg.batch_max:
                try:
                    item = self._in_q.get_nowait()
                    inst_id = str(item.get("instId") or item.get("inst_id"))
                    if inst_id:
                        pending[inst_id] = item
                    drained += 1
                except queue.Empty:
                    break

            # 队列外合并（coalesce 路径）
            if self._latest_by_inst:
                pending.update(self._latest_by_inst)
                self._latest_by_inst.clear()

            now = time.monotonic()
            if pending and (now >= next_flush):
                # 2) 批量发布
                to_pub = pending
                pending = {}
                try:
                    self._publish_batch(to_pub)
                except Exception as e:
                    logging.exception("[streamer] publish batch failed: %s", e)
                    # 失败：回滚到 pending，稍后重试；并尝试重连
                    for k, v in to_pub.items():
                        # 尽量别丢：若 pending 已积压很大，仍可能受限于 maxsize
                        self._try_requeue(v)
                    self._reconnect_with_backoff()
                next_flush = now + interval

            # 3) 心跳/空转
            self._process_io(time_limit=0.05)
            time.sleep(0.005)

    def _try_requeue(self, snapshot: dict) -> None:
        try:
            self._in_q.put_nowait(snapshot)
        except queue.Full:
            # 满则合并到 latest_map，避免无限阻塞
            inst_id = str(snapshot.get("instId") or snapshot.get("inst_id") or "")
            if inst_id:
                self._latest_by_inst[inst_id] = snapshot

    # ---------------- MQ 连接与发布 ---------------- #

    def _ensure_mq(self) -> None:
        if self._conn and self._conn.is_open and self._ch and self._ch.is_open:
            return

        self._close()
        params = pika.URLParameters(self.cfg.amqp_url)
        params.heartbeat = self.cfg.heartbeat
        params.blocked_connection_timeout = self.cfg.blocked_connection_timeout

        attempt = 0
        while not self._stop_evt.is_set():
            try:
                self._conn = pika.BlockingConnection(parameters=params)
                self._ch = self._conn.channel()

                if self.cfg.declare_exchange and self.cfg.exchange:
                    self._ch.exchange_declare(
                        exchange=self.cfg.exchange,
                        exchange_type=self.cfg.exchange_type,
                        durable=True,
                        passive=False
                    )
                # 默认交换机直投：确保目标队列已存在（且 durable 与消费者一致）
                if (self.cfg.exchange == "") and self.cfg.declare_queue_on_default_exchange:
                    rk = str(self.cfg.routing_key_fmt or "")
                    # 只有在 routing_key_fmt 不是模板（不含 {…}）时才可预声明
                    if ("{" not in rk) and ("}" not in rk) and rk:
                        try:
                            self._ch.queue_declare(queue=rk, durable=True)
                            logging.info("[streamer] ensured queue=%s for default-exchange direct publish", rk)
                        except Exception as qe:
                            logging.warning("[streamer] queue_declare(%s) failed: %s", rk, qe)
                # Publisher Confirms
                if self.cfg.publisher_confirms:
                    # 开启 confirm 模式，随后 basic_publish 将在 Unroutable 时抛出异常
                    self._ch.confirm_delivery()  # 详见 Pika 示例与文档
                # 处理 mandatory 返回
                self._ch.add_on_return_callback(self._on_returned)

                logging.info("[streamer] connected to MQ, exchange=%s", self.cfg.exchange)
                return
            except Exception as e:
                attempt += 1
                wait = min(self.cfg.reconnect_backoff_max,
                           self.cfg.reconnect_backoff_min * (1.6 ** attempt)) * (1 + random.random() * 0.3)
                logging.warning("[streamer] connect MQ failed: %s — retry in %.2fs", e, wait)
                time.sleep(wait)

    def _publish_batch(self, by_inst: Dict[str, dict]) -> None:
        if not (self._conn and self._ch and self._conn.is_open and self._ch.is_open):
            raise RuntimeError("channel not open")

        ttl = str(int(self.cfg.ttl_ms))  # RabbitMQ BasicProperties.expiration 要求字符串
        props = pika.BasicProperties(
            content_type="application/json",
            delivery_mode=(2 if self.cfg.delivery_mode_persistent else 1),
            expiration=ttl
        )

        sent = 0
        for inst_id, payload in by_inst.items():
            # 规范化消息
            msg = self._build_message(inst_id, payload)
            body = json.dumps(msg, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
            rk = self.cfg.routing_key_fmt.format(instId=inst_id)

            try:
                # mandatory=True → 若路由不到队列则触发 Basic.Return
                self._ch.basic_publish(
                    exchange=self.cfg.exchange,
                    routing_key=rk,
                    body=body,
                    properties=props,
                    mandatory=True
                )
                sent += 1
            except pika.exceptions.UnroutableError as ue:
                # 没有队列绑定或被 DLX/策略拒绝
                logging.warning("[streamer] unroutable inst=%s rk=%s err=%s", inst_id, rk, ue)
            except pika.exceptions.NackError as ne:
                # 确认模式下被 nack（落盘失败等）
                logging.error("[streamer] nack inst=%s rk=%s err=%s", inst_id, rk, ne)
                raise
            except Exception:
                # 其它异常留给上层重连处理
                raise

        # 在 confirm 模式下，允许 broker 异步 ack；这里处理 I/O 以接收 ack/return 事件
        self._process_io(time_limit=0.0)

        logging.debug("[streamer] batch published: %d, returns=%d", sent, self._returns)
        self._returns = 0

    def _on_returned(self, ch, method, properties, body):
        self._returns += 1
        try:
            preview = body.decode("utf-8")[:200]
        except Exception:
            preview = str(body)[:200]
        logging.warning("[streamer] Basic.Return reply_code=%s reply_text=%s rk=%s — body=%s",
                        getattr(method, "reply_code", "?"),
                        getattr(method, "reply_text", "?"),
                        getattr(method, "routing_key", "?"),
                        preview)
    # 健康快照：供 HealthBeat 采集
    def health_snapshot(self) -> dict:
        try:
            qsize = self._in_q.qsize()
        except Exception:
            qsize = None
        return {
            "queueSize": qsize,
            "returnsSinceLastFlush": self._returns
        }
    def _process_io(self, time_limit: float = 0.0) -> None:
        try:
            if self._conn and self._conn.is_open:
                self._conn.process_data_events(time_limit=time_limit)
        except Exception as e:
            logging.warning("[streamer] process_data_events error: %s", e)

    def _reconnect_with_backoff(self) -> None:
        self._close()
        self._ensure_mq()

    def _close(self) -> None:
        try:
            if self._ch and self._ch.is_open:
                self._ch.close()
        except Exception:
            pass
        try:
            if self._conn and self._conn.is_open:
                self._conn.close()
        except Exception:
            pass
        self._ch = None
        self._conn = None

    # ---------------- 序列化 ---------------- #

    def _build_message(self, inst_id: str, payload: dict) -> dict:
        """
        统一消息结构（与 rules.yaml 契约保持一致）：
        {
          "type": "feature.snapshot",
          "svc": "...",
          "ts": 1710000000123,
          "instId": "BTC-USDT-SWAP",
          "feat": {...}   # 原始指标字典；建议按模块分组
        }
        """
        ts = int(payload.get("ts") or time.time() * 1000)
        # 将已有键归拢到 'feat' 下，保留 instId/ts
        feat = dict(payload)
        feat.pop("instId", None)
        feat.pop("inst_id", None)
        feat.pop("ts", None)

        return {
            "type": "feature.snapshot",
            "svc": self.service_id,
            "ts": ts,
            "instId": inst_id,
            "feat": feat
        }


# ---------------- 启动入口（供独立运行） ---------------- #

def load_cfg(path: str) -> StreamerConfig:
    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}

    mq = (raw.get("mq") or {})
    exchanges = (mq.get("exchanges") or {})
    streamer = (raw.get("streamer") or {})

    return StreamerConfig(
        amqp_url=str(mq.get("host") or "amqp://guest:guest@localhost:5672/"),
        exchange=str(exchanges.get("features") or "ex.features"),
        exchange_type=str(streamer.get("exchange_type") or "fanout"),
        routing_key_fmt=str(streamer.get("routing_key_fmt") or "features.{instId}"),
        interval_ms=int(streamer.get("interval_ms") or 500),
        ttl_ms=int(streamer.get("ttl_ms") or 1000),
        publisher_confirms=bool(mq.get("publisher_confirms", True)),
        delivery_mode_persistent=bool(streamer.get("persistent", False)),
        queue_maxsize=int(streamer.get("queue_maxsize") or 10000),
        overflow_policy=str(streamer.get("overflow_policy") or "coalesce"),
        batch_max=int(streamer.get("batch_max") or 500),
        reconnect_backoff_min=float(streamer.get("reconnect_backoff_min") or 0.8),
        reconnect_backoff_max=float(streamer.get("reconnect_backoff_max") or 5.0),
        heartbeat=int(streamer.get("heartbeat") or 30),
        blocked_connection_timeout=int(streamer.get("blocked_connection_timeout") or 15),
        declare_exchange=bool(streamer.get("declare_exchange", True)),
    )


def main():
    import os
    # 默认读取同目录 config.yaml；你已放在 /root/copyTrade/indicator/config.yaml
    cfg_path = os.path.join(os.path.dirname(__file__), "config.yaml")
    cfg = load_cfg(cfg_path)

    with open(cfg_path, "r", encoding="utf-8") as f:
        whole = yaml.safe_load(f) or {}
    svc = (whole.get("service") or {}).get("id", "indicator-svc")

    FeatureStreamer(cfg, service_id=str(svc)).start()

    # 常驻进程：等待 Ctrl+C
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("shutting down streamer...")
    finally:
        # 关闭线程与连接
        # 实际部署由 app.py 持有实例并托管生命周期，这里仅独立模式
        pass


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(threadName)s] %(message)s"
    )
    main()
