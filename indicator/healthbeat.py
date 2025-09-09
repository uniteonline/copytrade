# /root/copyTrade/indicator/healthbeat.py
# -*- coding: utf-8 -*-
"""
HealthBeat — 指标服务健康上报
---------------------------------------------------------
• 周期性聚合以下健康指标并发布到 ex.ops.health：
  - svc/host/pid/uptime
  - OKX WS：connected/lastMsgAgoMs/reconnects
  - OrderBook：per-inst staleness（最后一次增量距今）
  - Streamer/RPC：内部队列积压、发布返回数
  - RabbitMQ：连接状态、每个重要队列的 message_count/consumer_count
  - 进程/系统：CPU%、RSS(MB)、loadavg（尽量不依赖第三方库）

• 发布端启用 Publisher Confirms；如不可路由（mandatory）会收到 Basic.Return 日志。
  参考：RabbitMQ confirms/pika confirm_delivery。  # noqa
"""
from __future__ import annotations

import json
import logging
import os
from contextlib import suppress
import random
import socket
import sys
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

import pika
import yaml

log = logging.getLogger(__name__)

# -------------------------- 可选系统指标依赖 --------------------------
try:
    import psutil  # type: ignore
except Exception:  # 允许无 psutil 环境
    psutil = None


# =========================== 配置结构 ===========================

@dataclass
class HealthBeatConfig:
    amqp_url: str
    exchange: str = "ex.ops.health"
    exchange_type: str = "topic"          # fanout/topic/direct
    routing_key: str = "ops.health.{svc}" # 会用 {svc} 替换
    interval_sec: float = 10.0            # 10s 周期
    ttl_ms: int = 5000                    # 每条健康包的 TTL
    publisher_confirms: bool = True
    heartbeat: int = 30
    blocked_connection_timeout: int = 20
    declare_exchange: bool = True
    # 需要探测积压的队列名（来自 config.yaml 的 mq.queues.*）
    probe_queues: List[str] = field(default_factory=list)
    # （新增）是否允许在探测不到队列时按配置自动创建
    auto_create_probe_queues: bool = False
    probe_queue_specs: Dict[str, Any] = field(default_factory=dict)
    reconnect_backoff_min: float = 0.8
    reconnect_backoff_max: float = 8.0


# =========================== 报告器主体 ===========================

class HealthBeatReporter:
    """
    用法（在 app.py 中）：
        hb = HealthBeatReporter.from_config(config_yaml, service_id)
        hb.register_source("okx", lambda: okx_client.health_snapshot())
        hb.register_source("orderbook", lambda: orderbook.health_snapshot())
        hb.register_source("streamer", lambda: streamer.health_snapshot())
        hb.register_source("rpc", lambda: rpc_server.health_snapshot())
        hb.start()
    各 source() 返回 dict，会并入 payload["sources"][name]。

    若未注册 source，也会上报基础信息与 RabbitMQ 积压。
    """

    def __init__(self, cfg: HealthBeatConfig, service_id: str):
        self.cfg = cfg
        self.svc = service_id
        self.host = socket.gethostname()
        self.pid = os.getpid()
        self._start_ts = time.time()

        # 由业务侧注入的健康快照函数
        self._sources: Dict[str, Callable[[], Dict[str, Any]]] = {}

        # MQ 连接对象
        self._conn: Optional[pika.BlockingConnection] = None
        self._ch: Optional[pika.adapters.blocking_connection.BlockingChannel] = None

        # 控制
        self._thr: Optional[threading.Thread] = None
        self._stop = threading.Event()

        # Basic.Return 计数（mandatory 未路由）
        self._returns = 0

    # --------- 工厂：从 config.yaml 解析 ---------
    @classmethod
    def from_config(cls, cfg_yaml: dict, service_id: str) -> "HealthBeatReporter":
        mq = (cfg_yaml.get("mq") or {})
        exchanges = (mq.get("exchanges") or {})
        queues = (mq.get("queues") or {})
        # （新增）可选自动创建所需的规格
        auto_create = bool(mq.get("auto_create_probe_queues", False))
        queue_specs = dict(mq.get("queue_specs") or {})
        # 需要探测的队列：按你的配置挑关键队列
        probe = []
        for k in ("eval_req", "feature_snapshot"):
            q = queues.get(k)
            if q:
                probe.append(q)

        streamer = (cfg_yaml.get("streamer") or {})
        hb_cfg = (cfg_yaml.get("health") or {})  # 可选 health 段

        cfg = HealthBeatConfig(
            amqp_url=str(mq.get("host") or "amqp://guest:guest@localhost:5672/"),
            exchange=str(exchanges.get("health") or "ex.ops.health"),
            exchange_type=str(hb_cfg.get("exchange_type") or "topic"),
            routing_key=str(hb_cfg.get("routing_key") or "ops.health.{svc}"),
            interval_sec=float(hb_cfg.get("interval_sec") or 10.0),
            ttl_ms=int(hb_cfg.get("ttl_ms") or 5000),
            publisher_confirms=bool(mq.get("publisher_confirms", True)),
            heartbeat=int(streamer.get("heartbeat") or 30),
            blocked_connection_timeout=int(streamer.get("blocked_connection_timeout") or 20),
            declare_exchange=bool(hb_cfg.get("declare_exchange", True)),
            probe_queues=probe,
            reconnect_backoff_min=float(hb_cfg.get("reconnect_backoff_min") or 0.8),
            reconnect_backoff_max=float(hb_cfg.get("reconnect_backoff_max") or 8.0),
            auto_create_probe_queues=auto_create,
            probe_queue_specs=queue_specs,
        )
        return cls(cfg, service_id)

    # ----------------- 业务注入指标源 -----------------
    def register_source(self, name: str, supplier: Callable[[], Dict[str, Any]]) -> None:
        self._sources[name] = supplier

    # ----------------- 生命周期 -----------------
    def start(self) -> None:
        if self._thr and self._thr.is_alive():
            return
        self._stop.clear()
        self._thr = threading.Thread(target=self._run, name="HealthBeat", daemon=True)
        self._thr.start()
        log.info("[health] started")

    def stop(self) -> None:
        self._stop.set()
        if self._thr:
            self._thr.join(timeout=5)
        self._close()

    # ================== 主循环 ==================
    def _run(self) -> None:
        self._ensure_mq()

        interval = max(1.0, float(self.cfg.interval_sec))
        next_at = time.monotonic() + interval

        while not self._stop.is_set():
            now = time.monotonic()
            if now >= next_at:
                try:
                    payload = self._build_payload()
                    self._publish(payload)
                except Exception as e:
                    log.exception("[health] publish failed: %s", e)
                    self._reconnect_with_backoff()
                next_at = now + interval

            self._process_io(0.05)
            time.sleep(0.01)

    # ================== 采集/拼包 ==================
    def _build_payload(self) -> Dict[str, Any]:
        ts = int(time.time() * 1000)
        uptime = max(0.0, time.time() - self._start_ts)

        # 系统/进程指标（尽量无依赖）
        cpu_pct = None
        rss_mb = None
        loadavg = None
        try:
            if psutil:
                p = psutil.Process(os.getpid())
                cpu_pct = p.cpu_percent(interval=0.0)  # 非阻塞
                rss_mb = p.memory_info().rss / (1024 * 1024)
                try:
                    loadavg = os.getloadavg()
                except Exception:
                    loadavg = None
            else:
                try:
                    loadavg = os.getloadavg()
                except Exception:
                    loadavg = None
        except Exception:
            pass

        # 业务源（OKX/OrderBook/Streamer/RPC…）
        sources: Dict[str, Any] = {}
        for name, fn in list(self._sources.items()):
            try:
                snap = fn() or {}
                sources[name] = snap
            except Exception as e:
                sources[name] = {"error": str(e)}

        # 队列积压：改用“临时 channel”做 passive declare，避免 404 关闭发布 channel
        queues: Dict[str, Any] = {}
        if self.cfg.probe_queues and self._conn and self._conn.is_open:
            for qname in self.cfg.probe_queues:
                if qname:
                    queues[qname] = self._probe_queue_once(qname)
        payload = {
            "type": "ops.health",
            "etype": "HEALTH",           # NEW: 事件类型（payload 字段）
            "svc": self.svc,
            "host": self.host,
            "pid": self.pid,
            "ts": ts,
            "uptimeSec": round(uptime, 1),
            "proc": {
                "cpuPct": cpu_pct,
                "rssMb": rss_mb,
                "loadavg": loadavg,
            },
            "mq": {
                "connected": bool(self._conn and self._conn.is_open and self._ch and self._ch.is_open),
                "returns": self._returns,
                "publisherConfirms": bool(self.cfg.publisher_confirms),
            },
            "queues": queues,
            "sources": sources,
        }
        # 清零 returns 计数（按发布周期统计）
        self._returns = 0
        return payload

    # ================== MQ 连接/发布 ==================
    def _ensure_mq(self) -> None:
        if self._conn and self._conn.is_open and self._ch and self._ch.is_open:
            return
        self._close()

        params = pika.URLParameters(self.cfg.amqp_url)
        params.heartbeat = self.cfg.heartbeat
        params.blocked_connection_timeout = self.cfg.blocked_connection_timeout

        attempt = 0
        while not self._stop.is_set():
            try:
                self._conn = pika.BlockingConnection(params)
                self._ch = self._conn.channel()
                if self.cfg.declare_exchange:
                    self._ch.exchange_declare(
                        exchange=self.cfg.exchange,
                        exchange_type=self.cfg.exchange_type,
                        durable=True,
                        passive=False
                    )
                if self.cfg.publisher_confirms:
                    # Publisher Confirms：发布可靠性保障
                    self._ch.confirm_delivery()
                # 处理 Basic.Return（mandatory 时不可路由）
                self._ch.add_on_return_callback(self._on_return)
                log.info("[health] connected to MQ exchange=%s", self.cfg.exchange)
                return
            except Exception as e:
                attempt += 1
                wait = min(self.cfg.reconnect_backoff_max,
                           self.cfg.reconnect_backoff_min * (1.7 ** attempt)) * (1 + 0.3 * random.random())
                log.warning("[health] connect MQ failed: %s — retry in %.2fs", e, wait)
                time.sleep(wait)

    def _publish(self, payload: Dict[str, Any]) -> None:
        if not (self._conn and self._conn.is_open and self._ch and self._ch.is_open):
            raise RuntimeError("MQ channel not open")
        rk = self.cfg.routing_key.format(svc=self.svc)
        ttl = str(int(self.cfg.ttl_ms))  # per-message TTL 字符串（ms）
        props = pika.BasicProperties(
            content_type="application/json",
            delivery_mode=1,          # 健康包多为瞬时信息：非持久
            expiration=ttl,
            headers={"etype": "HEALTH", "svc": self.svc},  # NEW: AMQP 头
            type="health",                                   # NEW: AMQP 属性 'type'
        )
        body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        # mandatory=True → 若不可路由将触发 Basic.Return（先于 confirm ack/nack）
        self._ch.basic_publish(
            exchange=self.cfg.exchange,
            routing_key=rk,
            body=body,
            properties=props,
            mandatory=True
        )
        # 处理 confirm/return 回调
        self._process_io(0.0)
        log.debug("[health] published rk=%s", rk)

    def _on_return(self, ch, method, properties, body):
        self._returns += 1
        try:
            preview = body.decode("utf-8")[:180]
        except Exception:
            preview = str(body)[:180]
        log.warning("[health] Basic.Return code=%s text=%s rk=%s body=%s",
                    getattr(method, "reply_code", "?"),
                    getattr(method, "reply_text", "?"),
                    getattr(method, "routing_key", "?"),
                    preview)

    def _process_io(self, time_limit: float) -> None:
        try:
            if self._conn and self._conn.is_open:
                self._conn.process_data_events(time_limit=time_limit)
        except Exception as e:
            log.debug("[health] process_data_events error: %s", e)

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

    # ================== 队列探测（使用临时 channel） ==================
    def _probe_queue_once(self, qname: str) -> Dict[str, Any]:
        """
        在独立的临时 channel 上对队列执行 passive declare：
          - 队列存在：返回 message_count/consumer_count
          - 队列不存在(404)：若允许且提供了规格，则尝试按规格创建并（可选）绑定，然后返回统计
          - 任何错误：以 {"error": "..."} 返回，但不影响发布 channel
        """
        if not (self._conn and self._conn.is_open):
            return {"error": "conn_not_open"}
        ch = None
        try:
            ch = self._conn.channel()
            ok = ch.queue_declare(queue=qname, passive=True)
            return {
                "messages": int(getattr(ok.method, "message_count", 0)),
                "consumers": int(getattr(ok.method, "consumer_count", 0)),
            }
        except pika.exceptions.ChannelClosedByBroker as e:
            code = getattr(e, "reply_code", None)
            text = getattr(e, "reply_text", "")
            # 仅在 404 且允许自动创建时尝试创建
            if code == 404 and self.cfg.auto_create_probe_queues:
                spec = self.cfg.probe_queue_specs.get(qname) or {}
                if not spec:
                    # 没有提供规格就不创建，避免误建成默认参数队列
                    # 显式返回一个可识别的错误码，便于观测与告警
                    return {"error": "no_spec_for_auto_create"}
                try:
                    ch2 = self._conn.channel()
                    # 若提供了 exchange_type 且需要绑定，则先确保交换机存在
                    bind = (spec.get("bind") or {})
                    ex = str(bind.get("exchange") or "")
                    rk = str(bind.get("routing_key") or "")
                    ex_type = str(spec.get("exchange_type") or "")
                    if ex and ex_type:
                        ch2.exchange_declare(
                            exchange=ex,
                            exchange_type=ex_type,
                            durable=True,
                            passive=False
                        )
                    # 按规格声明队列
                    ch2.queue_declare(
                        queue=qname,
                        durable=bool(spec.get("durable", True)),
                        auto_delete=bool(spec.get("auto_delete", False)),
                        arguments=dict(spec.get("arguments") or {}),
                        passive=False,
                    )
                    if ex and rk:
                        ch2.queue_bind(queue=qname, exchange=ex, routing_key=rk)
                    # 创建完成后再被动声明一次拿统计
                    ok2 = ch2.queue_declare(queue=qname, passive=True)
                    res = {
                        "created": True,
                        "messages": int(getattr(ok2.method, "message_count", 0)),
                        "consumers": int(getattr(ok2.method, "consumer_count", 0)),
                    }
                    with suppress(Exception):
                        ch2.close()
                    return res
                except Exception as ce:
                    return {"error": f"create_failed:{ce}"}
            # 不创建或非 404：回传错误文本
            return {"error": f"{code}:{text}"}
        except Exception as e:
            return {"error": str(e)}
        finally:
            # 只关闭临时 channel；发布 channel 不受影响
            with suppress(Exception):
                if ch and getattr(ch, "is_open", False):
                    ch.close()
 
