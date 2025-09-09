# /root/copyTrade/indicator/app.py
# -*- coding: utf-8 -*-
"""
Indicator Service — Application Entrypoint (orchestrator only)

本文件只负责：
- 读取 config.yaml，初始化日志
- 创建并装配：MQ 发布/消费、OKX WS 客户端、指标引擎、健康心跳
- 注册回调：订单簿/成交 → 指标引擎 → 周期发布 features.snapshot
- 启动风险评估 RPC（下单系统的评估请求 → 用最新特征同步应答）
- 优雅关停

实现细节（订单簿重建、seqId 对齐、指标计算、发布确认等）
全部在各自模块中完成：
  ws/okx_client.py
  book/orderbook.py
  features/core.py, features/cvd.py, features/profile.py, features/vpin.py
  quantiles/state.py
  mq/streamer.py, mq/rpc_eval.py
  ops/healthbeat.py

文档依据（仅供设计对齐）：
- OKX V5 API（books-l2-tbt / books 增量、seqId/prevSeqId、初始快照改 REST/SBE）.
- RabbitMQ Publisher Confirms / 消费者确认；aio-pika 的 confirms 支持.
"""

from __future__ import annotations

import asyncio
import threading
import logging
import signal
import sys
import time
import json
from pathlib import Path
from typing import Dict, Optional
from streamer import load_cfg, StreamerConfig
import yaml
import aio_pika
# ------- 业务模块（由你按 11 件套创建）-------
# WS 管理 & 数据源
from okx_client import OKXClient          # 提供 start()/close()，回调 on_orderbook/on_trade
# 指标引擎（聚合各 features/*）
from core import FeatureEngine               # 指标引擎（BBO/OFI/Depth/Microprice）
from cvd import build_from_config as build_cvd
from profile import build_multi_from_config as build_profile_multi
from vpin import build_from_config as build_vpin
from state import build_from_config as build_state
# 特征广播（后台线程 + Publisher Confirms）
from streamer import FeatureStreamer
from rpc_eval import RpcServer, merge_rules
try:
    from healthbeat import HealthBeatReporter
except Exception:
    HealthBeatReporter = None

# ================= 工具 =================
APP_ROOT = Path("/root/copyTrade/indicator").resolve()
CONFIG_PATH = APP_ROOT / "config.yaml"
RULES_PATH = APP_ROOT / "rules.yaml"

def load_config(path: Path) -> dict:
    if not path.exists():
        raise FileNotFoundError(f"config not found: {path}")
    with path.open("r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}
    # 一些默认值兜底
    cfg.setdefault("service", {}).setdefault("id", "indicator-svc")
    cfg.setdefault("features", {}).setdefault("windows", {})
    cfg["features"]["windows"].setdefault("ofi_ms", 1000)
    cfg["features"]["windows"].setdefault("cvd_sec", 60)
    # profile 模块默认：避免用户漏配
    cfg.setdefault("features", {}).setdefault("profile", {})
    cfg["features"]["profile"].setdefault("bootstrap_minutes", 480)
    cfg["features"]["profile"].setdefault("bootstrap_trade_pages", 80)
    cfg.setdefault("okx", {}).setdefault("channels", {})
    cfg["okx"].setdefault("instIds", ["BTC-USDT-SWAP", "ETH-USDT-SWAP", "SOL-USDT-SWAP"])
    cfg.setdefault("mq", {}).setdefault("publisher_confirms", True)
    cfg["mq"].setdefault("prefetch", 8)

    return cfg


def setup_logging(cfg: dict) -> None:
    level_name = cfg.get("service", {}).get("log_level", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s.%(msecs)03d %(levelname)s [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    # 降噪常见三方库
    logging.getLogger("aio_pika").setLevel(logging.WARNING)
    logging.getLogger("aiohttp").setLevel(logging.WARNING)


# ================= 应用编排 =================
class IndicatorApp:
    """
    负责装配各组件并管理其生命周期。
    """
    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.service_id: str = cfg["service"]["id"]

        # 组件占位
        self.streamer: Optional[FeatureStreamer] = None
        self.okx: Optional[OKXClient] = None
        self.engine: Optional[FeatureEngine] = None
        self.cvd = None
        self.vpin = None
        self.profile = None
        self.state = None
        self.health: Optional[object] = None
        # 缓存波动刻度（供发布到 snapshot["vol"]）
        self._vol_state: Dict[str, dict] = {}
        # 发布节流：默认 100ms/标的（可按需暴露为 config.features.pub_interval_ms）
        self._pub_interval_ms: int = int(cfg.get("features", {}).get("pub_interval_ms", 100))
        self._last_pub_ms: Dict[str, int] = {inst: 0 for inst in cfg["okx"]["instIds"]}

        self._main_tasks: list[asyncio.Task] = []
        self._stop_evt = asyncio.Event()
        self._rpc_thread: Optional[threading.Thread] = None
        # ---------- MONITOR_STATUS 双通道 ----------
        self._amqp_url: Optional[str] = None
        qcfg = ((self.cfg.get("mq", {}) or {}).get("queues", {}) or {})
        # 事件直投队列（默认：status.events）
        self._status_queue_name: str = str(qcfg.get("status_events", "status.events"))
        # 快照请求队列（默认：q.snapshot.req）
        self._snapreq_queue_name: str = str(qcfg.get("snapshot_req", "q.snapshot.req"))
        # AMQP 资源
        self._status_conn: Optional[aio_pika.RobustConnection] = None
        self._status_chan: Optional[aio_pika.abc.AbstractChannel] = None
        self._feature_queue_name: Optional[str] = None
        self._feature_eval_queue_name: Optional[str] = None

    # ----- 生命周期 -----
    async def start(self):
        scfg: StreamerConfig = load_cfg(str(CONFIG_PATH))
        # —— 目标队列名（生产 + eval 各一）
        try:
            q_feat = str(((self.cfg.get("mq", {}) or {}).get("queues", {}) or {}).get("feature_snapshot", "q.features.snapshot"))
        except Exception:
            q_feat = "q.features.snapshot"
        q_feat_eval = str(((self.cfg.get("mq", {}) or {}).get("queues", {}) or {}).get("feature_snapshot_eval", "q.features.eval"))
        # 发布改为 fanout：一次发布复制到多个队列
        ex_name = str((((self.cfg.get("mq", {}) or {}).get("exchanges", {}) or {}).get("features")) or "ex.features")
        scfg.exchange = ex_name
        scfg.declare_exchange = True
        scfg.routing_key_fmt = ""   # fanout 忽略 routing_key
        self._feature_queue_name = q_feat
        self._feature_eval_queue_name = q_feat_eval
        # —— 取 AMQP 连接串（mq.url / rabbitmq.url / StreamerConfig.*）
        mq_cfg = (self.cfg.get("mq", {}) or {})
        self._amqp_url = (
            mq_cfg.get("url")
            or mq_cfg.get("host")              # <<< 关键：支持 config.yaml 里的 mq.host
            or (self.cfg.get("rabbitmq", {}) or {}).get("url")
            or getattr(scfg, "url", None)
            or getattr(scfg, "amqp_url", None)
        )
        if self._amqp_url:
            scfg.amqp_url = self._amqp_url
        else:
            logging.warning("[APP] AMQP url not found; FeatureStreamer will use default: %s", scfg.amqp_url)

        # 确保交换机/队列/绑定就绪（fanout → 同时投递到 snapshot & eval）
        try:
            await self._ensure_feature_bindings(exchange_name=ex_name, queues=[q_feat, q_feat_eval])
        except Exception as e:
            logging.warning("[APP] ensure feature bindings failed: %s", e)

        logging.info("[APP] Feature publish via fanout exchange=%s → queues=[%s,%s]",
                     ex_name, q_feat, q_feat_eval)

        # 现在再创建并启动 Streamer（使用修正后的 amqp_url 与 rk）
        self.streamer = FeatureStreamer(scfg, service_id=self.service_id)
 
        self.streamer.start()

        # 2) 指标引擎（BBO/OFI/Depth/Microprice）— 从 config 注入窗口/档位
        feat_cfg = (self.cfg.get("features") or {})
        win_cfg = (feat_cfg.get("windows") or {})
        depth_levels = feat_cfg.get("depth_levels", [5, 10])
        ofi_ms = win_cfg.get("ofi_ms", 1000)
        ofi_windows = ofi_ms if isinstance(ofi_ms, (list, tuple)) else [ofi_ms]
        self.engine = FeatureEngine(
            depth_levels=depth_levels,
            ofi_windows_ms=ofi_windows,
        )

        # 3) 其他指标模块：CVD / VPIN / Profile / State(滑窗分位 & ATR%)
        self.cvd = build_cvd(self.cfg)
        self.vpin = build_vpin(self.cfg)
        # 使用多时段 Volume Profile（480/960/1440/4320）
        self.profile = build_profile_multi(self.cfg)
        self.state = build_state(self.cfg)
        # 4) 健康心跳
        if HealthBeatReporter:
            self.health = HealthBeatReporter.from_config(self.cfg, self.service_id)
            # 注册健康源（WS / Streamer / 分位状态）
            try:
                if hasattr(OKXClient, "health_snapshot"):
                    self.health.register_source("okx", lambda: self.okx.health_snapshot() if self.okx else {})
            except Exception:
                pass
            try:
                if hasattr(self.streamer, "health_snapshot"):
                    self.health.register_source("streamer", self.streamer.health_snapshot)
            except Exception:
                pass
            self.health.start()

        # 5) OKX WS 客户端
        #    在创建 WS 客户端前，先准备 MONITOR_STATUS 发布器 & 启动快照请求监听
        await self._setup_status_publisher()
        # 监听 SNAPSHOT_REQ，请求则广播当前 MONITOR_STATUS
        snap_task = asyncio.create_task(self._snapshot_req_server(), name="status-snapshot-req")
        self._main_tasks.append(snap_task)
        #    回调：订单簿增量 / 成交推送
        self.okx = OKXClient(
            cfg=self.cfg,
            on_orderbook=self._on_orderbook,
            on_trade=self._on_trade,
            on_status=self._on_ws_status,
            # 直接把多时段 TPO 采样回放给 MultiProfile
            on_tpo_sample=getattr(self.profile, "on_tpo_sample", None)
          )
        # 若用户把 bootstrap_trade_pages 配成 <=0（无限），给个醒目提示（okx_client 内仍有安全硬上限）
        try:
            pboot = (self.cfg.get("features", {}) or {}).get("profile", {}) or {}
            pages_cfg = int(pboot.get("bootstrap_trade_pages", 80))
            minutes_cfg = int(pboot.get("bootstrap_minutes", 480))
            mode_cfg = str(pboot.get("bootstrap_mode", "gap"))
            if pages_cfg <= 0:
                logging.warning("[APP] profile.bootstrap_trade_pages<=0 (unlimited). "
                                "该模式仅用于全量回灌，生产建议设置为合理上限；okx_client 内部已设置安全硬上限。")
            logging.info("[APP] backfill config: mode=%s, minutes=%d, pages=%s",
                         mode_cfg, minutes_cfg, ("unlimited" if pages_cfg <= 0 else str(pages_cfg)))

        except Exception:
            pass
        ws_task = asyncio.create_task(self.okx.start(), name="okx-ws")
        self._main_tasks.append(ws_task)
        # 6) 周期性刷新 ATR%（用于状态归一化），优先使用标记价 K 线
        vol_task = asyncio.create_task(self._vol_refresh_loop(), name="vol-refresh")
        self._main_tasks.append(vol_task)
        # 7) 启动风险评估 RPC（后台线程，不阻塞主事件循环）
        self._start_rpc_eval_thread()
        # 8) 安全关停
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, lambda s=sig: asyncio.create_task(self.stop(s)))
            except NotImplementedError:
                # Windows 等平台可能不支持
                pass

        logging.info("[APP] started; service_id=%s, instIds=%s",
                     self.service_id, ",".join(self.cfg["okx"]["instIds"]))

        await self._stop_evt.wait()

    async def stop(self, sig=None):
        if sig:
            logging.info("[APP] shutdown signal: %s", sig.name if hasattr(sig, "name") else sig)
        # 停任务（先停 WS 消费 → 停 RPC → 停心跳 → 停 MQ）
        try:
            if self.okx:
                await self.okx.close()
        except Exception as e:
            logging.warning("[APP] close okx error: %s", e)
        try:
            if self.health and hasattr(self.health, "close"):
                # 若 healthbeat 提供 async close
                maybe_close = getattr(self.health, "close")
                if asyncio.iscoroutinefunction(maybe_close):
                    await maybe_close()
                else:
                    maybe_close()
        except Exception as e:
            logging.warning("[APP] close healthbeat error: %s", e)
        try:
            if self.streamer:
                # FeatureStreamer 为线程组件，使用 stop()
                self.streamer.stop()
        except Exception as e:
            logging.warning("[APP] close streamer error: %s", e)

        # 取消附加任务
        for t in self._main_tasks:
            t.cancel()
        await asyncio.gather(*self._main_tasks, return_exceptions=True)
        # 关闭 MONITOR_STATUS AMQP 资源
        try:
            if self._status_chan and not self._status_chan.is_closed:
                await self._status_chan.close()
        except Exception as e:
            logging.debug("[APP] close status channel error: %s", e)
        try:
            if self._status_conn and not self._status_conn.is_closed:
                await self._status_conn.close()
        except Exception as e:
            logging.debug("[APP] close status conn error: %s", e)
        self._stop_evt.set()
        logging.info("[APP] stopped")

    # ----- 回调：来自 OKX WS -----
    async def _on_orderbook(self, inst_id: str, book_snapshot: dict):
        """
        订单簿增量（已在 ws/okx_client 内部完成 seqId/prevSeqId 校验与本地簿更新）
        book_snapshot 形如：
            {
              "ts": 1723351985123,
              "seqId": 123456,
              "l1": {"bid": [px, sz], "ask": [px, sz]},
              "l5": {"bids": [[px,sz],...], "asks": [...]},
              "l10": {...},  # 可选
              "raw": {...}   # 可选：原始簿（供深度指标用）
            }
        """
        if not self.engine:
            return
        # 1) 将簿传给指标引擎做滚动更新（ws 已在快照里补了扁平字段）
        self.engine.update_from_orderbook(inst_id, book_snapshot)

        # 2) Profile 的 TPO 采样（用 mid 近似市场档位）
        try:
            bp, ap = book_snapshot.get("best_bid", 0.0), book_snapshot.get("best_ask", 0.0)
            mid = (float(bp) + float(ap)) * 0.5 if (bp and ap) else None
            if mid and self.profile:
                # MultiProfile 提供 on_tpo_sample（无协程）
                if hasattr(self.profile, "on_tpo_sample"):
                    self.profile.on_tpo_sample(inst_id, mid, int(book_snapshot.get("ts") or 0))
 
        except Exception:
            pass

        # 3) 节流发布 features.snapshot
        now_ms = int(time.time() * 1000)
        last = self._last_pub_ms.get(inst_id, 0)
        if now_ms - last >= self._pub_interval_ms:
            self._last_pub_ms[inst_id] = now_ms
            snap = self.engine.build_snapshot(inst_id, now_ms)
            if snap:
                # 3.0 附带原始订单簿轻量字段，便于在 MQ 端直接核对（bids/asks/l1/l5/l10/BBO）
                try:
                    book = {}
                    for k in ("best_bid", "best_ask", "best_bid_size", "best_ask_size",
                              "bids", "asks", "l1", "l5", "l10", "seqId"):
                        v = book_snapshot.get(k)
                        if v is not None:
                            book[k] = v
                    if book:
                        snap["book"] = book
                except Exception:
                    pass
                # 3.1 状态分位样本更新（spread_bps / mp_delta_bps）
                try:
                    sp_bps = ((snap.get("features") or {}).get("spread") or {}).get("bps")
                    mpdbps = ((snap.get("features") or {}).get("microprice") or {}).get("delta_bps")
                    if sp_bps is not None:
                        self.state.update_metric(inst_id, "spread_bps", float(sp_bps), now_ms)
                    if mpdbps is not None:
                        self.state.update_metric(inst_id, "microprice_delta_bps", float(mpdbps), now_ms)
                except Exception:
                    pass

                # 3.2 汇总 CVD / VPIN / Profile / State
                try:
                    if self.cvd:
                        cvd_snap = self.cvd.build_snapshot(inst_id, now_ms)
                        if cvd_snap:
                            snap["cvd"] = cvd_snap.get("cvd")
                    if self.vpin:
                        vpin_snap = self.vpin.snapshot(inst_id)
                        if vpin_snap:
                            snap["vpin"] = vpin_snap.get("vpin")
                            snap["vpin_risk"] = vpin_snap.get("risk")
                    # ===== 多窗口 Volume Profile =====
                    if self.profile and hasattr(self.profile, "snapshot_all"):
                        mp = self.profile.snapshot_all(inst_id)  # {label: snapshot|None}
                        multi: dict = {}
                        for label, s in (mp or {}).items():
                            if not s:
                                continue
                            prof = (s.get("profile") or {})
                            vol = (prof.get("volume") or {})
                            tpo = (prof.get("tpo") or None)
                            multi[label] = {
                                "volume": {
                                    "poc": vol.get("poc"),
                                    "vah": vol.get("vah"),
                                    "val": vol.get("val"),
                                    "hvn": vol.get("hvn", []),
                                    "lvn": vol.get("lvn", []),
                                    "bins": vol.get("bins"),
                                    "total": vol.get("total"),
                                },
                                "tpo": {
                                    "poc": (tpo or {}).get("poc"),
                                    "vah": (tpo or {}).get("vah"),
                                    "val": (tpo or {}).get("val"),
                                    "bins": (tpo or {}).get("bins"),
                                    "total": (tpo or {}).get("total"),
                                } if tpo else None
                            }
                        if multi:
                            snap["profile_multi"] = multi  # <- MQ 消费方据此做风控（各窗 VAH/VAL/HVN/LVN）
                        # 额外：基于 mid 的最近节点（用于分批止盈/止损/拦截反向单）
                        try:
                            if "best_bid" in book_snapshot and "best_ask" in book_snapshot:
                                mid_px = (float(book_snapshot["best_bid"]) + float(book_snapshot["best_ask"])) * 0.5
                                nodes = self.profile.nearest_nodes(inst_id, ref_price=mid_px, max_per_side=2, dedup_bins=1.0)
                                snap["profile_nodes"] = {"ref_price": mid_px, **nodes}
                        except Exception:
                            pass
                    if self.state:
                        snap["state"] = self.state.snapshot(inst_id, keys=["spread_bps","microprice_delta_bps"], now_ms=now_ms)
                except Exception as e:
                    logging.debug("[APP] enrich snapshot error: %s", e)
                # 3.3 附加波动刻度（ATR% / median% / source）
                try:
                    vs = self._vol_state.get(inst_id)
                    if vs:
                        snap["vol"] = {
                            "atr_pct": float(vs.get("atr_pct") or 0.0),
                            "median_pct": float(vs.get("median_pct") or 0.0),
                            "source": vs.get("source"),
                            "ts": vs.get("ts"),  # 与 OKXClient.get_vol_state 一致（epoch seconds）
                        }
                except Exception:
                    pass
                try:
                    # 广播到 ex.features（内部已有 routing_key=features.{instId}）
                    snap["instId"] = inst_id
                    logging.debug("[APP] enqueue snapshot inst=%s has_profile=%s",
                                  inst_id, "profile_multi" in snap)
                    self.streamer.put(snap)
                except Exception as e:
                    logging.warning("[APP] publish features failed: %s", e)

    async def _on_trade(self, inst_id: str, trade: dict):
        """
        成交推送（ws/okx_client 已做字段统一）：
            trade = {"ts":..., "px": float, "sz": float, "side": "buy"/"sell", "tradeId": "..."}
        """
        # 交易事件：CVD / VPIN / Profile
        try:
            if self.cvd:
                self.cvd.update(inst_id, trade)
            if self.vpin:
                self.vpin.update_trade(inst_id, trade)
            if self.profile:
                # MultiProfile 使用 on_trade
                if hasattr(self.profile, "on_trade"):
                    self.profile.on_trade(inst_id, trade)
                elif hasattr(self.profile, "update_trade"):
                    self.profile.update_trade(inst_id, trade)
            # 若你的 FeatureEngine 需要逐笔（目前为 BBO 引擎，可忽略）
            if self.engine and hasattr(self.engine, "update_from_trade"):
                self.engine.update_from_trade(inst_id, trade)  # 可为 no-op
        except Exception:
            pass

    async def _on_ws_status(self, status: dict):
        """
        WS 状态（可用来触发降级、告警等）
        例如：{"event":"resync","instId":"BTC-USDT-SWAP"} / {"event":"fallback","channel":"books"}
        """
        logging.info("[WS][STATUS] %s", status)
        # 如果 OKXClient 送上来的是 MONITOR_STATUS，则转发到 status.events
        try:
            if str(status.get("event")).upper() == "MONITOR_STATUS":
                await self._publish_monitor_status(status)
        except Exception as e:
            logging.debug("[APP] forward MONITOR_STATUS failed: %s", e)
        # 其余状态仅记录或交给 healthbeat

    # ----- 评估 RPC：供 mq/rpc_eval 调用 -----
    async def _snapshot_for_eval(self, inst_id: str) -> dict:
        """
        返回“最新可用”的指标快照；若无数据，返回 error 字段并由 rpc_eval 决定 allow=false。
        """
        now_ms = int(time.time() * 1000)
        if not self.engine:
            return {"instId": inst_id, "ts": now_ms, "error": "engine-not-ready"}
        snap = self.engine.build_snapshot(inst_id, now_ms) or {}
        # 与发布路径保持一致地补齐其他模块，便于 RPC 即时评估
        try:
            if self.cvd:
                cvd_snap = self.cvd.build_snapshot(inst_id, now_ms)
                if cvd_snap:
                    snap["cvd"] = cvd_snap.get("cvd")
            if self.vpin:
                vpin_snap = self.vpin.snapshot(inst_id)
                if vpin_snap:
                    snap["vpin"] = vpin_snap.get("vpin")
                    snap["vpin_risk"] = vpin_snap.get("risk")
            if self.profile:
                prof = self.profile.snapshot(inst_id, now_ms)
                if prof:
                    snap["profile"] = prof.get("profile")
            if self.state:
                snap["state"] = self.state.snapshot(inst_id, keys=["spread_bps","microprice_delta_bps"], now_ms=now_ms)
        except Exception:
            pass
        return snap or {"instId": inst_id, "ts": now_ms, "error": "no-data"}

    async def _vol_refresh_loop(self):
        """周期刷新 ATR%（% 值，如 0.85 表示 0.85%），供状态归一化使用。"""
        if not self.okx or not self.state:
            return
        insts = list(self.cfg.get("okx", {}).get("instIds", []))
        while not self._stop_evt.is_set():
            for inst in insts:
                try:
                    vs = await self.okx.get_vol_state(inst)
                    atr_pct = float(vs.get("atr_pct") or 0.0)
                    if atr_pct > 0:
                        self.state.update_atr(inst, atr_pct=atr_pct)
                    # 缓存最近一次的波动刻度，供发布路径使用
                    self._vol_state[inst] = vs
                except Exception as e:
                    logging.debug("[APP] get_vol_state error %s: %s", inst, e)
                await asyncio.sleep(0)  # 让出循环
            await asyncio.sleep(30.0)  # 30s 刷新一次
    # ================= MONITOR_STATUS: AMQP 双通道实现 =================
    async def _setup_status_publisher(self) -> None:
        """
        建立到 RabbitMQ 的连接与通道；确保 status.events 与 q.snapshot.req 存在。
        使用默认交换机（直连）→ routing_key = 队列名（直投队列）。
        """
        if not self._amqp_url:
            # 没有 AMQP url 就不启用（只记录日志，不抛错）
            logging.warning("[APP] skip setting up status publisher: no AMQP url")
            return
        if self._status_conn and not self._status_conn.is_closed:
            return
        self._status_conn = await aio_pika.connect_robust(
            self._amqp_url, heartbeat=30,
            client_properties={"connection_name": "indicator_status"}
        )
        self._status_chan = await self._status_conn.channel()
        try:
            await self._status_chan.set_qos(prefetch_count=32)
        except Exception:
            pass
        # 确保两个队列存在（幂等）
        await self._status_chan.declare_queue(self._status_queue_name, durable=True)
        await self._status_chan.declare_queue(self._snapreq_queue_name, durable=True)
        logging.info("[APP] status publisher is ready; events=%s snap_req=%s",
                     self._status_queue_name, self._snapreq_queue_name)

    async def _publish_monitor_status(self, evt: dict) -> None:
        """
        把 OKXClient 产生的（或本服务生成的）MONITOR_STATUS 事件直投到 status.events。
        事件字段需满足消费端解析：etype/event/instId/coin/monitored/reason/msg_id/ts。
        """
        if not self._status_chan or self._status_chan.is_closed:
            await self._setup_status_publisher()
            if not self._status_chan:
                return
        # 规范化/补齐字段：coin、msg_id、ts
        inst = (evt.get("instId") or "").upper()
        if not evt.get("coin"):
            try:
                evt["coin"] = (inst.split("-", 1)[0] if inst else "")
            except Exception:
                evt["coin"] = ""
        if not evt.get("msg_id"):
            now_ms = int(time.time() * 1000)
            evt["msg_id"] = f"monstat:{inst}:{now_ms}"
        if not evt.get("ts"):
            evt["ts"] = int(time.time() * 1000)
        if not evt.get("etype"):
            evt["etype"] = "INFO"
        # 直投到队列（默认交换机）
        msg = aio_pika.Message(
            body=json.dumps(evt, separators=(",", ":")).encode("utf-8"),
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            content_type="application/json",
            message_id=str(evt.get("msg_id")),
            timestamp=int(time.time())
        )
        await self._status_chan.default_exchange.publish(
            message=msg,
            routing_key=self._status_queue_name
        )
        logging.debug("[APP] MONITOR_STATUS -> %s %s", self._status_queue_name, evt)

    async def _snapshot_req_server(self) -> None:
        """
        监听 q.snapshot.req；收到 {"type":"SNAPSHOT_REQ"}（或 "MONITOR_STATUS_REQ"）时，
        将当前所有 inst 的 MONITOR_STATUS 逐条广播到 status.events，
        供消费端重启/对齐。
        """
        if not self._amqp_url:
            logging.warning("[APP] skip snapshot-req server: no AMQP url")
            return
        # 复用连接，单独开 channel
        conn = self._status_conn or await aio_pika.connect_robust(
            self._amqp_url, heartbeat=30,
            client_properties={"connection_name": "indicator_status_snap"})
        chan = await conn.channel()
        try:
            await chan.set_qos(prefetch_count=16)
        except Exception:
            pass
        queue = await chan.declare_queue(self._snapreq_queue_name, durable=True)
        logging.info("[APP] listening snapshot-req queue=%s", self._snapreq_queue_name)

        async with queue.iterator() as it:
            async for message in it:
                async with message.process(ignore_processed=True):
                    try:
                        data = json.loads(message.body)
                    except Exception:
                        continue
                    typ = str(data.get("type") or data.get("event") or "").upper()
                    if typ in ("SNAPSHOT_REQ", "MONITOR_STATUS_REQ"):
                        await self._emit_monitor_status_snapshot(reason="snapshot_req")

    async def _emit_monitor_status_snapshot(self, reason: str = "snapshot_req") -> None:
        """
        读取 OKX 当前各合约是否 bookReady（通过 OKXClient.health_snapshot），
        按消费端期望结构发 MONITOR_STATUS。
        """
        try:
            hs = self.okx.health_snapshot() if self.okx else {}
        except Exception:
            hs = {}
        by_inst = (hs.get("inst") or {})
        now_ms = int(time.time() * 1000)
        for inst in self.cfg.get("okx", {}).get("instIds", []):
            rec = by_inst.get(inst) or {}
            ready = bool(rec.get("bookReady", False))
            evt = {
                "etype": "INFO",
                "event": "MONITOR_STATUS",
                "instId": inst,
                "coin": (inst.split("-", 1)[0] if inst else ""),
                "monitored": ready,
                "reason": reason if ready else f"{reason}:not_ready",
                "msg_id": f"monstat:{inst}:{now_ms}",
                "ts": now_ms,
            }
            await self._publish_monitor_status(evt)
    # ----- 后台线程：启动 RPC 评估服务 -----
    def _start_rpc_eval_thread(self) -> None:
        def _run():
            try:
                try:
                    with RULES_PATH.open("r", encoding="utf-8") as f:
                        user_rules = yaml.safe_load(f) or {}
                except Exception:
                    user_rules = {}
                rules = merge_rules(user_rules)
                srv = RpcServer(self.cfg, rules)
                srv.connect()
                logging.info("[APP] rpc-eval server started")
                srv.serve_forever()
            except Exception as e:
                logging.exception("[APP] rpc-eval thread exited: %s", e)

        t = threading.Thread(target=_run, name="rpc-eval", daemon=True)
        t.start()
        self._rpc_thread = t
    async def _ensure_feature_bindings(self, exchange_name: str, queues: list[str]) -> None:
        """
        fanout 交换机 + 多队列绑定：一次发布，多队列各收一份（snapshot / eval）。
        """
        if not (self._amqp_url and exchange_name and queues):
            return
        conn = await aio_pika.connect_robust(
            self._amqp_url, heartbeat=30,
            client_properties={"connection_name": "indicator_feature_bindings"}
        )
        try:
            ch = await conn.channel()
            ex = await ch.declare_exchange(exchange_name, aio_pika.ExchangeType.FANOUT, durable=True)
            for q in queues:
                qobj = await ch.declare_queue(q, durable=True)
                await qobj.bind(ex)
            await ch.close()
        finally:
            await conn.close()
# ================= main =================
async def amain():
    cfg = load_config(CONFIG_PATH)
    setup_logging(cfg)
    app = IndicatorApp(cfg)
    await app.start()


def main():
    try:
        asyncio.run(amain())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
