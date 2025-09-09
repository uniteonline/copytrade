# tests/test_indicator_integration.py
# -*- coding: utf-8 -*-
import os
import sys
import time
import json
import math
import threading
from pathlib import Path

import pytest
import pika
import yaml

# --- 将 repo 根目录加入 sys.path，便于按文件路径导入 ---
REPO_ROOT = Path("/root/copyTrade/indicator").resolve()
assert REPO_ROOT.exists(), f"repo path not found: {REPO_ROOT}"
sys.path.insert(0, str(REPO_ROOT))

# ---- 兼容导入：core/streamer 直接 import；其他模块按文件路径装载（避免无 __init__.py） ----
from core import FeatureEngine  # 你的 features 核心（包含 microprice/spread/OFI/topN）
from streamer import FeatureStreamer, StreamerConfig

import importlib.util


def _load_from_path(mod_name: str, file_path: Path):
    spec = importlib.util.spec_from_file_location(mod_name, str(file_path))
    if spec is None or spec.loader is None:
        raise ImportError(f"cannot import {mod_name} from {file_path}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# features/cvd.py
CVD = _load_from_path("features_cvd", REPO_ROOT / "features" / "cvd.py")
# features/profile.py
PROFILE = _load_from_path("features_profile", REPO_ROOT / "features" / "profile.py")
# vpin.py
VPIN = _load_from_path("vpin", REPO_ROOT / "vpin.py")
# state.py
STATE = _load_from_path("state", REPO_ROOT / "state.py")


# -------------------- RabbitMQ 连接 & 工具 --------------------

def _load_cfg():
    cfg_path = REPO_ROOT / "config.yaml"
    with cfg_path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _get_amqp_url():
    env = os.getenv("AMQP_URL")
    if env:
        return env
    cfg = _load_cfg()
    return ((cfg.get("mq") or {}).get("host")) or "amqp://guest:guest@localhost:5672/"


class MQ:
    """简单的 AMQP 帮手：声明 exchange、临时队列、绑定、收消息"""
    def __init__(self, url: str):
        params = pika.URLParameters(url)
        params.heartbeat = 30
        params.blocked_connection_timeout = 30
        self.conn = pika.BlockingConnection(params)
        self.ch = self.conn.channel()

    def declare_exchange(self, name: str, typ: str = "fanout"):
        self.ch.exchange_declare(exchange=name, exchange_type=typ, durable=True, passive=False)

    def temp_queue_bind(self, exchange: str, routing_key: str = "#"):
        q = self.ch.queue_declare(queue="", exclusive=True, durable=False, auto_delete=True)
        qname = q.method.queue
        # 绑定（fanout 无视 routing key；topic/direct 需要匹配）
        self.ch.queue_bind(queue=qname, exchange=exchange, routing_key=routing_key)
        return qname

    def consume_one(self, queue: str, timeout_sec: float = 3.0):
        self.conn.process_data_events(0)
        deadline = time.time() + timeout_sec
        while time.time() < deadline:
            method, props, body = self.ch.basic_get(queue=queue, auto_ack=True)
            if body:
                return method, props, body
            time.sleep(0.02)
            self.conn.process_data_events(0.05)
        return None, None, None

    def close(self):
        try:
            if self.ch and self.ch.is_open:
                self.ch.close()
        finally:
            try:
                if self.conn and self.conn.is_open:
                    self.conn.close()
            except Exception:
                pass


# -------------------- 造数据（订单簿 & 成交） --------------------

def _push_orderbook_series(engine: FeatureEngine, inst: str):
    """
    制造一小段 BBO 变化，以便生成：
      - 有效 mid/spread/microprice & microprice_delta_bps
      - OFI（1s 窗口）
      - topN depth / queue imbalance
    """
    now = lambda: int(time.time() * 1000)

    # 基础价位（逐步上移 + 尺寸变化产生 OFI）
    frames = [
        # ts, best_bid, best_ask, bid_sz, ask_sz, bids(list), asks(list)
        (now(), 100.00, 100.10, 5.0, 5.0),
        (now()+50, 100.01, 100.11, 6.0, 4.5),
        (now()+100, 100.03, 100.13, 7.0, 4.0),
        (now()+150, 100.02, 100.12, 5.5, 5.2),  # 反向一下
        (now()+200, 100.05, 100.15, 7.5, 4.2),
    ]

    # 构造 top10，用简单阶梯
    def mk_books(bp, ap):
        bids = [[round(bp - i * 0.01, 2), 3.0 + i * 0.2] for i in range(10)]
        asks = [[round(ap + i * 0.01, 2), 2.5 + i * 0.2] for i in range(10)]
        return bids, asks

    for ts, bp, ap, bs, asz in frames:
        bids, asks = mk_books(bp, ap)
        engine.update_from_orderbook(inst, {
            "ts": ts,
            "best_bid": bp, "best_ask": ap,
            "best_bid_size": bs, "best_ask_size": asz,
            "bids": bids, "asks": asks
        })
        time.sleep(0.01)


def _push_trades_cvd_vpin(inst: str, cvd: "CVD.CVDTracker", vpin: "VPIN.VPINEngine", px0: float):
    """
    制造一小段买/卖成交：让 CVD 有窗口，VPIN 桶尽快填满。
    通过把 VPIN 桶体量配置得很小来“快速出数”。
    """
    now = lambda: int(time.time() * 1000)
    trades = []
    # 买多一点，再来几笔卖，穿插价格轻微波动
    for i in range(20):
        side = "buy" if i % 3 != 0 else "sell"
        px = px0 + (0.02 * (1 if side == "buy" else -1)) + 0.001 * i
        sz = 5 + (i % 4)  # 5~8
        t = {"ts": now() + i * 20, "px": px, "sz": sz, "side": side}
        trades.append(t)

    for t in trades:
        cvd.update(inst, t)
        vpin.update_trade(inst, t)
        time.sleep(0.005)


def _sample_profile(inst: str, profile: "PROFILE.ProfileEngine", mids: list, ts0: int):
    """用 mid 采样做 TPO（30s 一次的近似；这儿缩短间隔）"""
    for i, m in enumerate(mids):
        profile.update_l1(inst, mid_px=m, ts_ms=ts0 + i * 500)  # 0.5s 做一个“槽”


# -------------------- 构建并发布快照 --------------------

def _build_combined_snapshot(inst: str,
                             fe_snap: dict,
                             cvd_snap: dict,
                             vpin_snap: dict,
                             prof_snap: dict,
                             state_snap: dict) -> dict:
    """
    统一成 FeatureStreamer 期望的 payload（顶层会被打包到 feat 内）
    - fe_snap 是 core.FeatureEngine.build_snapshot 的结果（含 features.* / bbo 等）
    - 其他模块我们统一放到同一层，方便下游消费
    """
    now = int(time.time() * 1000)
    payload = {
        "instId": inst,
        "ts": now,
        # 保留 FeatureEngine 的结构（features.* / bbo）
        "features": fe_snap.get("features", {}),
        "bbo": fe_snap.get("bbo", {}),
        "windows_ms": fe_snap.get("windows_ms", []),
        # 追加其余指标
        "cvd": (cvd_snap or {}).get("cvd"),
        "vpin": (vpin_snap or {}).get("vpin"),
        "risk": (vpin_snap or {}).get("risk"),
        "profile": (prof_snap or {}).get("profile"),
        "state": state_snap or {},
    }
    return payload


# -------------------- 测试：指标齐全且能通过 MQ --------------------

@pytest.mark.integration
def test_indicators_streamed_to_mq_and_consumed_ok():
    """
    目标：
      1) 在当前 indicator 系统内得到风控所需所有关键指标：
         - 修正入场价：microprice.delta_bps、spread.bps
         - 上涨盈利保护/下跌亏损保护：spread 守卫（我们验证 spread.bps 存在）
         - 拦截反向开单：OFI（至少 1s 窗口）
         - 另外：CVD（sec/min 窗口）、VPIN（risk.toxic）、Profile(POC/VAH/VAL)、State(分位/ATR快照)
      2) 这些指标被打包并发布到 ex.features，消费者可从队列中收到并解析
    """
    inst = "BTC-USDT-SWAP"

    # 1) 指标引擎
    fe = FeatureEngine(depth_levels=[5, 10], ofi_windows_ms=[1000, 2000, 3000])

    # CVD：窗口 10s / 1min
    cvd = CVD.build_from_config({"features": {"windows": {"cvd_secs": [10], "cvd_mins": [1]}}})
    # VPIN：把桶体量调小，快速填满
    vpin = VPIN.VPINEngine(bucket_kind="quote", bucket_size=20_000.0, window_buckets=10,
                           use_ewma=True, ewma_alpha=0.3, threshold=0.6)
    # Profile：设定一个合理的 price_step（比如 0.1）
    profile = PROFILE.ProfileEngine(price_step=0.1, use_quote=False, value_area_pct=0.70,
                                    tpo_sample_ms=300, rolling_ms=None, session_mode="none")
    # State：60s 窗口
    state = STATE.StateEngine(window_ms=60_000, algo="exact")

    # 2) 造数据
    _push_orderbook_series(fe, inst)
    # 计算一次快照（core）
    fe_snap = fe.build_snapshot(inst, int(time.time() * 1000))
    assert fe_snap, "FeatureEngine snapshot is None"

    # CVD/VPIN：用 fe_snap 的 mid 附近作为价格基准造成交
    mid0 = fe_snap["bbo"]["mid"]
    _push_trades_cvd_vpin(inst, cvd, vpin, px0=mid0)
    cvd_snap = cvd.build_snapshot(inst)
    vpin_snap = vpin.snapshot(inst)

    # Profile：用一段 mid 采样
    ts0 = int(time.time() * 1000)
    mids = [mid0 - 0.1, mid0 - 0.05, mid0, mid0 + 0.03, mid0 + 0.06, mid0 + 0.1]
    _sample_profile(inst, profile, mids, ts0)
    prof_snap = profile.snapshot(inst)

    # State：把当前 spread_bps / microprice.delta_bps 推入滑窗，并导出分位+ATR快照
    spread_bps = fe_snap["features"]["spread"]["bps"]
    mp_dbps = fe_snap["features"]["microprice"]["delta_bps"]
    state.update_metric(inst, "spread_bps", spread_bps)
    state.update_metric(inst, "microprice_delta_bps", mp_dbps)
    # 给个 ATR%（假设 0.8%），用于 normalize 的可用性
    state.update_atr(inst, atr_pct=0.8)
    state_snap = state.snapshot(inst)

    # 3) 合并为单个 features 快照
    combined = _build_combined_snapshot(inst, fe_snap, cvd_snap, vpin_snap, prof_snap, state_snap)

    # 4) 启动 FeatureStreamer 并发布到 ex.features
    cfg_yaml = _load_cfg()
    ex_features = ((cfg_yaml.get("mq") or {}).get("exchanges") or {}).get("features", "ex.features")
    amqp_url = _get_amqp_url()

    # 开启 streamer
    s_cfg = StreamerConfig(
        amqp_url=amqp_url,
        exchange=ex_features,
        exchange_type="fanout",           # 你的默认；若用 topic 也没问题，下面绑定用 '#'
        routing_key_fmt="features.{instId}",
        interval_ms=250,
        ttl_ms=2000,
        publisher_confirms=True,          # 生产场景建议开启
        delivery_mode_persistent=False,
        queue_maxsize=1000,
        overflow_policy="coalesce",
        batch_max=100,
        reconnect_backoff_min=0.5,
        reconnect_backoff_max=3.0,
        heartbeat=30,
        blocked_connection_timeout=15,
        declare_exchange=True,
    )
    streamer = FeatureStreamer(s_cfg, service_id="test-indicator")
    streamer.start()
    try:
        # 建立临时消费者绑定，验证确实能收到发布
        mq = MQ(amqp_url)
        try:
            # 确保 exchange 存在（streamer 也会 declare；这里双保险）
            mq.declare_exchange(ex_features, typ="fanout")
            qname = mq.temp_queue_bind(ex_features, routing_key="#")

            # 发布
            streamer.put(combined)
            # flush 等待 streamer 定时批量发送
            time.sleep(0.4)

            method, props, body = mq.consume_one(qname, timeout_sec=3.0)
            assert body, "did not receive a features message from RabbitMQ"
            msg = json.loads(body.decode("utf-8"))
            assert msg.get("type") == "feature.snapshot"
            assert msg.get("instId") == inst
            feat = msg.get("feat") or {}

            # ------- 关键断言：风控所需指标齐全 -------

            # 1) microprice & spread（用于“修正入场价 / 盈亏保护”）
            micro = ((feat.get("features") or {}).get("microprice")) or {}
            spread = ((feat.get("features") or {}).get("spread")) or {}
            assert isinstance(micro.get("delta_bps"), (int, float))
            assert isinstance(spread.get("bps"), (int, float))

            # 2) OFI 1s 窗口（用于“拦截反向开单”的方向一致性）
            ofi = ((feat.get("features") or {}).get("ofi") or {}).get("sum") or {}
            assert "1000" in ofi, "OFI 1s window missing"

            # 3) CVD（窗口）
            cvd_blk = feat.get("cvd") or {}
            assert (cvd_blk.get("windows") or {}).get("sec") or (cvd_blk.get("windows") or {}).get("min")

            # 4) VPIN（含风险标志 toxic）
            vpin_blk = feat.get("vpin") or {}
            risk_blk = feat.get("risk") or {}
            assert any(k in vpin_blk for k in ("ma", "ewma")), "VPIN value missing"
            assert "toxic" in (risk_blk or {}), "VPIN risk.toxic missing"

            # 5) Profile 结构锚点（POC/VAH/VAL 之一必须存在）
            prof_blk = feat.get("profile") or {}
            vol_prof = prof_blk.get("volume") or {}
            assert any(vol_prof.get(k) is not None for k in ("poc", "vah", "val")), "profile anchors missing"

            # 6) State 分位/ATR 快照存在
            st = feat.get("state") or {}
            assert "_atr" in st and "atr_pct" in (st["_atr"] or {}), "state ATR snapshot missing"
        finally:
            mq.close()
    finally:
        streamer.stop()


# -------------------- （可选）测试：RPC 评估闭环 --------------------

@pytest.mark.integration
def test_rpc_eval_roundtrip_if_server_running():
    """
    若你已在同一 RabbitMQ 上运行了 rpc_eval.py（风险评估服务），
    这里会发一条请求到 q.risk.eval.request，并等待 reply_to 的应答。
    没有服务在跑的话，本用例会被 xfail。
    """
    cfg = _load_cfg()
    amqp_url = _get_amqp_url()
    ex_features = ((cfg.get("mq") or {}).get("exchanges") or {}).get("features", "ex.features")
    q_eval = ((cfg.get("mq") or {}).get("queues") or {}).get("eval_req", "q.risk.eval.request")

    # 先投一条 features 快照，确保评估服务的特征缓存有东西可用
    mq = MQ(amqp_url)
    try:
        mq.declare_exchange(ex_features, typ="fanout")
        # 用一个简化的 features（至少包含 features.spread/microprice.ofi）
        minimal_feat = {
            "instId": "BTC-USDT-SWAP",
            "ts": int(time.time() * 1000),
            "features": {
                "spread": {"abs": 0.1, "bps": 8.0},
                "microprice": {"value": 100.1, "delta": 0.02, "delta_bps": 2.0},
                "ofi": {"sum": {"1000": 5.0}}
            },
            "bbo": {"mid": 100.05, "best_bid": 100.00, "best_ask": 100.10,
                    "best_bid_size": 5.0, "best_ask_size": 5.0}
        }
        # 直接按 FeatureStreamer 的消息结构手工发布
        body = json.dumps({
            "type": "feature.snapshot",
            "svc": "test-indicator",
            "ts": minimal_feat["ts"],
            "instId": minimal_feat["instId"],
            "feat": minimal_feat
        }, ensure_ascii=False).encode("utf-8")
        mq.ch.basic_publish(exchange=ex_features, routing_key="features.BTC-USDT-SWAP", body=body)

        # 发送评估请求（RPC pattern: reply_to + correlation_id）
        reply_q = mq.ch.queue_declare(queue="", exclusive=True, durable=False, auto_delete=True).method.queue
        corr_id = f"test-{int(time.time()*1000)}"
        req = {
            "instId": "BTC-USDT-SWAP",
            "side": "buy",
            "intendedPx": 100.07,
            "qty": 10
        }
        props = pika.BasicProperties(content_type="application/json",
                                     delivery_mode=1,
                                     correlation_id=corr_id,
                                     reply_to=reply_q)
        try:
            mq.ch.basic_publish(exchange="", routing_key=q_eval,
                                properties=props, body=json.dumps(req).encode("utf-8"))
        except Exception:
            pytest.xfail("RPC eval server not running or queue not declared")

        # 等待回复
        deadline = time.time() + 3.0
        resp = None
        while time.time() < deadline:
            m, p, b = mq.ch.basic_get(queue=reply_q, auto_ack=True)
            if b:
                r = json.loads(b.decode("utf-8"))
                if isinstance(r, dict) and r.get("correlation_id") == corr_id:
                    # 某些实现会把 corr_id 放在 props；我们的服务把它放在 AMQP props 上
                    pass
                resp = r
                break
            time.sleep(0.05)
            mq.conn.process_data_events(0.05)

        if resp is None:
            pytest.xfail("no RPC response — is rpc_eval.py running?")

        assert resp.get("ok") is True
        assert "allow" in resp
        assert "priceOffsetBps" in resp
        assert isinstance(resp.get("notes"), list)
    finally:
        mq.close()
