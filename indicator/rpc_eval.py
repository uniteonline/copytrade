# /root/copyTrade/indicator/rpc_eval.py
# -*- coding: utf-8 -*-
"""
Risk Evaluation RPC Server
--------------------------
- Consumes q.risk.eval.request
- Keeps a live in-memory feature cache by also consuming q.features.snapshot
- Applies rules.yaml to respond with: allow / priceOffsetBps / split / notes
- Implements: prefetch + manual ack; correlation_id idempotency; optional publisher confirms

Config files:
  /root/copyTrade/indicator/config.yaml
  /root/copyTrade/indicator/rules.yaml   (optional; sane defaults used if absent)

Request schema (q.risk.eval.request):
{
  "instId": "BTC-USDT-SWAP",
  "side": "buy"|"sell",
  "intendedPx": 63542.1,
  "qty": 100,                           # contracts or base units (opaque here)
  "deadlineMs": 80,                     # optional: evaluation soft TTL
  "featureSnapshot": {...}              # optional override; else uses latest cache
}

Response schema (RPC reply via `reply_to` with same `correlation_id`):
{
  "ok": true,
  "allow": true,
  "priceOffsetBps": 4.5,                # recommended maker offset (+ widen)
  "split": 1,                           # suggested child order count
  "notes": ["vpin_ok","spread_p50_ok"],
  "ttlMs": 100
}

Run:
  python3 -u /root/copyTrade/indicator/rpc_eval.py
"""

from __future__ import annotations

import os, json, time, math, logging, threading
from collections import OrderedDict
from typing import Any, Dict, Optional, Tuple, List

import pika       # pip install pika
import yaml       # pip install pyyaml

# --------------------------- Paths & Config ---------------------------

ROOT = "/root/copyTrade/indicator"
CFG_PATH = os.path.join(ROOT, "config.yaml")
RULES_PATH = os.path.join(ROOT, "rules.yaml")

log = logging.getLogger("rpc_eval")


def load_yaml(path: str) -> dict:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    except Exception:
        return {}


def setup_logging(level: str) -> None:
    lvl = getattr(logging, str(level or "INFO").upper(), logging.INFO)
    logging.basicConfig(
        level=lvl,
        format="%(asctime)s.%(msecs)03d %(levelname)s [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )


# --------------------------- Idempotency Cache ---------------------------

class IdempotencyCache:
    """Simple in-memory LRU with TTL keyed by correlation_id."""
    def __init__(self, maxsize: int = 4096, ttl_sec: int = 120):
        self._max = maxsize
        self._ttl = ttl_sec
        self._lock = threading.RLock()
        self._data: OrderedDict[str, Tuple[float, bytes]] = OrderedDict()

    def get(self, key: Optional[str]) -> Optional[bytes]:
        if not key:
            return None
        now = time.time()
        with self._lock:
            item = self._data.get(key)
            if not item:
                return None
            ts, payload = item
            if now - ts > self._ttl:
                # expired
                self._data.pop(key, None)
                return None
            # touch
            self._data.move_to_end(key)
            return payload

    def put(self, key: Optional[str], payload: bytes) -> None:
        if not key:
            return
        now = time.time()
        with self._lock:
            self._data[key] = (now, payload)
            self._data.move_to_end(key)
            # evict
            while len(self._data) > self._max:
                self._data.popitem(last=False)


# --------------------------- Feature Store ---------------------------

class FeatureStore:
    """
    Keeps the latest features per instId fed from q.features.snapshot.
    Thread-safe read/write.
    """
    def __init__(self):
        self._lock = threading.RLock()
        self._by_inst: Dict[str, dict] = {}

    def upsert(self, inst_id: str, snapshot: dict) -> None:
        """
        兼容两种发布格式：
          a) 直接扁平：{ microprice:{...}, spread_bps:..., ofi:{...}, vpin:{...}, ... }
          b) 包一层：{ type:..., ts:..., instId:..., feat:{ ... }, ... }
             其中 feat 内部可能再有 features 分组：features.spread.bps / features.microprice / ofi.sum
        这里统一“展平为顶层键”，以适配 evaluate() 的读取逻辑。
        """
        def _flatten(msg: dict) -> dict:
            data = (msg.get("feat") or msg).copy()
            # 若存在 features 分组，映射常用键到顶层
            feats = data.get("features") or {}
            if feats:
                spread_bps = ((feats.get("spread") or {}).get("bps"))
                micro = (feats.get("microprice") or {})
                ofi_sum = ((feats.get("ofi") or {}).get("sum")) or {}
                if spread_bps is not None:
                    data["spread_bps"] = spread_bps
                if micro:
                    data["microprice"] = micro
                if ofi_sum:
                    data["ofi"] = ofi_sum
                # 可保留原结构，也可删除
                # data.pop("features", None)
            return data
        flat = _flatten(snapshot or {})
        with self._lock:
            self._by_inst[inst_id] = flat

    def get(self, inst_id: str) -> Optional[dict]:
        with self._lock:
            return self._by_inst.get(inst_id)


# --------------------------- Rule Engine ---------------------------

DEFAULT_RULES = {
    "vpin": {
        "block_when_toxic": True,       # snapshot['vpin']['risk']['toxic'] → block
        "split_thresholds": [           # (vpin, split)
            [0.70, 2],
            [0.78, 3],
            [0.85, 5]
        ]
    },
    "spread": {
        "cap_bps": 15.0,                # hard cap; if >cap → widen offset to ≥cap/2
        "pctl_guard": "p80"             # reserved if you wire state quantiles in
    },
    "microprice": {
        "max_abs_offset_bps": 12.0      # clamp |microprice_delta_bps|
    },
    "ofi": {
        "direction_confirm": True,      # 需方向一致
        "window_ms": 1000,              # 取哪个窗口
        "block_abs": 50.0,              # |OFI| 超过此值且与方向相反 → 直接 allow=false（反向单拦截）
        "offset_k_per_ofi": 0.0         # 每 1 OFI 映射为多少 bps（默认 0=不影响偏移）
 
    },
    "split": {
        "base": 1,
        "max": 8
    },
    "response": {
        "default_ttl_ms": 120
    }
}


def merge_rules(user_rules: dict) -> dict:
    def deep_update(a: dict, b: dict) -> dict:
        for k, v in b.items():
            if isinstance(v, dict) and isinstance(a.get(k), dict):
                a[k] = deep_update(a[k], v)
            else:
                a[k] = v
        return a
    rules = dict(DEFAULT_RULES)
    if user_rules:
        rules = deep_update(rules, user_rules)
    return rules


# --------------------------- Evaluation Logic ---------------------------

def _sign_for_side(side: str) -> int:
    s = (side or "").lower()
    if s == "buy":
        return +1
    if s == "sell":
        return -1
    return 0


def evaluate(req: dict, feat: dict, rules: dict) -> dict:
    """
    Core policy:
      1) VPIN toxic → block (if enabled)
      2) Price offset: base on microprice delta (clamped) and spread guard
      3) Directional sanity: OFI sign vs side (optional soft/hard)
      4) Split: escalate by VPIN level
    """
    notes: List[str] = []
    allow = True
    price_offset_bps = 0.0
    split = int(rules["split"]["base"])

    side = str(req.get("side", "")).lower()
    intended_px = float(req.get("intendedPx") or 0.0)
    inst = req.get("instId")

    # 1) VPIN / Toxicity
    vpin = (feat or {}).get("vpin") or {}
    toxic = bool(((vpin.get("risk") or {}).get("toxic")) or False)
    vpin_val = float(vpin.get("ma") or vpin.get("ewma") or 0.0)

    if rules["vpin"]["block_when_toxic"] and toxic:
        allow = False
        notes.append("blocked_vpin_toxic")
    else:
        notes.append("vpin_ok")

    # 2) Microprice → offset
    mp = (feat or {}).get("microprice") or {}
    mp_delta_bps = float(mp.get("delta_bps") or mp.get("deltaBps") or 0.0)
    cap = float(rules["microprice"]["max_abs_offset_bps"])
    mp_delta_bps = max(-cap, min(cap, mp_delta_bps))
    # Buy: if microprice > mid (正),建议正向 offset（吃近一点）；Sell 相反
    sgn = _sign_for_side(side)
    price_offset_bps = max(0.0, sgn * mp_delta_bps)
    if price_offset_bps > 0:
        notes.append(f"microprice_bias_{price_offset_bps:.2f}bps")

    # 3) Spread guard：若当前点差很大，建议至少一半点差的偏移（顺势保护：顺势时趋近，逆势时远离）
    spread_bps = float((feat or {}).get("spread_bps") or 0.0)
    spread_cap = float(rules["spread"]["cap_bps"])
    if spread_bps > 0:
        base_guard = min(spread_cap / 2.0, spread_bps / 2.0)
        # 若 microprice 与下单方向一致（顺势）：允许略微“贴近”挂单以保护盈利（更易成交）
        if sgn * mp_delta_bps > 0:
            price_offset_bps = max(price_offset_bps, base_guard * 0.8)
            notes.append("trend_protect_profit")
        else:
            # 逆势：适度加大 offset，降低误成交（亏损保护）
            price_offset_bps = max(price_offset_bps, base_guard * 1.2)
            notes.append("trend_protect_loss")
    if spread_bps > spread_cap:
        notes.append(f"spread_wide_{spread_bps:.2f}bps")

    # 4) OFI 方向一致性（软/硬）
    if rules["ofi"]["direction_confirm"]:
        # 以 window_ms 选择的 ofi 指标（例如 ofi_ms_1000）
        ofi = (feat or {}).get("ofi") or {}
        ofi_w = ofi.get(str(rules["ofi"]["window_ms"])) or ofi.get("w1s")
        ofi_val = float(ofi_w or 0.0)
        if ofi_val * sgn < 0:
            # 方向反向：若绝对值超过 block_abs → 直接拦截；否则软处理
            block_abs = float(rules["ofi"].get("block_abs", 0.0))
            k_ofi = float(rules["ofi"].get("offset_k_per_ofi", 0.0))
            if abs(ofi_val) >= block_abs > 0.0:
                allow = False
                notes.append("blocked_ofi_opposes_strong")
            else:
                notes.append("ofi_opposes_side")
                if k_ofi > 0.0:
                    price_offset_bps = max(price_offset_bps, k_ofi * max(0.0, abs(ofi_val)))
                split = max(split, 2)

    # 5) Escalate split by VPIN level
    for thr, n in rules["vpin"]["split_thresholds"]:
        if vpin_val >= float(thr):
            split = max(split, int(n))
    split = min(split, int(rules["split"]["max"]))

    # Clamp and finalize
    price_offset_bps = max(0.0, float(price_offset_bps))
    resp = {
        "ok": True,
        "allow": bool(allow),
        "priceOffsetBps": float(round(price_offset_bps, 4)),
        "split": int(split),
        "notes": notes,
        "instId": inst,
        "side": side,
        "ttlMs": int(rules["response"]["default_ttl_ms"])
    }
    return resp


# --------------------------- MQ Plumbing ---------------------------

class RpcServer:
    def __init__(self, cfg: dict, rules: dict):
        self.cfg = cfg
        self.rules = rules
        self.features = FeatureStore()
        self._feat_queue_name: Optional[str] = None

        mq = cfg.get("mq", {})
        self.amqp_url = mq.get("host")
        self.exchanges = mq.get("exchanges", {})
        self.queues = mq.get("queues", {})
        self.prefetch = int(mq.get("prefetch", 8))
        self.use_confirms = bool(mq.get("publisher_confirms", True))

        if not self.amqp_url:
            raise RuntimeError("config.mq.host (amqp URL) is required")

        self._conn: Optional[pika.BlockingConnection] = None
        self._ch_req: Optional[pika.adapters.blocking_connection.BlockingChannel] = None
        # 注意：特征消费者在独立线程内自建连接与通道，避免跨线程共享
        self._ch_feat: Optional[pika.adapters.blocking_connection.BlockingChannel] = None  # unused in main thread


    # ---- Connection / Topology ----
    def connect(self):
        params = pika.URLParameters(self.amqp_url)
        params.heartbeat = 30
        params.blocked_connection_timeout = 60
        self._conn = pika.BlockingConnection(params)

        # Request channel (RPC)
        self._ch_req = self._conn.channel()
        # Prefetch per-consumer (global_qos=False) is the general best practice.
        # See RabbitMQ docs on consumer prefetch.
        self._ch_req.basic_qos(prefetch_count=self.prefetch, global_qos=False)
        if self.use_confirms:
            # Publisher confirms for reliable replies. :contentReference[oaicite:3]{index=3}
            self._ch_req.confirm_delivery()

        # Declare queues we consume
        q_eval = self.queues.get("eval_req", "q.risk.eval.request")
        # 选定**特征**队列（优先 eval 专用，其次 snapshot）
        self._feat_queue_name = (
            self.queues.get("feature_snapshot_eval")
            or self.queues.get("feature_snapshot", "q.features.snapshot")
        )
        self._ch_req.queue_declare(queue=q_eval, durable=True)

        # Start features consumer in a background thread
        self._feat_thread = threading.Thread(target=self._consume_features, daemon=True)
        self._feat_thread.start()

        # Bind health exchange if present (optional)
        # exchanges: features, risk_eval, health
        # The server replies directly to `reply_to` (RPC), so no extra bind is required.

        # Start RPC consumer
        self._start_rpc_consumer(q_eval)

    # ---- Feature consumer ----
    def _consume_features(self):
        """
        特征消费者在独立线程里**独立建立连接与通道**。
        Pika 官方文档指出：不要跨线程共享连接/通道，应当每个线程一个连接。¹
        """
        # 使用在 connect() 中选定的队列（优先 eval）
        q_feat = (
            self._feat_queue_name
            or self.queues.get("feature_snapshot_eval")
            or self.queues.get("feature_snapshot", "q.features.snapshot")
        )
        params = pika.URLParameters(self.amqp_url)
        params.heartbeat = 30
        params.blocked_connection_timeout = 60

        try:
            conn = pika.BlockingConnection(params)
            ch = conn.channel()
            # per-consumer prefetch（global_qos=False）是推荐姿势，可避免单消费者过载²
            ch.basic_qos(prefetch_count=32, global_qos=False)
            ch.queue_declare(queue=q_feat, durable=True)
            log.info("feature consumer started on queue=%s", q_feat)
            def _on_feat(ch_, method, props, body: bytes):
                try:
                    payload = json.loads(body.decode("utf-8"))
                    inst = payload.get("instId")
                    if inst:
                        self.features.upsert(inst, payload)
                except Exception as e:
                    log.warning("feature decode failed: %s", e)
                finally:
                    ch_.basic_ack(method.delivery_tag)

            ch.basic_consume(queue=q_feat, on_message_callback=_on_feat, auto_ack=False)
            ch.start_consuming()
        except Exception as e:
            log.exception("Feature consumer stopped: %s", e)
        finally:
            try:
                ch.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass  

    # ---- RPC consumer ----
    def _start_rpc_consumer(self, q_name: str):
        def _on_req(ch, method, props, body: bytes):
            corr_id = getattr(props, "correlation_id", None)
            reply_to = getattr(props, "reply_to", None)

            # Idempotent re-delivery handling
            cached = self.idem.get(corr_id)
            if cached is not None:
                self._safe_reply(ch, reply_to, corr_id, cached)
                ch.basic_ack(method.delivery_tag)
                return

            # Decode request
            try:
                req = json.loads(body.decode("utf-8"))
                inst = req.get("instId")
                feat = req.get("featureSnapshot") or (self.features.get(inst) if inst else None)
                if not feat:
                    feat = {}
                resp = evaluate(req, feat, self.rules)
                payload = json.dumps(resp, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
            except Exception as e:
                log.exception("eval failed: %s", e)
                resp = {"ok": False, "allow": False, "error": str(e)}
                payload = json.dumps(resp, separators=(",", ":"), ensure_ascii=False).encode("utf-8")

            # Reply then ack
            self._safe_reply(ch, reply_to, corr_id, payload)
            self.idem.put(corr_id, payload)
            ch.basic_ack(method.delivery_tag)

        self._ch_req.basic_consume(queue=q_name, on_message_callback=_on_req, auto_ack=False)

    def _safe_reply(self, ch, reply_to: Optional[str], corr_id: Optional[str], payload: bytes):
        if not reply_to:
            log.warning("no reply_to; dropping response (corr_id=%s)", corr_id)
            return
        props = pika.BasicProperties(
            content_type="application/json",
            delivery_mode=1,  # non-persistent for RPC reply
            correlation_id=corr_id
        )
        # RPC response pattern via reply_to/correlation_id per RabbitMQ tutorial. :contentReference[oaicite:4]{index=4}
        ch.basic_publish(
            exchange="",
            routing_key=reply_to,          # direct reply-to or user queue
            properties=props,
            body=payload,
            mandatory=False
        )
        # If publisher confirms enabled, ensure broker acked publish. :contentReference[oaicite:5]{index=5}
        # (In BlockingConnection, confirm_delivery() makes publish raise on failure.)

    def serve_forever(self):
        assert self._ch_req is not None
        log.info("RPC eval server is up; consuming ...")
        try:
            self._ch_req.start_consuming()
        except KeyboardInterrupt:
            log.info("Interrupted, shutting down ...")
        finally:
            try:
                if self._ch_req and self._ch_req.is_open:
                    self._ch_req.stop_consuming()
            except Exception:
                pass
            try:
                if self._conn and self._conn.is_open:
                    self._conn.close()
            except Exception:
                pass


# --------------------------- Entrypoint ---------------------------

def main():
    cfg = load_yaml(CFG_PATH)
    setup_logging(((cfg.get("service") or {}).get("log_level")) or "INFO")
    log.info("loading config from %s", CFG_PATH)

    user_rules = load_yaml(RULES_PATH)
    if user_rules:
        log.info("rules loaded from %s", RULES_PATH)
    else:
        log.warning("rules.yaml not found; using defaults")
    rules = merge_rules(user_rules)

    srv = RpcServer(cfg, rules)
    srv.connect()
    srv.serve_forever()


if __name__ == "__main__":
    main()
