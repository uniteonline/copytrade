import os
import yaml
import logging      
import time, random, functools
import re
import sys
import uuid
import pika
from math import isfinite

import okx.Account as Account        # 账户与余额、杠杆设置接口
import okx.MarketData as MarketData  # 市场数据接口，包括交易对查询
import okx.PublicData  as PublicData 
from okx.Trade import TradeAPI
import json, pathlib
import math
from typing import Dict, Any, DefaultDict, Tuple, Callable
from collections import defaultdict
from contextlib import suppress
from risk_guardian import RiskGuardian, quick_eval_from_event
from decimal import Decimal, ROUND_FLOOR

# ============================
# 一次性拉取 Volume Profile 辅助
# ============================
# RabbitMQ 连接参数优先读取环境变量，无则用默认：
#   RABBIT_HOST=127.0.0.1, RABBIT_PORT=5672, RABBIT_VHOST=/
#   RABBIT_USER=monitor, RABBIT_PASSWORD=P@ssw0rd
# 风控交换机：RISK_EXCHANGE=ex.risk.eval ；routing_key 为空串
RISK_EXCHANGE = os.getenv("RISK_EXCHANGE", "ex.risk.eval")
ROUTING_KEY   = os.getenv("RISK_ROUTING_KEY", "")

def rpc_indicator_snapshot(inst_id: str, bars: int = 240, timeout: float = 3.0) -> dict:
    cred = pika.PlainCredentials(
        os.getenv("RABBIT_USER", "monitor"),
        os.getenv("RABBIT_PASSWORD", "P@ssw0rd")
    )
    params = pika.ConnectionParameters(
        host=os.getenv("RABBIT_HOST", "127.0.0.1"),
        port=int(os.getenv("RABBIT_PORT", "5672")),
        virtual_host=os.getenv("RABBIT_VHOST", "/"),
        heartbeat=30,
        blocked_connection_timeout=30,
        credentials=cred
    )
    conn = pika.BlockingConnection(params)
    ch   = conn.channel()
    reply_q = ch.queue_declare(queue="", exclusive=True, durable=False, auto_delete=True)
    corr_id = str(uuid.uuid4())
    props = pika.BasicProperties(
        reply_to=reply_q.method.queue,
        correlation_id=corr_id,
        content_type="application/json",
        delivery_mode=1
    )
    body = json.dumps({
        "op": "features.snapshot",
        "instId": inst_id,
        "window": {"mode": "bars", "n": int(bars)},
        "fields": ["bbo", "profile", "vol"]
    }).encode("utf-8")
    ch.basic_publish(exchange=RISK_EXCHANGE, routing_key=ROUTING_KEY, properties=props, body=body)

    result = None
    def _on_resp(ch_, method, properties, body):
        nonlocal result
        if properties.correlation_id == corr_id:
            try:
                result = json.loads(body.decode("utf-8"))
            except Exception:
                result = {"ok": False, "error": "bad-json", "raw": body.decode("utf-8", "ignore")}
            ch_.basic_cancel(consumer_tag=corr_id)
    ch.basic_consume(queue=reply_q.method.queue, on_message_callback=_on_resp, auto_ack=True, consumer_tag=corr_id)

    deadline = time.time() + timeout
    while result is None and time.time() < deadline:
        ch.connection.process_data_events(time_limit=0.1)
        time.sleep(0.05)
    try: conn.close()
    except Exception: pass
    if result is None:
        raise TimeoutError("indicator RPC timeout")
    return result

def _fmt_singles(singles) -> str:
    out = []
    for it in (singles or []):
        try:
            if isinstance(it, (list, tuple)) and len(it) >= 2:
                lo, hi = float(it[0]), float(it[1])
            elif isinstance(it, dict):
                lo = float(it.get("low", it.get("lo")))
                hi = float(it.get("high", it.get("hi")))
            else:
                lo = hi = float(it)
            out.append(f"{lo:.2f}-{hi:.2f}")
        except Exception:
            continue
    return ", ".join(out[:6]) if out else "(none)"
# 简洁打印浮点数组前 N 个（用于 HVN / LVN / nodes）
def _fmt_head(arr, n=6, prec=2) -> str:
    if not arr:
        return "(none)"
    out = []
    try:
        for x in arr[: max(1, int(n))]:
            try:
                out.append(f"{float(x):.{int(prec)}f}")
            except Exception:
                out.append(str(x))
    except Exception:
        return "(bad)"
    return ", ".join(out)
def peek_features_snapshot(queue="q.features.snapshot", host="127.0.0.1", vhost="/", n=10, wait=10.0, **kwargs):
    """
    兼容两种用法：
      1) 作为 RabbitMQ peeker 使用（queue/host/vhost/n/wait）。
      2) 直接传入一条快照 dict（peek_features_snapshot(snap=...) 或把 dict 作为第1参）。
         可通过 raw=True 打印原始 JSON（默认由 env FEATURE_PEEK_RAW 控制，1=开）。
    """
    # --- 兼容：直接传快照 dict 的用法 ---
    if isinstance(queue, dict) or "snap" in kwargs:
        import json as _json, os as _os
        snap = queue if isinstance(queue, dict) else kwargs.get("snap") or {}
        # 取 instId 的多重兜底
        inst = (
            snap.get("instId") or snap.get("inst_id") or snap.get("inst") or
            snap.get("symbol") or snap.get("coin") or snap.get("base") or
            snap.get("uly") or "?"
        )
        vol  = ((snap.get("vol") or {}).get("atr_pct"))
        # —— ① 已筛过的“节点”视图（消费方真正使用的少量节点）——
        nodes = (snap.get("profile_nodes") or {})
        up_hvn   = nodes.get("up_hvn")   or []
        down_lvn = nodes.get("down_lvn") or []
        hvn_node_prices = [x[0] for x in up_hvn[:3]   if isinstance(x, (list, tuple)) and x]
        lvn_node_prices = [x[0] for x in down_lvn[:3] if isinstance(x, (list, tuple)) and x]

        # —— ② 完整体积档位视图（生产方日志里的 HVN/LVN 全量）——
        multi   = (snap.get("profile_multi") or {})
        # 你如果还有其他窗口（比如 "24h"、"7d"），这里可以按需更换 key
        vol72   = (multi.get("72h") or {}).get("volume") or {}
        hvn_all = vol72.get("hvn") or []
        lvn_all = vol72.get("lvn") or []
        hvn_all_prices = [(x[0] if isinstance(x, (list, tuple)) else x) for x in hvn_all[:5]]
        lvn_all_prices = [(x[0] if isinstance(x, (list, tuple)) else x) for x in lvn_all[:5]]
        vpin_obj = (snap.get("vpin") or {})
        vpin = vpin_obj.get("ewma", vpin_obj.get("ma", 0.0))
        # logging.info(
        #     "[peek][inline] inst=%s atr=%.3f%% "
        #     "hvn_nodes=%d %s lvn_nodes=%d %s "
        #     "hvn_all=%d %s lvn_all=%d %s vpin=%.3f",
        #     inst,
        #     float(vol or 0.0),
        #     int(len(up_hvn)),   hvn_node_prices,
        #     int(len(down_lvn)), lvn_node_prices,
        #     int(len(hvn_all)),  hvn_all_prices,
        #     int(len(lvn_all)),  lvn_all_prices,
        #     float(vpin or 0.0),
        # )
        # 是否把原始 JSON 打到 INFO（默认看环境变量 FEATURE_PEEK_RAW）
        raw_flag = kwargs.get("raw")
        if raw_flag is None:
            raw_flag = _os.getenv("FEATURE_PEEK_RAW", "0") != "0"
        try:
            raw_limit = int(_os.getenv("FEATURE_PEEK_RAW_LIMIT", "4096"))
        except Exception:
            raw_limit = 4096
        raw_txt = _json.dumps(snap, ensure_ascii=False, separators=(",", ":"))
        if raw_flag:
            if raw_limit and len(raw_txt) > raw_limit:
                logging.info("[peek][inline][raw] %s... (+%dB)", raw_txt[:raw_limit], len(raw_txt) - raw_limit)
            else:
                logging.info("[peek][inline][raw] %s", raw_txt)
        else:
            logging.debug("[peek][inline][raw] %s", raw_txt)
        return
    # --- 传统 peeker 路径 ---
    # 用 %s 包装 n，避免外部误传非整型导致格式化报错
    #logging.info("[peek] queue=%s host=%s vhost=%s (n=%s, wait=%.1fs)", queue, host, vhost, str(n), wait)

# ===== 工具函数：步进取整 & 合法化合约张数 =====
def _floor_to_step(value: float, step: float) -> float:
    """向下按步进取整（兼容浮点/字符串输入）"""
    v = float(value)
    s = float(step)
    if s <= 0:
        return v
    return math.floor(v / s) * s

def _normalize_contracts(contracts_raw: float, lot_sz: float, min_sz: float) -> float:
    """把原始合约张数按步进向下取整，并校验最小张数"""
    sz = _floor_to_step(contracts_raw, lot_sz)
    if sz < float(min_sz):
        return 0.0
    return sz