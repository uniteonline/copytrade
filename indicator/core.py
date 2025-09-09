# core.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import math
import time
from dataclasses import dataclass, field
from typing import Dict, List, Tuple, Optional

import numpy as np

try:
    from numba import njit
    NUMBA_OK = True
except Exception:  # numba 可选
    NUMBA_OK = False

# ========= 数值内核（numba 优先，回退 numpy） =========

def _np_ofi_events(bid_px: np.ndarray, ask_px: np.ndarray,
                   bid_sz: np.ndarray, ask_sz: np.ndarray) -> np.ndarray:
    """
    OFI 事件值 e_n（按 BBO 变化）
    e_n = 1{B_n >= B_{n-1}}*qB_n - 1{B_n <= B_{n-1}}*qB_{n-1}
          - 1{A_n <= A_{n-1}}*qA_n + 1{A_n >= A_{n-1}}*qA_{n-1}
    参考 Cont-Kukanov-Stoikov、Markwick 解释。
    """
    n = bid_px.shape[0]
    e = np.zeros(n, dtype=np.float64)
    if n <= 1:
        return e
    b_ge = (bid_px[1:] >= bid_px[:-1]).astype(np.float64)
    b_le = (bid_px[1:] <= bid_px[:-1]).astype(np.float64)
    a_le = (ask_px[1:] <= ask_px[:-1]).astype(np.float64)
    a_ge = (ask_px[1:] >= ask_px[:-1]).astype(np.float64)

    e[1:] = b_ge * bid_sz[1:] - b_le * bid_sz[:-1] - a_le * ask_sz[1:] + a_ge * ask_sz[:-1]
    return e

def _np_microprice(bid_px: float, ask_px: float, bid_sz: float, ask_sz: float) -> float:
    """
    Microprice / Weighted Mid（BBO）：
      I = Qb / (Qb + Qa)
      Micro = I * Ask + (1 - I) * Bid  （= VAMP_bbo，与 hftbacktest 教程一致）
    当一侧为 0 时退化为 mid。
    """
    den = bid_sz + ask_sz
    mid = 0.5 * (bid_px + ask_px)
    if den <= 0:
        return mid
    I = bid_sz / den
    return I * ask_px + (1.0 - I) * bid_px

if NUMBA_OK:
    @njit(cache=True, fastmath=True)
    def _nb_ofi_events(bid_px, ask_px, bid_sz, ask_sz):
        n = bid_px.shape[0]
        e = np.zeros(n, np.float64)
        if n <= 1:
            return e
        for i in range(1, n):
            e[i] = ( (1.0 if bid_px[i] >= bid_px[i-1] else 0.0) * bid_sz[i]
                   - (1.0 if bid_px[i] <= bid_px[i-1] else 0.0) * bid_sz[i-1]
                   - (1.0 if ask_px[i] <= ask_px[i-1] else 0.0) * ask_sz[i]
                   + (1.0 if ask_px[i] >= ask_px[i-1] else 0.0) * ask_sz[i-1] )
        return e

    @njit(cache=True, fastmath=True)
    def _nb_microprice(bid_px, ask_px, bid_sz, ask_sz):
        den = bid_sz + ask_sz
        mid = 0.5 * (bid_px + ask_px)
        if den <= 0.0:
            return mid
        I = bid_sz / den
        return I * ask_px + (1.0 - I) * bid_px

# 选择内核
_ofi_events = _nb_ofi_events if NUMBA_OK else _np_ofi_events
_microprice = _nb_microprice if NUMBA_OK else _np_microprice

# ========= 流式缓存 =========

@dataclass
class L1Ring:
    """存储最近一段时间的 BBO 序列用于窗口聚合"""
    cap: int
    ts: np.ndarray = field(init=False)
    bid_px: np.ndarray = field(init=False)
    ask_px: np.ndarray = field(init=False)
    bid_sz: np.ndarray = field(init=False)
    ask_sz: np.ndarray = field(init=False)
    size: int = 0
    head: int = 0  # 下一写入位置

    def __post_init__(self):
        self.ts = np.zeros(self.cap, dtype=np.int64)
        self.bid_px = np.zeros(self.cap, dtype=np.float64)
        self.ask_px = np.zeros(self.cap, dtype=np.float64)
        self.bid_sz = np.zeros(self.cap, dtype=np.float64)
        self.ask_sz = np.zeros(self.cap, dtype=np.float64)

    def push(self, ts_ms: int, bp: float, ap: float, bs: float, asz: float):
        i = self.head
        self.ts[i] = ts_ms
        self.bid_px[i] = bp
        self.ask_px[i] = ap
        self.bid_sz[i] = bs
        self.ask_sz[i] = asz
        self.head = (i + 1) % self.cap
        self.size = min(self.size + 1, self.cap)

    def slice_recent_ms(self, now_ms: int, window_ms: int) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
        """返回最近 window_ms 内（含边界）的视图（按时间升序）"""
        if self.size == 0:
            return (np.empty(0),)*5
        # 收集有效索引
        idxs = []
        start_ts = now_ms - int(window_ms)
        # 遍历环形缓冲区
        for k in range(self.size):
            pos = (self.head - 1 - k) % self.cap
            if self.ts[pos] >= start_ts:
                idxs.append(pos)
            else:
                break
        if not idxs:
            return (np.empty(0),)*5
        idxs = np.array(list(reversed(idxs)), dtype=np.int64)
        return (self.ts[idxs], self.bid_px[idxs], self.ask_px[idxs], self.bid_sz[idxs], self.ask_sz[idxs])

@dataclass
class DepthSnapshot:
    ts: int
    # 期望为按价格优先排序的列表/数组: [(price, size), ...]
    top_bids: List[Tuple[float, float]]
    top_asks: List[Tuple[float, float]]

# ========= 特征引擎 =========

class FeatureEngine:
    """
    订单流特征引擎：
      • update_from_orderbook(inst_id, book)   — 每次 L2 快照/增量调用
      • build_snapshot(inst_id, now_ms)        — 汇总输出本次要下发的特征
    """
    def __init__(
        self,
        depth_levels: List[int] = (5, 10),
        ofi_windows_ms: List[int] = (500, 1000, 2000, 3000),
        ring_seconds: float = 6.0,   # 缓冲 2×最大窗口，默认 6s
        expected_bps: int = 1500     # 预估每秒 BBO 更新数（TBT: 100~1000+）
    ):
        self.depth_levels = sorted({int(x) for x in depth_levels if int(x) > 0})
        self.ofi_windows_ms = sorted({int(x) for x in ofi_windows_ms if int(x) > 0})
        self._ring_cap = max(4096, int(ring_seconds * max(200, expected_bps)))
        self._l1: Dict[str, L1Ring] = {}
        self._depth: Dict[str, DepthSnapshot] = {}

    # ---- 输入：来自 orderbook.py 的标准化对象 ----
    def update_from_orderbook(self, inst_id: str, book: dict):
        """
        book 需要包含：
          - 'ts'（毫秒）
          - 'best_bid', 'best_ask' ：float 价格
          - 'best_bid_size', 'best_ask_size' ：float 数量
          - 可选 'bids', 'asks' ：Top-N [(px, sz)...]
        """
        ts = int(book.get("ts") or 0)
        bb = float(book.get("best_bid") or 0)
        ba = float(book.get("best_ask") or 0)
        bbs = float(book.get("best_bid_size") or 0)
        bas = float(book.get("best_ask_size") or 0)
        if ts <= 0 or bb <= 0 or ba <= 0:
            return

        ring = self._l1.get(inst_id)
        if ring is None:
            # 以 __init__ 的容量参数创建
            ring = self._l1.setdefault(inst_id, L1Ring(cap=self._ring_cap))

        ring.push(ts, bb, ba, bbs, bas)

        # 记录 Top-N 深度（可用于 TopN Depth 与 Queue Imbalance_N）
        bids = book.get("bids") or []
        asks = book.get("asks") or []
        # 仅保存我们需要的 N=max(depth_levels) 档
        n_keep = max(self.depth_levels) if self.depth_levels else 0
        if n_keep > 0 and (bids or asks):
            self._depth[inst_id] = DepthSnapshot(
                ts=ts,
                top_bids=list(bids[:n_keep]),
                top_asks=list(asks[:n_keep])
            )

    # ---- 计算：单次快照 ----
    def _calc_bbo_features(self, ring: L1Ring) -> dict:
        i = (ring.head - 1) % ring.cap
        bb, ba = ring.bid_px[i], ring.ask_px[i]
        bbs, bas = ring.bid_sz[i], ring.ask_sz[i]
        mid = 0.5 * (bb + ba)
        spread = max(0.0, ba - bb)
        micro = _microprice(bb, ba, bbs, bas)
        den = bbs + bas
        qi_top1 = 0.0 if den <= 0 else (bbs - bas) / den

        return {
            "mid": mid,
            "best_bid": bb, "best_ask": ba,
            "best_bid_size": bbs, "best_ask_size": bas,
            "spread": spread,
            "spread_bps": (spread / mid * 1e4) if mid > 0 else 0.0,
            "microprice": micro,
            "microprice_delta": micro - mid,
            "microprice_delta_bps": ((micro - mid) / mid * 1e4) if mid > 0 else 0.0,
            "qi_top1": qi_top1
        }

    def _calc_ofi_windows(self, ring: L1Ring, now_ms: int) -> dict:
        out = {}
        for w in self.ofi_windows_ms:
            ts, bb, ba, bbs, bas = ring.slice_recent_ms(now_ms, w)
            if ts.size == 0 or ts.size == 1:
                out[str(w)] = 0.0
                continue
            e = _ofi_events(bb, ba, bbs, bas)
            out[str(w)] = float(np.nansum(e))
        return out

    def _calc_topn_depth(self, inst_id: str) -> dict:
        ds = self._depth.get(inst_id)
        res = {"bid": {}, "ask": {}, "qi": {}}
        if not ds:
            return res
        # 预先构造累计
        bid_arr = np.array(ds.top_bids[:], dtype=np.float64)
        ask_arr = np.array(ds.top_asks[:], dtype=np.float64)
        # 形如 [[px, sz], ...]
        bsz = bid_arr[:, 1] if bid_arr.size else np.zeros(0)
        asz = ask_arr[:, 1] if ask_arr.size else np.zeros(0)

        for n in self.depth_levels:
            sb = float(bsz[:min(n, bsz.size)].sum()) if bsz.size else 0.0
            sa = float(asz[:min(n, asz.size)].sum()) if asz.size else 0.0
            res["bid"][str(n)] = sb
            res["ask"][str(n)] = sa
            den = sb + sa
            res["qi"][str(n)] = 0.0 if den <= 0 else (sb - sa) / den
        return res

    # ---- 对外：构建可下发的特征快照 ----
    def build_snapshot(self, inst_id: str, now_ms: Optional[int] = None, for_eval: bool = False) -> Optional[dict]:
        ring = self._l1.get(inst_id)
        if ring is None or ring.size == 0:
            return None
        now_ms = int(now_ms or time.time() * 1000)
        bbo = self._calc_bbo_features(ring)
        ofi = self._calc_ofi_windows(ring, now_ms)
        depth = self._calc_topn_depth(inst_id)

        return {
            "instId": inst_id,
            "ts": now_ms,
            "features": {
                "spread": {
                    "abs": bbo["spread"],
                    "bps": bbo["spread_bps"]
                },
                "microprice": {
                    "value": bbo["microprice"],
                    "delta": bbo["microprice_delta"],
                    "delta_bps": bbo["microprice_delta_bps"]
                },
                "queue_imbalance": {
                    "top1": bbo["qi_top1"],
                    "topN": depth.get("qi", {})
                },
                "topN_depth": {
                    "bid": depth.get("bid", {}),
                    "ask": depth.get("ask", {})
                },
                "ofi": {
                    "sum": ofi  # {"500": x, "1000": y, ...} (单位：数量，不做标准化)
                }
            },
            "bbo": {
                "mid": bbo["mid"],
                "best_bid": bbo["best_bid"],
                "best_ask": bbo["best_ask"],
                "best_bid_size": bbo["best_bid_size"],
                "best_ask_size": bbo["best_ask_size"],
            },
            "windows_ms": self.ofi_windows_ms,
        }

    # 兼容：若上游需要逐笔 trade 更新，这里可做 no-op（指标主要来源于 BBO/簿）
    def update_from_trade(self, inst_id: str, trade: dict) -> None:
        return