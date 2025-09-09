# /root/copyTrade/indicator/book/orderbook.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Tuple, Optional
import math
import time
import logging


Number = float
Price = float
Size = float
Level = Tuple[Price, Size]


def _to_int(x) -> Optional[int]:
    try:
        return int(x) if x is not None else None
    except Exception:
        return None


def _to_float(x) -> Optional[float]:
    try:
        f = float(x)
        # 过滤 NaN/Inf
        return f if math.isfinite(f) else None
    except Exception:
        return None


@dataclass
class L2OrderBook:
    """
    L2 OrderBook（OKX/Okcoin v5 books / books-l2-tbt 增量）
    - 通过 apply_snapshot() 初始化
    - 通过 apply_update() 应用增量（校验 seqId/prevSeqId）
    - 提供 BBO / Top-N / Spread / Staleness 查询
    """
    inst_id: str
    depth_limit: int = 400                 # 可设为 50/100/200/400
    stale_after_ms: int = 1500             # 超过此毫秒数未更新则认为陈旧

    # 内部状态
    bids: Dict[Price, Size] = field(default_factory=dict)   # 价->量（买，取最高）
    asks: Dict[Price, Size] = field(default_factory=dict)   # 价->量（卖，取最低）
    last_seq: Optional[int] = None
    last_ts: Optional[int] = None

    # 统计/诊断
    updates_applied: int = 0
    updates_skipped: int = 0
    resync_count: int = 0

    # ---------- 基础维护 ----------
    def reset(self) -> None:
        self.bids.clear()
        self.asks.clear()
        self.last_seq = None
        self.last_ts = None
        self.updates_applied = 0
        self.updates_skipped = 0

    # ---------- 快照 ----------
    def apply_snapshot(self, bids: List[List], asks: List[List], seq_id: int, ts_ms: int) -> None:
        """
        使用 REST /books 或 WS snapshot 行初始化本地簿。
        期望元素形如 [[px, sz, ...], ...]，px/sz 可为 str/float。
        """
        self.bids.clear()
        self.asks.clear()

        for row in bids:
            if len(row) < 2:
                continue
            px = _to_float(row[0]); sz = _to_float(row[1])
            if px is None or sz is None:
                continue
            if sz > 0:
                self.bids[px] = sz

        for row in asks:
            if len(row) < 2:
                continue
            px = _to_float(row[0]); sz = _to_float(row[1])
            if px is None or sz is None:
                continue
            if sz > 0:
                self.asks[px] = sz

        self._trim_levels()
        self.last_seq = _to_int(seq_id)
        self.last_ts = _to_int(ts_ms)

    # ---------- 增量 ----------
    def apply_update(
        self,
        bids: List[List],
        asks: List[List],
        seq_id: int,
        prev_seq_id: int,
        ts_ms: int,
    ) -> Dict[str, object]:
        """
        应用一次增量（OKX v5：books / books-l2-tbt / books50-l2-tbt）
        - 校验 prev_seq_id == last_seq（或 prev_seq_id == seq_id 的“无变化”情形）
        - 按价位覆盖/删除：size=0 → 删除该价位
        返回 {applied:bool, need_resync:bool, reason:str}
        """
        status = {"applied": False, "need_resync": False, "reason": ""}

        seq = _to_int(seq_id)
        prev = _to_int(prev_seq_id)
        ts = _to_int(ts_ms)

        if self.last_seq is None:
            status["need_resync"] = True
            status["reason"] = "no-snapshot"
            self.updates_skipped += 1
            return status

        # 序列连续性：正常 prev == last_seq；或特殊情形 prev == seq（无有效变化）
        if not (prev == self.last_seq or prev == seq):
            status["need_resync"] = True
            status["reason"] = f"seq-mismatch have={self.last_seq} prev={prev} seq={seq}"
            self.resync_count += 1
            self.updates_skipped += 1
            return status

        # 按档更新
        for row in bids or []:
            if len(row) < 2:
                continue
            px = _to_float(row[0]); sz = _to_float(row[1])
            if px is None or sz is None:
                continue
            if sz == 0:
                self.bids.pop(px, None)
            else:
                self.bids[px] = sz

        for row in asks or []:
            if len(row) < 2:
                continue
            px = _to_float(row[0]); sz = _to_float(row[1])
            if px is None or sz is None:
                continue
            if sz == 0:
                self.asks.pop(px, None)
            else:
                self.asks[px] = sz

        self._trim_levels()
        self.last_seq = seq
        self.last_ts = ts
        self.updates_applied += 1
        status["applied"] = True
        return status

    # ---------- 查询 ----------
    def get_bbo(self) -> Optional[Tuple[Level, Level]]:
        """
        返回 ((best_bid_px, size), (best_ask_px, size))；若缺任一侧则返回 None。
        """
        if not self.bids or not self.asks:
            return None
        best_bid = max(self.bids.items(), key=lambda kv: kv[0])
        best_ask = min(self.asks.items(), key=lambda kv: kv[0])
        return (best_bid, best_ask)

    def spread(self) -> Optional[float]:
        bbo = self.get_bbo()
        if not bbo:
            return None
        (bp, _), (ap, _) = bbo
        return max(0.0, ap - bp)

    def mid(self) -> Optional[float]:
        bbo = self.get_bbo()
        if not bbo:
            return None
        (bp, _), (ap, _) = bbo
        return (bp + ap) / 2.0

    def top_n(self, n: int) -> Dict[str, List[Level]]:
        """
        返回 Top-N 档（买：价降序；卖：价升序），n 会被 depth_limit 约束。
        """
        n = max(0, min(int(n), self.depth_limit))
        bids = sorted(self.bids.items(), key=lambda kv: kv[0], reverse=True)[:n]
        asks = sorted(self.asks.items(), key=lambda kv: kv[0])[:n]
        return {"bids": bids, "asks": asks}

    def depth_summary(self, n: int) -> Dict[str, float]:
        """
        汇总 Top-N 的累计数量与名义金额（以 mid 近似）
        """
        n = max(0, min(int(n), self.depth_limit))
        b = sorted(self.bids.items(), key=lambda kv: kv[0], reverse=True)[:n]
        a = sorted(self.asks.items(), key=lambda kv: kv[0])[:n]
        m = self.mid()
        def _sum_amount(levels: List[Level]) -> float:
            # 若无 mid，用各层价格乘数量累加
            if m is None:
                return sum(p * s for p, s in levels)
            return sum(m * s for _, s in levels)
        return {
            "bid_qty": sum(s for _, s in b),
            "ask_qty": sum(s for _, s in a),
            "bid_amount": _sum_amount(b),
            "ask_amount": _sum_amount(a),
        }

    def is_stale(self, now_ms: Optional[int] = None) -> bool:
        """
        根据 last_ts 与 stale_after_ms 判断簿是否“陈旧”
        """
        if self.last_ts is None:
            return True
        now = int(time.time() * 1000) if now_ms is None else int(now_ms)
        return (now - int(self.last_ts)) > int(self.stale_after_ms)

    # ---------- 导出轻量快照（供上游发布/特征计算） ----------
    def light_snapshot(self, levels: List[int] = [5, 10]) -> dict:
        """
        返回轻量快照：包含 BBO、Top-5/Top-10（按需配置）、spread 等。
        """
        out = {
            "instId": self.inst_id,
            "ts": self.last_ts,
            "seqId": self.last_seq,
            "stale": self.is_stale(),
            "bbo": None,
            "spread": None,
        }
        bbo = self.get_bbo()
        if bbo:
            (bp, bs), (ap, asz) = bbo
            out["bbo"] = {"bid": [bp, bs], "ask": [ap, asz]}
            out["spread"] = max(0.0, ap - bp)

        for n in levels:
            n = int(n)
            top = self.top_n(n)
            out[f"l{n}"] = {
                "bids": [[p, s] for p, s in top["bids"]],
                "asks": [[p, s] for p, s in top["asks"]],
            }
        return out

    # ---------- 私有：裁剪与容量控制 ----------
    def _trim_levels(self) -> None:
        """
        只保留各侧前 depth_limit 档，其余价位剔除以控制内存与排序成本。
        """
        if len(self.bids) > self.depth_limit:
            # 取前 N 个最高价，其余剔除
            keep = set(p for p, _ in sorted(self.bids.items(), key=lambda kv: kv[0], reverse=True)[: self.depth_limit])
            for p in list(self.bids.keys()):
                if p not in keep:
                    self.bids.pop(p, None)

        if len(self.asks) > self.depth_limit:
            keep = set(p for p, _ in sorted(self.asks.items(), key=lambda kv: kv[0])[: self.depth_limit])
            for p in list(self.asks.keys()):
                if p not in keep:
                    self.asks.pop(p, None)
