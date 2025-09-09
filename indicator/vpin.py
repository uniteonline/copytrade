# /root/copyTrade/indicator/vpin.py
# -*- coding: utf-8 -*-
"""
VPIN: Volume-Synchronized Probability of Informed Trading (Flow Toxicity)
=========================================================================
生产级毒性流指标引擎：
- 体积时钟（volume clock）：按固定“体积桶”分割成交流（支持 base/quote 维度）
- 每个桶的不平衡度：imbalance_k = |Buy_k - Sell_k| / V
- VPIN = 最近 N 个桶的不平衡度的均值（也支持 EWMA）
- 适配 OKX trades 流：side ∈ {"buy","sell"}，含 px、sz、ts(ms)

文献/口径：
- 体积时钟 + 体积桶 + VPIN 定义（Easley, López de Prado, O'Hara）:
  https://www.quantresearch.org/VPIN.pdf  （见 P3：volume clock / bucket / BVC）
- 工程实现借鉴 flowrisk 的递归/EWMA 风格：
  https://github.com/hanxixuana/flowrisk

注意：
- 我们拥有逐笔的 taker side → 直接准确划分买/卖量，无需 BVC 近似。
- VPIN 与成交量/波动率相关性的学术争议：Andersen & Bondarenko (2014) 等，
  将其作为“停手/降频”的风险指示更稳妥，而非孤立预测器。
"""

from __future__ import annotations

import math
import threading
from dataclasses import dataclass, field
from typing import Deque, Dict, Optional, Tuple, List
from collections import deque

import numpy as np


Number = float
Ms = int


# ============================= 工具 =============================

def _clip_nonneg(x: float) -> float:
    return x if x > 0.0 else 0.0


def _is_buy(side: str) -> bool:
    if not side:
        return False
    s = side.lower()
    # 兼容大小写/同义字段
    return s in ("buy", "b", "bid", "taker_buy")


# ============================= 数据结构 =============================

@dataclass
class _BucketWin:
    """
    单合约的滚动窗口状态
    """
    bucket_size: float                 # 固定桶体量 V（base 或 quote）
    window_buckets: int                # 计算 VPIN 用的滚动桶数 N
    use_ewma: bool                     # 是否计算/输出 EWMA 版本
    ewma_alpha: float = 0.2            # EWMA 平滑系数（0<alpha<=1）
    threshold: float = 0.75            # “停手阀”阈值（0~1）
    bucket_kind: str = "quote"         # "base" | "quote"
    # 当前桶累计
    cur_buy: float = 0.0
    cur_sell: float = 0.0
    cur_filled: float = 0.0            # 已填体量（与 bucket_size 同维度）
    cur_start_ts: Optional[Ms] = None
    # 历史桶不平衡度队列
    imb_q: Deque[float] = field(default_factory=deque)
    ts_q: Deque[int] = field(default_factory=deque)
    maxlen: int = 256                  # 保存上限，>= window_buckets
    # 统计
    total_buckets: int = 0
    vpin_ma: Optional[float] = None
    vpin_ewma: Optional[float] = None

    def __post_init__(self):
        self.maxlen = max(int(self.maxlen), int(self.window_buckets) * 4)
        self.imb_q = deque(maxlen=self.maxlen)
        self.ts_q = deque(maxlen=self.maxlen)

    # ----------- 核心：向当前桶填入成交体量，可能跨越多个桶 -----------
    def ingest(self, ts: Ms, buy_vol: float, sell_vol: float) -> List[Tuple[float, Ms]]:
        """
        向“当前桶”灌入体量；当达到或超过桶体量 V 时，结算若干完整桶。
        返回列表：[(imbalance_k, ts_close_k), ...]
        注意：支持单笔成交跨越多个桶（自动切割并分摊到多个桶）
        """
        out: List[Tuple[float, Ms]] = []

        # 初始化当前桶
        if self.cur_start_ts is None:
            self.cur_start_ts = ts

        # 剩余可填
        remaining = self.bucket_size - self.cur_filled
        # 待分摊（可能很大）
        b_left = _clip_nonneg(buy_vol)
        s_left = _clip_nonneg(sell_vol)

        while True:
            # 当前循环本桶还能填多少
            remaining = self.bucket_size - self.cur_filled
            if remaining <= 1e-12:
                # 理论上不应该出现，安全保护
                remaining = 0.0

            # 本轮放入量 = 在“买/卖残余体量”里截断 remaining
            # 先合计本轮将填入的总量 fill_used，不区分买/卖
            fill_used = min(remaining, b_left + s_left)
            if fill_used <= 0.0:
                break

            # 以“买/卖残余体量的相对占比”切分本轮注入
            if b_left + s_left > 0:
                b_take = fill_used * (b_left / (b_left + s_left))
                s_take = fill_used - b_take
            else:
                b_take = 0.0
                s_take = 0.0

            # 注入当前桶
            self.cur_buy += b_take
            self.cur_sell += s_take
            self.cur_filled += fill_used

            # 消耗对应残余
            b_left = _clip_nonneg(b_left - b_take)
            s_left = _clip_nonneg(s_left - s_take)

            # 桶满 → 结算
            if self.cur_filled + 1e-12 >= self.bucket_size:
                imb = abs(self.cur_buy - self.cur_sell) / max(1e-12, self.bucket_size)
                out.append((imb, ts))
                self._commit_bucket(imb, ts)
                # 进入下一个桶
                self._reset_bucket(ts)
                # 若还有残余体量，继续 while 循环填下一个桶
            else:
                # 未满 → 退出
                break

        return out

    def _commit_bucket(self, imb: float, ts: Ms) -> None:
        self.total_buckets += 1
        self.imb_q.append(float(imb))
        self.ts_q.append(int(ts))
        # 更新 MA
        if len(self.imb_q) >= 1:
            # 用 window_buckets 窗口做 MA
            q = np.asarray(self.imb_q, dtype=np.float64)
            if q.size >= self.window_buckets:
                self.vpin_ma = float(q[-self.window_buckets:].mean())
            else:
                self.vpin_ma = float(q.mean())
        # 更新 EWMA（可选）
        if self.use_ewma:
            if self.vpin_ewma is None:
                self.vpin_ewma = float(imb)
            else:
                a = float(self.ewma_alpha)
                self.vpin_ewma = a * float(imb) + (1.0 - a) * float(self.vpin_ewma)

    def _reset_bucket(self, next_ts: Ms) -> None:
        self.cur_buy = 0.0
        self.cur_sell = 0.0
        self.cur_filled = 0.0
        self.cur_start_ts = next_ts

    def partial_imbalance(self) -> Optional[float]:
        """
        当前未满桶的“即时不平衡度（按已填部分）”，用于诊断。
        """
        if self.cur_filled <= 0.0:
            return None
        return abs(self.cur_buy - self.cur_sell) / max(1e-12, self.bucket_size)

    def fill_ratio(self) -> float:
        """
        当前桶填充比例（0~1）
        """
        return min(1.0, self.cur_filled / max(1e-12, self.bucket_size))

    def toxic(self) -> Tuple[bool, str]:
        """
        是否触发“停手阀”
        规则：vpin_ma 或 vpin_ewma 有任一 >= threshold
        """
        if self.vpin_ma is not None and self.vpin_ma >= self.threshold:
            return True, f"VPIN_MA({self.vpin_ma:.3f}) >= {self.threshold:.3f}"
        if self.vpin_ewma is not None and self.vpin_ewma >= self.threshold:
            return True, f"VPIN_EWMA({self.vpin_ewma:.3f}) >= {self.threshold:.3f}"
        return False, ""


# ============================= 主引擎 =============================

class VPINEngine:
    """
    VPIN 毒性流引擎（多合约）
    ----------------------------------------------------
    • update_trade(instId, trade): 注入一笔成交（ts, px, sz, side）
    • snapshot(instId): 导出该合约的实时 VPIN 诊断
    • snapshot_all(): 导出全部已见合约
    配置：
      - bucket_kind: "base"|"quote"  体量桶单位（币数量/名义额）
      - bucket_size: float           每桶体量 V
      - window_buckets: int          滚动窗口桶数 N
      - use_ewma / ewma_alpha        是否同时输出 EWMA
      - threshold                    停手阀阈值（0~1）
    """

    def __init__(
        self,
        bucket_kind: str = "quote",
        bucket_size: float = 2_000_000.0,  # 例：每桶 200 万 USDT 名义额
        window_buckets: int = 50,
        use_ewma: bool = True,
        ewma_alpha: float = 0.2,
        threshold: float = 0.75,
    ):
        kind = str(bucket_kind or "quote").lower()
        if kind not in ("base", "quote"):
            raise ValueError("bucket_kind 必须为 'base' 或 'quote'")
        if bucket_size <= 0:
            raise ValueError("bucket_size 必须 > 0")
        self.bucket_kind = kind
        self.bucket_size = float(bucket_size)
        self.window_buckets = int(max(2, window_buckets))
        self.use_ewma = bool(use_ewma)
        self.ewma_alpha = float(max(1e-6, min(1.0, ewma_alpha)))
        self.threshold = float(max(0.0, min(1.0, threshold)))

        self._states: Dict[str, _BucketWin] = {}
        self._lock = threading.RLock()

    def _ensure(self, inst_id: str) -> _BucketWin:
        st = self._states.get(inst_id)
        if st:
            return st
        st = _BucketWin(
            bucket_size=self.bucket_size,
            window_buckets=self.window_buckets,
            use_ewma=self.use_ewma,
            ewma_alpha=self.ewma_alpha,
            threshold=self.threshold,
            bucket_kind=self.bucket_kind,
        )
        self._states[inst_id] = st
        return st

    # --------------- 输入：逐笔成交（OKX trades） ---------------

    def update_trade(self, inst_id: str, trade: dict) -> List[Tuple[float, Ms]]:
        """
        trade: { "ts": int(ms), "px": str|float, "sz": str|float, "side": "buy"|"sell" }
        返回：完成的桶 [(imbalance, ts_close), ...]（便于上层做事件化处理）
        """
        try:
            ts = int(trade.get("ts") or trade.get("timestamp") or 0)
            px = float(trade.get("px") or trade.get("price") or 0.0)
            sz = float(trade.get("sz") or trade.get("size") or 0.0)
            side = (trade.get("side") or trade.get("takerSide") or "").lower()
        except Exception:
            return []

        if ts <= 0 or px <= 0.0 or sz <= 0.0 or (side not in ("buy", "sell")):
            return []

        with self._lock:
            st = self._ensure(inst_id)

            # 将“体量”映射到桶维度：base=币数量，quote=名义额=px*sz
            if st.bucket_kind == "base":
                vol = sz
            else:
                vol = px * sz

            if _is_buy(side):
                buy_vol, sell_vol = vol, 0.0
            else:
                buy_vol, sell_vol = 0.0, vol

            return st.ingest(ts=ts, buy_vol=buy_vol, sell_vol=sell_vol)

    # --------------- 输出：快照 ---------------

    def snapshot(self, inst_id: str) -> Optional[dict]:
        with self._lock:
            st = self._states.get(inst_id)
            if not st:
                return None
            toxic, reason = st.toxic()
            return {
                "instId": inst_id,
                "bucket": {
                    "kind": st.bucket_kind,
                    "size": st.bucket_size,
                    "fill_ratio": st.fill_ratio(),             # 当前桶填充比例（0~1）
                    "partial_imbalance": st.partial_imbalance()
                },
                "window": {
                    "buckets": st.window_buckets,
                    "total_buckets": st.total_buckets,
                    "last_close_ts": (int(st.ts_q[-1]) if st.ts_q else None),
                },
                "vpin": {
                    "ma": st.vpin_ma,
                    "ewma": st.vpin_ewma if st.use_ewma else None,
                    "threshold": st.threshold
                },
                "risk": {
                    "toxic": toxic,
                    "reason": reason if toxic else "",
                }
            }

    def snapshot_all(self) -> List[dict]:
        with self._lock:
            return [self.snapshot(k) for k in list(self._states.keys()) if self.snapshot(k) is not None]

    # --------------- 动态调参（运行期） ---------------

    def reconfigure(
        self,
        *,
        bucket_kind: Optional[str] = None,
        bucket_size: Optional[float] = None,
        window_buckets: Optional[int] = None,
        threshold: Optional[float] = None,
        use_ewma: Optional[bool] = None,
        ewma_alpha: Optional[float] = None,
    ) -> None:
        with self._lock:
            if bucket_kind:
                kind = bucket_kind.lower()
                if kind not in ("base", "quote"):
                    raise ValueError("bucket_kind 必须为 'base' 或 'quote'")
                self.bucket_kind = kind
            if bucket_size is not None:
                if bucket_size <= 0:
                    raise ValueError("bucket_size 必须 > 0")
                self.bucket_size = float(bucket_size)
            if window_buckets is not None:
                self.window_buckets = int(max(2, window_buckets))
            if threshold is not None:
                self.threshold = float(max(0.0, min(1.0, threshold)))
            if use_ewma is not None:
                self.use_ewma = bool(use_ewma)
            if ewma_alpha is not None:
                self.ewma_alpha = float(max(1e-6, min(1.0, ewma_alpha)))

            # 更新各合约实例的窗口参数
            for st in self._states.values():
                st.bucket_kind = self.bucket_kind
                st.bucket_size = self.bucket_size
                st.threshold = self.threshold
                st.use_ewma = self.use_ewma
                st.ewma_alpha = self.ewma_alpha
                st.window_buckets = self.window_buckets
                st.maxlen = max(st.maxlen, st.window_buckets * 4)
                st.imb_q = deque(st.imb_q, maxlen=st.maxlen)
                st.ts_q = deque(st.ts_q, maxlen=st.maxlen)


# ============================= 便捷工厂 =============================

def build_from_config(cfg: dict) -> VPINEngine:
    """
    从 config.yaml 读取 features.vpin 段：
    features:
      vpin:
        enabled: true
        bucket_kind: "quote"            # "base"|"quote"
        bucket_size: 2000000            # 每桶体量（USDT 或 币数量）
        window_buckets: 50
        use_ewma: true
        ewma_alpha: 0.2
        threshold: 0.75
    """
    feat = (cfg or {}).get("features", {}) or {}
    vpc = (feat.get("vpin") or {}) if isinstance(feat.get("vpin"), dict) else {}

    kind = str(vpc.get("bucket_kind", "quote")).lower()
    size = float(vpc.get("bucket_size", 2_000_000.0))
    win = int(vpc.get("window_buckets", 50))
    use_ewma = bool(vpc.get("use_ewma", True))
    alpha = float(vpc.get("ewma_alpha", 0.2))
    thr = float(vpc.get("threshold", 0.75))

    return VPINEngine(
        bucket_kind=kind,
        bucket_size=size,
        window_buckets=win,
        use_ewma=use_ewma,
        ewma_alpha=alpha,
        threshold=thr,
    )
