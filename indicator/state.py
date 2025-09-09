# /root/copyTrade/indicator/state.py
# -*- coding: utf-8 -*-
"""
Rolling State — Per-Inst Sliding Quantiles & ATR% Normalization
===============================================================
功能：
- 为每个 instId / 指标维护“时间窗口”内的样本，提供 P20/P50/P80/P90 等分位数与基础统计
- 维护并缓存 ATR%（外部喂入，通常来自 Mark-Price K 线的 Wilder ATR%）
- 提供按 ATR 的归一化：把 bps/pct 类指标与 ATR% 对齐（如 spread_bps / atr_pct）

实现细节：
- 精确滑窗分位数：双端队列保存 (ts, value)，配合有序列表（bisect.insort / bisect_left）支持 O(log n) 插入与过期删除
- 时间窗口：毫秒粒度（默认来自 config.features.windows.quantile_lookback_min）
- 线程安全：RLock；高频下仍建议单线程消费或在上游做粘包
- 可拔插近似算法位：algo='exact' | 'p2' | 'tdigest'（默认 exact；p2/tdigest 需要额外实现/依赖）
参考：
- P² 在线分位数算法（Jain & Chlamtac, 1985）:contentReference[oaicite:1]{index=1}
- t-digest 在线分位数草图（Ted Dunning）:contentReference[oaicite:2]{index=2}
- Wilder ATR 定义（真波幅的移动平均）:contentReference[oaicite:3]{index=3}
"""

from __future__ import annotations

import time
import math
import threading
import logging
from dataclasses import dataclass, field
from typing import Deque, Dict, List, Optional, Tuple
from collections import deque
import bisect


Number = float
Ms = int

log = logging.getLogger(__name__)


# ============================== 工具 ==============================

def _now_ms() -> Ms:
    return int(time.time() * 1000)


def _is_finite(x: float) -> bool:
    return x == x and abs(x) != float("inf")


def _quantile_from_sorted(sorted_vals: List[float], q: float) -> Optional[float]:
    """线性插值分位数（与 numpy.quantile(method='linear') 一致）"""
    n = len(sorted_vals)
    if n == 0:
        return None
    if q <= 0:
        return float(sorted_vals[0])
    if q >= 1:
        return float(sorted_vals[-1])
    idx = q * (n - 1)
    lo = int(math.floor(idx))
    hi = int(math.ceil(idx))
    if lo == hi:
        return float(sorted_vals[lo])
    w = idx - lo
    return float(sorted_vals[lo] * (1.0 - w) + sorted_vals[hi] * w)


# ============================== 数据结构 ==============================

@dataclass
class _WinSeries:
    """
    单一指标的滑窗序列：
    - q: 时间顺序队列 [(ts, val), ...]
    - s: 有序值列表（支持 O(log n) 插入/删除，重复值安全）
    """
    window_ms: Ms
    q: Deque[Tuple[Ms, Number]] = field(default_factory=deque)
    s: List[Number] = field(default_factory=list)
    count: int = 0
    sum: float = 0.0
    sum_sq: float = 0.0
    last_ts: Optional[Ms] = None

    def push(self, ts: Ms, val: Number) -> None:
        if not _is_finite(float(val)):
            return
        self.q.append((ts, float(val)))
        bisect.insort(self.s, float(val))
        self.count += 1
        self.sum += float(val)
        self.sum_sq += float(val) * float(val)
        self.last_ts = ts
        self._expire(ts)

    def _expire(self, now_ms: Ms) -> None:
        cutoff = now_ms - self.window_ms
        q = self.q
        s = self.s
        changed = False
        while q and q[0][0] < cutoff:
            _, v = q.popleft()
            # 从有序表删除第一个等于 v 的位置
            i = bisect.bisect_left(s, v)
            if 0 <= i < len(s) and s[i] == v:
                s.pop(i)
                self.count -= 1
                self.sum -= v
                self.sum_sq -= v * v
                changed = True
            # 极少数情况下数值因浮点误差不精确匹配，退化扫描（近邻容差）
            elif s:
                # 寻找最接近 v 的位置（1~2 步退避）
                j = max(0, min(len(s) - 1, i))
                if abs(s[j] - v) <= 1e-12:
                    s.pop(j)
                    self.count -= 1
                    self.sum -= v
                    self.sum_sq -= v * v
                    changed = True
                elif j + 1 < len(s) and abs(s[j + 1] - v) <= 1e-12:
                    s.pop(j + 1)
                    self.count -= 1
                    self.sum -= v
                    self.sum_sq -= v * v
                    changed = True
                else:
                    # 避免死循环：跳过这条（日志提示）
                    log.debug("[state] expire miss-match val=%r sorted_head/tail=%r/%r", v, s[:2], s[-2:])
        if changed and self.count < 0:
            # 容错：不允许出现负数
            self.count = max(0, self.count)

    # ---- 统计/分位 ----
    def quantile(self, q: float, now_ms: Optional[Ms] = None) -> Optional[float]:
        if now_ms is not None:
            self._expire(now_ms)
        return _quantile_from_sorted(self.s, q)

    def p20(self) -> Optional[float]: return self.quantile(0.20)
    def p50(self) -> Optional[float]: return self.quantile(0.50)
    def p80(self) -> Optional[float]: return self.quantile(0.80)
    def p90(self) -> Optional[float]: return self.quantile(0.90)

    def stats(self, now_ms: Optional[Ms] = None) -> Dict[str, Optional[float]]:
        if now_ms is not None:
            self._expire(now_ms)
        n = self.count
        if n <= 0:
            return {"n": 0, "min": None, "max": None, "mean": None, "std": None}
        mean = self.sum / n
        var = max(0.0, self.sum_sq / n - mean * mean)
        std = math.sqrt(var)
        return {
            "n": n,
            "min": float(self.s[0]) if self.s else None,
            "max": float(self.s[-1]) if self.s else None,
            "mean": float(mean),
            "std": float(std),
        }


@dataclass
class _ATRState:
    atr_pct: Optional[float] = None     # 以百分数表示：如 0.85 表示 0.85%
    ts: Optional[Ms] = None
    ttl_ms: int = 120_000               # 2 分钟超时（可调）

    def valid(self, now_ms: Ms) -> bool:
        if self.atr_pct is None or self.ts is None:
            return False
        return (now_ms - int(self.ts)) <= int(self.ttl_ms)


@dataclass
class _KeyState:
    series: _WinSeries


# ============================== 主引擎 ==============================

class StateEngine:
    """
    滑窗分位数与 ATR% 归一化引擎（多 instId × 多指标）
    ----------------------------------------------------------------
    • update_metric(inst, key, ts, value): 推入一个指标值
    • update_atr(inst, atr_pct, ts): 更新该标的 ATR%（百分数，如 0.85 表示 0.85%）
    • snapshot(inst): 导出该标的所有已知指标的 P20/P50/P80/P90 + 基础统计
    • normalize(inst, key, value): 将 bps/% 类指标按 ATR% 做归一化（返回 "×ATR" 尺度）
    """

    def __init__(
        self,
        window_ms: int,
        algo: str = "exact",                      # 'exact' | 'p2' | 'tdigest'（预留）
        atr_ttl_ms: int = 120_000,
        # 归一化口径：key -> {'type': 'bps'|'pct'|'none'}
        norm_rules: Optional[Dict[str, Dict[str, str]]] = None,
    ):
        if window_ms <= 0:
            raise ValueError("window_ms must be > 0")
        self.window_ms = int(window_ms)
        self.algo = str(algo).lower()
        if self.algo not in ("exact", "p2", "tdigest"):
            raise ValueError("algo must be one of: exact | p2 | tdigest")

        self._data: Dict[Tuple[str, str], _KeyState] = {}
        self._atr: Dict[str, _ATRState] = {}
        self._atr_ttl = int(atr_ttl_ms)
        self._lock = threading.RLock()

        # 归一化规则：默认对常见 key 套路
        self._norm: Dict[str, Dict[str, str]] = norm_rules.copy() if norm_rules else {
            # spread / 微价格偏移是“bps”类：先转 % 再 / ATR%
            "spread_bps": {"type": "bps"},
            "microprice_delta_bps": {"type": "bps"},
            "ret_pct": {"type": "pct"},
            "mid_move_pct": {"type": "pct"},
            # 若某指标无需归一化：{"type": "none"}
        }

    # ---- 管理/内部 ----
    def _get_series(self, inst: str, key: str) -> _WinSeries:
        ks = (inst, key)
        st = self._data.get(ks)
        if st is not None:
            return st.series
        # algo 预留：当前仅 exact 模式
        series = _WinSeries(window_ms=self.window_ms)
        self._data[ks] = _KeyState(series=series)
        return series

    # ---- 输入 ----
    def update_metric(self, inst: str, key: str, value: Number, ts_ms: Optional[Ms] = None) -> None:
        """
        推入一个值。ts_ms 为 None 时使用当前时间。
        """
        ts = int(ts_ms or _now_ms())
        with self._lock:
            self._get_series(inst, key).push(ts, float(value))

    def update_atr(self, inst: str, atr_pct: Number, ts_ms: Optional[Ms] = None) -> None:
        """
        更新 ATR%（百分数；例如 0.85 表示 0.85%）
        """
        ts = int(ts_ms or _now_ms())
        with self._lock:
            self._atr[inst] = _ATRState(atr_pct=float(atr_pct), ts=ts, ttl_ms=self._atr_ttl)

    # ---- 查询/导出 ----
    def quantiles(self, inst: str, key: str, now_ms: Optional[Ms] = None) -> Dict[str, Optional[float]]:
        now = int(now_ms or _now_ms())
        with self._lock:
            ser = self._get_series(inst, key)
            return {
                "p20": ser.quantile(0.20, now),
                "p50": ser.quantile(0.50, now),
                "p80": ser.quantile(0.80, now),
                "p90": ser.quantile(0.90, now),
            }

    def stats(self, inst: str, key: str, now_ms: Optional[Ms] = None) -> Dict[str, Optional[float]]:
        now = int(now_ms or _now_ms())
        with self._lock:
            ser = self._get_series(inst, key)
            base = ser.stats(now)
            base.update(self.quantiles(inst, key, now))
            base["last_ts"] = ser.last_ts
            return base

    def snapshot(self, inst: str, keys: Optional[List[str]] = None, now_ms: Optional[Ms] = None) -> Dict[str, dict]:
        """
        导出某标的全部/指定 key 的统计快照（含分位数与基础统计）
        """
        now = int(now_ms or _now_ms())
        with self._lock:
            # 收集该 inst 相关的 keys
            all_keys = [k for (i, k) in self._data.keys() if i == inst]
        if keys is None:
            keys = sorted(set(all_keys))
        out: Dict[str, dict] = {}
        for k in keys:
            out[k] = self.stats(inst, k, now)
        # 附带 ATR 信息
        atr = self._atr.get(inst)
        out["_atr"] = {
            "atr_pct": (atr.atr_pct if (atr and atr.valid(now)) else None),
            "ts": (atr.ts if atr else None),
            "ttl_ms": self._atr_ttl,
        }
        return out

    # ---- 归一化 ----
    def normalize(self, inst: str, key: str, value: Number, *, ts_ms: Optional[Ms] = None) -> Optional[float]:
        """
        将一个值按 ATR% 进行归一化：
          - 若 key 的规则为 'bps'：先 bps→%（/10000），再 / atr_pct
          - 若 key 的规则为 'pct'：直接 / atr_pct
          - 规则缺失或 ATR 失效 → 返回 None
        返回值含义：以“×ATR”为单位的无量纲值（如 0.5 表示 0.5 × ATR）
        """
        now = int(ts_ms or _now_ms())
        rule = self._norm.get(key)
        if not rule:
            return None
        atr = self._atr.get(inst)
        if not atr or not atr.valid(now) or not _is_finite(float(atr.atr_pct or 0)):
            return None
        atr_pct = float(atr.atr_pct)  # 例如 0.85 表示 0.85%
        if atr_pct <= 0:
            return None

        t = rule.get("type", "none")
        v = float(value)
        if t == "bps":
            v_pct = v / 10000.0
            return v_pct / atr_pct
        elif t == "pct":
            return v / atr_pct
        elif t == "none":
            return v
        return None

    # ---- 维护/清理 ----
    def evict_inst(self, inst: str) -> None:
        """删除某标的全部指标与 ATR 状态"""
        with self._lock:
            for k in list(self._data.keys()):
                if k[0] == inst:
                    self._data.pop(k, None)
            self._atr.pop(inst, None)

    def evict_key(self, inst: str, key: str) -> None:
        with self._lock:
            self._data.pop((inst, key), None)

    # ---- 动态配置 ----
    def reconfigure_window(self, window_ms: int) -> None:
        if window_ms <= 0:
            return
        with self._lock:
            self.window_ms = int(window_ms)
            # 现有序列的窗口大小同步更新
            for st in self._data.values():
                st.series.window_ms = self.window_ms

    def set_norm_rule(self, key: str, kind: str) -> None:
        """运行期追加/更改归一化规则"""
        kind = str(kind).lower()
        if kind not in ("bps", "pct", "none"):
            raise ValueError("kind must be one of: bps | pct | none")
        self._norm[key] = {"type": kind}


# ============================== 便捷工厂 ==============================

def build_from_config(cfg: dict) -> StateEngine:
    """
    从 config.yaml 读取窗口与默认归一化规则：
    features:
      windows:
        quantile_lookback_min: 60
    """
    feat = (cfg or {}).get("features", {}) or {}
    win = (feat.get("windows") or {}) if isinstance(feat.get("windows"), dict) else {}
    lookback_min = int(win.get("quantile_lookback_min", 60))
    window_ms = max(1, lookback_min) * 60_000

    # 你也可以在配置中加自定义的 normalize 规则，这里默认内置常见键
    engine = StateEngine(window_ms=window_ms, algo="exact", atr_ttl_ms=120_000)
    return engine


# ============================== 使用示例（注释） ==============================
# se = build_from_config(config)
# se.update_atr("BTC-USDT-SWAP", atr_pct=0.85)            # 0.85%
# se.update_metric("BTC-USDT-SWAP", "spread_bps", 6.2)    # 6.2 bps
# se.update_metric("BTC-USDT-SWAP", "spread_bps", 4.0)
# print(se.quantiles("BTC-USDT-SWAP", "spread_bps"))      # {'p20':..., 'p50':..., 'p80':..., 'p90':...}
# print(se.normalize("BTC-USDT-SWAP", "spread_bps", 5.0)) # = (5/10000)/0.0085 ≈ 0.0588 × ATR
