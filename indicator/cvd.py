# /root/copyTrade/indicator/features/cvd.py
# -*- coding: utf-8 -*-
"""
CVD — Cumulative Volume Delta (by taker side)
---------------------------------------------
流式累计“买方成交量 - 卖方成交量”，并支持多窗口（秒/分钟）滚动聚合。

输入（trade dict，来自 OKX WS trades）：
    {
        "ts":   1733646800123,          # 毫秒
        "px":   25123.5,                # 成交价
        "sz":   12.0,                    # 成交数量（OKX SWAP: 合约张数）
        "side": "buy" | "sell"          # taker 方向（OKX 定义）
    }

输出（snapshot）示例：
    {
      "instId": "BTC-USDT-SWAP",
      "ts": 1733646801123,
      "cvd": {
        "cum": {                  # 自进程启动以来的累计
          "base":  1523.0,
          "quote": 3.21e7,
          "buy_base":  90234.0, "sell_base":  88711.0,
          "buy_quote": 1.742e9,  "sell_quote": 1.709e9,
          "count_buy": 12345, "count_sell": 12010
        },
        "windows": {              # 滚动窗口
          "sec": {
            "10": {"delta_base": 120.0, "delta_quote": 3.1e5,
                   "buy_base":  630.0, "sell_base": 510.0,
                   "buy_quote": 1.65e6,"sell_quote": 1.34e6,
                   "count_buy": 52, "count_sell": 44,
                   "tbr_base": 0.553, "tbr_quote": 0.551},    # taker-buy ratio
            "60": { ... }
          },
          "min": {
            "1":  {...},
            "5":  {...}
          }
        }
      }
    }

参考/口径：
- CVD：在给定区间内“taker 买量 - taker 卖量”的累计与滚动值。:contentReference[oaicite:1]{index=1}
- OKX trades 的 `side` 为成交的“taker 方向”；可据此计算买/卖成交量与 CVD。:contentReference[oaicite:2]{index=2}
"""

from __future__ import annotations

from dataclasses import dataclass, field
from collections import deque, defaultdict
from typing import Deque, Dict, List, Optional, Tuple

import math
import time
import threading


Number = float
Ms = int


# ===== 工具 =====

def _now_ms() -> Ms:
    return int(time.time() * 1000)


def _is_buy(side: str) -> bool:
    return str(side).lower() == "buy"


@dataclass
class _WinSum:
    """
    单一时间窗的“可过期累加器”
    - 使用 deque 存储进入窗口的交易贡献项
    - O(1) 推入 & 按时间逐步弹出过期项
    """
    window_ms: Ms
    # 元素: (ts, base, quote, is_buy)
    q: Deque[Tuple[Ms, Number, Number, bool]] = field(default_factory=deque)
    # 运行和
    buy_base: Number = 0.0
    sell_base: Number = 0.0
    buy_quote: Number = 0.0
    sell_quote: Number = 0.0
    cnt_buy: int = 0
    cnt_sell: int = 0

    def push(self, ts: Ms, base: Number, quote: Number, is_buy: bool) -> None:
        self.q.append((ts, base, quote, is_buy))
        if is_buy:
            self.buy_base += base
            self.buy_quote += quote
            self.cnt_buy += 1
        else:
            self.sell_base += base
            self.sell_quote += quote
            self.cnt_sell += 1

    def expire(self, now_ms: Ms) -> None:
        cutoff = now_ms - self.window_ms
        q = self.q
        while q and q[0][0] < cutoff:
            ts, base, quote, is_buy = q.popleft()
            if is_buy:
                self.buy_base -= base
                self.buy_quote -= quote
                self.cnt_buy -= 1
            else:
                self.sell_base -= base
                self.sell_quote -= quote
                self.cnt_sell -= 1

    def snapshot(self, now_ms: Ms) -> Dict[str, Number]:
        self.expire(now_ms)
        buy_b, sell_b = max(0.0, self.buy_base), max(0.0, self.sell_base)
        buy_q, sell_q = max(0.0, self.buy_quote), max(0.0, self.sell_quote)
        sum_b = buy_b + sell_b
        sum_q = buy_q + sell_q
        tbr_b = (buy_b / sum_b) if sum_b > 0 else 0.0
        tbr_q = (buy_q / sum_q) if sum_q > 0 else 0.0
        return {
            "delta_base": buy_b - sell_b,
            "delta_quote": buy_q - sell_q,
            "buy_base": buy_b, "sell_base": sell_b,
            "buy_quote": buy_q, "sell_quote": sell_q,
            "count_buy": max(0, self.cnt_buy),
            "count_sell": max(0, self.cnt_sell),
            "tbr_base": tbr_b, "tbr_quote": tbr_q
        }


@dataclass
class _InstState:
    """单个 instId 的 CVD 状态"""
    # 自启动以来累计
    cum_buy_base: Number = 0.0
    cum_sell_base: Number = 0.0
    cum_buy_quote: Number = 0.0
    cum_sell_quote: Number = 0.0
    cum_cnt_buy: int = 0
    cum_cnt_sell: int = 0
    # 窗口集合
    windows: Dict[Ms, _WinSum] = field(default_factory=dict)
    # 最近成交（诊断/可选输出）
    last_trade: Optional[dict] = None


class CVDTracker:
    """
    CVD 指标服务（线程安全；单进程内使用）
    ------------------------------------------------
    • 支持多 instId 并行维护
    • 支持秒级/分钟级多窗口
    • 口径：base（合约张数）与 quote（名义额=px*sz）同时计算
    ------------------------------------------------
    用法：
        cvd = CVDTracker(windows_sec=[10,60], windows_min=[1,5])
        cvd.update("BTC-USDT-SWAP", {"ts":..., "px":..., "sz":..., "side":"buy"})
        snap = cvd.build_snapshot("BTC-USDT-SWAP")  # dict → 可直接发 MQ
    """
    def __init__(
        self,
        windows_sec: List[int] | Tuple[int, ...] = (10, 30, 60),
        windows_min: List[int] | Tuple[int, ...] = (1, 5),
        max_queue_minutes: int = 15
    ):
        # 归一化与上限
        uniq_sec = sorted({int(x) for x in windows_sec if int(x) > 0})
        uniq_min = sorted({int(x) for x in windows_min if int(x) > 0})
        self._windows_ms: List[Ms] = [s * 1000 for s in uniq_sec] + [m * 60_000 for m in uniq_min]
        self._max_keep_ms: Ms = max(self._windows_ms) if self._windows_ms else 60_000
        # 万一用户配了过大窗口，设置一个软上限：不超过 max_queue_minutes
        self._max_keep_ms = min(self._max_keep_ms, int(max_queue_minutes) * 60_000)

        self._states: Dict[str, _InstState] = defaultdict(_InstState)
        self._lock = threading.RLock()
        self._sec_labels = [str(s) for s in uniq_sec]
        self._min_labels = [str(m) for m in uniq_min]

    # ---------- 输入 ----------
    def update(self, inst_id: str, trade: dict) -> None:
        """
        推入一笔成交（来自 OKX WS trades）
        期待字段：ts(ms), px(float), sz(float), side('buy'|'sell')
        """
        ts = int(trade.get("ts") or 0)
        px = float(trade.get("px") or 0.0)
        sz = float(trade.get("sz") or 0.0)
        side = str(trade.get("side") or "").lower()

        if ts <= 0 or px <= 0 or sz <= 0 or side not in ("buy", "sell"):
            return

        is_buy = _is_buy(side)
        base = sz
        quote = px * sz

        with self._lock:
            st = self._states[inst_id]
            # 初始化窗口
            if not st.windows:
                for w in self._windows_ms:
                    st.windows[w] = _WinSum(w)

            # 1) 累计（自启动）
            if is_buy:
                st.cum_buy_base += base
                st.cum_buy_quote += quote
                st.cum_cnt_buy += 1
            else:
                st.cum_sell_base += base
                st.cum_sell_quote += quote
                st.cum_cnt_sell += 1

            # 2) 窗口累加
            for win in st.windows.values():
                win.push(ts, base, quote, is_buy)

            # 3) 记录最近成交
            st.last_trade = {"ts": ts, "px": px, "sz": sz, "side": side}

            # 4) 简单修剪（若极端闲时，仍可防止队列过长）
            #    仅按“最慢窗口”剪切，内部 expire() 会精确边界。
            cutoff = ts - self._max_keep_ms
            for win in st.windows.values():
                # 只要最早元素还在就尝试弹出；略过快路径
                if win.q and win.q[0][0] < cutoff:
                    win.expire(ts)

    # ---------- 输出 ----------
    def build_snapshot(self, inst_id: str, now_ms: Optional[Ms] = None) -> Optional[dict]:
        now = _now_ms() if now_ms is None else int(now_ms)
        with self._lock:
            st = self._states.get(inst_id)
            if not st or not st.windows:
                return None

            cum_buy_b = max(0.0, st.cum_buy_base)
            cum_sell_b = max(0.0, st.cum_sell_base)
            cum_buy_q = max(0.0, st.cum_buy_quote)
            cum_sell_q = max(0.0, st.cum_sell_quote)

            cum = {
                "base":  cum_buy_b - cum_sell_b,
                "quote": cum_buy_q - cum_sell_q,
                "buy_base":  cum_buy_b, "sell_base":  cum_sell_b,
                "buy_quote": cum_buy_q, "sell_quote": cum_sell_q,
                "count_buy": max(0, st.cum_cnt_buy),
                "count_sell": max(0, st.cum_cnt_sell),
            }

            win_sec: Dict[str, dict] = {}
            win_min: Dict[str, dict] = {}
            for w in sorted(st.windows.keys()):
                snap = st.windows[w].snapshot(now)
                # 放进秒或分钟命名空间
                if w % 60_000 == 0:
                    label = str(w // 60_000)
                    win_min[label] = snap
                else:
                    label = str(w // 1000)
                    win_sec[label] = snap

            return {
                "instId": inst_id,
                "ts": now,
                "cvd": {
                    "cum": cum,
                    "windows": {
                        "sec": win_sec,
                        "min": win_min
                    }
                },
                "last_trade": st.last_trade,
                "labels": {"sec": self._sec_labels, "min": self._min_labels}
            }

    # ---------- 管理 ----------
    def reset(self, inst_id: Optional[str] = None) -> None:
        """重置某个 instId 或全部"""
        with self._lock:
            if inst_id is None:
                self._states.clear()
            else:
                self._states.pop(inst_id, None)

    def instruments(self) -> List[str]:
        with self._lock:
            return list(self._states.keys())


# ===== 便捷工厂 =====

def build_from_config(cfg: dict) -> CVDTracker:
    """
    从 config.yaml 注入：
    features:
      windows:
        cvd_sec: 60
        # 你也可以传数组: cvd_secs: [10, 30, 60]
        #                cvd_mins: [1, 5]
    """
    feat = (cfg or {}).get("features", {}) or {}
    win = (feat.get("windows") or {}) if isinstance(feat.get("windows"), dict) else {}
    # 兼容单值与数组两种写法
    sec = []
    if "cvd_secs" in win and isinstance(win["cvd_secs"], (list, tuple)):
        sec = [int(x) for x in win["cvd_secs"]]
    elif "cvd_sec" in win:
        sec = [int(win["cvd_sec"])]

    mins = []
    if "cvd_mins" in win and isinstance(win["cvd_mins"], (list, tuple)):
        mins = [int(x) for x in win["cvd_mins"]]
    elif "cvd_min" in win:
        mins = [int(win["cvd_min"])]

    # 提供默认：60s + 1/5 分钟
    if not sec:
        sec = [60]
    if not mins:
        mins = [1, 5]

    return CVDTracker(windows_sec=sec, windows_min=mins)
