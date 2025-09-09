# /root/copyTrade/indicator/features/profile.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import math
import time
import threading
from dataclasses import dataclass, field
from typing import Dict, Deque, Optional, Tuple, List
from collections import deque, defaultdict

import numpy as np
import logging

Number = float
Ms = int


# ============================= 工具 =============================

def _now_ms() -> Ms:
    return int(time.time() * 1000)


def _floor_to_step(px: float, step: float) -> float:
    """
    将价格落入离散价阶（bin 起点），与 okx_client._bin_price/sqlite 对齐：
      - 先对 (px/step) 加微小 eps 再 floor，避免 0.1*3 ≠ 0.3 的浮点误差
      - 最后保留 8 位小数，避免键值抖动
    """
    b = math.floor((px + 1e-12) / step) * step
    return float(round(b, 8))


def _is_local_max(v: np.ndarray, i: int) -> bool:
    if i <= 0 or i >= v.size - 1:
        return False
    return v[i] > v[i - 1] and v[i] > v[i + 1]


def _is_local_min(v: np.ndarray, i: int) -> bool:
    if i <= 0 or i >= v.size - 1:
        return False
    return v[i] < v[i - 1] and v[i] < v[i + 1]


# ============================= 数据结构 =============================

@dataclass
class _Event:
    ts: Ms
    bucket: float
    base: Number   # 成交量（张/币）
    quote: Number  # 名义额（price*size）
    tpo: int       # TPO 计数（基于采样）


@dataclass
class _Buckets:
    """一个 instId 的聚合桶（支持过期扣减）"""
    step: float
    # bucket(px)->(base, quote, tpo)
    agg: Dict[float, Tuple[Number, Number, int]] = field(default_factory=dict)
    # 时间有序队列，用于窗口/会话过期扣减
    q: Deque[_Event] = field(default_factory=deque)
    total_base: Number = 0.0
    total_quote: Number = 0.0
    total_tpo: int = 0

    def add(self, ev: _Event):
        b = self.agg.get(ev.bucket)
        if b is None:
            self.agg[ev.bucket] = (ev.base, ev.quote, ev.tpo)
        else:
            self.agg[ev.bucket] = (b[0] + ev.base, b[1] + ev.quote, b[2] + ev.tpo)
        self.q.append(ev)
        self.total_base += ev.base
        self.total_quote += ev.quote
        self.total_tpo += ev.tpo

    def _dec(self, ev: _Event):
        b = self.agg.get(ev.bucket)
        if b is None:
            return
        nb = (b[0] - ev.base, b[1] - ev.quote, b[2] - ev.tpo)
        # 清理到 0
        if nb[0] <= 1e-12 and nb[1] <= 1e-8 and nb[2] <= 0:
            self.agg.pop(ev.bucket, None)
        else:
            self.agg[ev.bucket] = (max(0.0, nb[0]), max(0.0, nb[1]), max(0, nb[2]))
        self.total_base = max(0.0, self.total_base - ev.base)
        self.total_quote = max(0.0, self.total_quote - ev.quote)
        self.total_tpo = max(0, self.total_tpo - ev.tpo)

    def expire_until(self, cutoff_ms: Ms) -> list[_Event]:
        """过期并返回被移除的事件（供外层清理索引）。"""
        dropped: list[_Event] = []
        while self.q and self.q[0].ts < cutoff_ms:
            ev = self.q.popleft()
            self._dec(ev)
            dropped.append(ev)
        return dropped

    # 导出为升序数组
    def to_arrays(self, use_quote: bool) -> Tuple[np.ndarray, np.ndarray]:
        if not self.agg:
            return np.empty(0, dtype=np.float64), np.empty(0, dtype=np.float64)
        prices = np.fromiter(self.agg.keys(), dtype=np.float64)
        v = np.array(
            [self.agg[p][1] if use_quote else self.agg[p][0] for p in prices],
            dtype=np.float64,
        )
        order = np.argsort(prices)
        return prices[order], v[order]

    def to_arrays_tpo(self) -> Tuple[np.ndarray, np.ndarray]:
        if not self.agg:
            return np.empty(0, dtype=np.float64), np.empty(0, dtype=np.float64)
        prices = np.fromiter(self.agg.keys(), dtype=np.float64)
        v = np.array([self.agg[p][2] for p in prices], dtype=np.float64)
        order = np.argsort(prices)
        return prices[order], v[order]


@dataclass
class _InstState:
    step: float
    # 窗口/会话配置
    rolling_ms: Optional[Ms] = None   # 滚动窗口（毫秒）；None 表示不用
    session_mode: str = "day"         # "day"|"none"
    session_tz_offset_min: int = 0    # 会话时区偏移（分钟），默认 UTC
    # L1 -> TPO 采样节流（避免同一毫秒多次+1）
    last_tpo_sample_slot: Optional[int] = None
    tpo_sample_ms: int = 30_000       # 30s 
    # 诊断节流：最近一次日志时间
    last_diag_ms: Optional[int] = None
    # 最近一次入队事件的时间，用于乱序诊断
    last_enq_ts: Optional[int] = None
    # 聚合桶
    buckets: _Buckets = field(init=False)
    # —— 新增：逐笔压缩（VP 聚合）——
    vp_sample_ms: int = 60_000                 # 将成交按价阶×时间槽合并
    last_vp_evt: Dict[tuple[float, int], _Event] = field(default_factory=dict)
    def __post_init__(self):
        self.buckets = _Buckets(step=self.step)


# ============================= 主引擎 =============================

class ProfileEngine:
    """
    Volume Profile / Market Profile（TPO）结构锚点
    ----------------------------------------------------------------
    • update_trade(instId, trade):   逐笔（price, size）→ Volume Profile
    • update_l1(instId, mid, ts):    采样（如每30s一次）→ TPO/Market Profile
    • snapshot(instId):              计算 POC / VAH / VAL / HVN / LVN
    ----------------------------------------------------------------
    POC: 最大体量价位
    VAH/VAL: 自 POC 向两侧扩张，直至覆盖 ~value_area_pct 的总体量
    HVN/LVN: 直方图局部极大/极小 + 显著性过滤
    """

    def __init__(
        self,
        price_step: float,
        use_quote: bool = False,               # True: 用名义额做体量；False: 用张数
        value_area_pct: float = 0.70,         # 70% 价值区
        hvn_lvn_min_bins: int = 3,            # 至少多少 bins 才检测节点
        hvn_prominence_pct: float = 12.0,     # HVN 显著性阈值（相对均值 %）
        lvn_prominence_pct: float = 12.0,     # LVN 显著性阈值（相对均值 %）
        max_nodes: int = 5,                   # HVN/LVN 各最多输出几个
        rolling_ms: Optional[Ms] = None,      # 滚动窗口，不用则 None
        session_mode: str = "day",            # "day"|"none"
        session_tz_offset_min: int = 0,       # 以分钟表示的时区偏移
        tpo_sample_ms: int = 30_000,          # L1 采样间隔（近似 TPO）
        vp_sample_ms: int = 60_000,           # 成交合并时间槽（毫秒）
        steps_by_inst: Optional[Dict[str, float]] = None,  # 可按 instId 覆盖更细的步长
        emit_nodes: bool = True,               # 是否输出 HVN/LVN；默认开启
    ):
        assert price_step > 0, "price_step 必须 > 0"
        self.price_step = float(price_step)
        self.use_quote = bool(use_quote)
        self.value_area_pct = max(0.01, min(0.99, float(value_area_pct)))
        self.hvn_lvn_min_bins = int(max(3, hvn_lvn_min_bins))
        self.hvn_prom = float(max(0.0, hvn_prominence_pct))
        self.lvn_prom = float(max(0.0, lvn_prominence_pct))
        self.max_nodes = int(max(1, max_nodes))
        self.emit_nodes = bool(emit_nodes)
        self.rolling_ms = int(rolling_ms) if rolling_ms else None
        self.session_mode = session_mode
        self.session_tz_offset_min = int(session_tz_offset_min)
        self.tpo_sample_ms = int(tpo_sample_ms)
        self.vp_sample_ms = int(vp_sample_ms)
        # instId 定制步长（仅当提供且 >0 时生效）
        self._steps_by_inst: Dict[str, float] = {}
        if isinstance(steps_by_inst, dict):
            for k, v in steps_by_inst.items():
                try:
                    fv = float(v)
                    if fv > 0:
                        self._steps_by_inst[str(k)] = fv
                except Exception:
                    continue
        self._states: Dict[str, _InstState] = {}
        self._lock = threading.RLock()

    # --------------------- 输入：成交 ---------------------

    def update_trade(self, inst_id: str, trade: dict) -> None:
        """
        trade: {ts(ms), px, sz}
        """
        ts = int(trade.get("ts") or 0)
        # 兜底：若拿到的是“秒级时间戳”，统一升为毫秒
        if 0 < ts < 10_000_000_000:  # 10位多半是秒
            ts *= 1000
        px = float(trade.get("px") or 0.0)
        sz = float(trade.get("sz") or 0.0)
        if ts <= 0 or px <= 0 or sz <= 0:
            # 诊断：记录被丢弃成交（只在 DEBUG 级别出现）
            logging.info(
                "[PROFILE][DROP] inst=%s bad trade: ts=%r px=%r sz=%r raw=%r",
                inst_id, trade.get("ts"), trade.get("px"), trade.get("sz"), trade,
            )
            return

        with self._lock:
            st = self._ensure_state(inst_id)
            self._maybe_rotate_session(st, ts)
            # 乱序诊断（你现在是按时间升序喂，若触发说明上游仍有乱序）
            if st.last_enq_ts is not None and ts < st.last_enq_ts:
                logging.warning("[PROFILE][DIAG] inst=%s trade ts out-of-order: last=%d curr=%d", inst_id, st.last_enq_ts, ts)
 
            bucket_px = _floor_to_step(px, st.step)
            if st.vp_sample_ms <= 0:
                # 关闭合并：逐笔入队（旧语义）
                ev = _Event(ts=ts, bucket=bucket_px, base=sz, quote=px * sz, tpo=0)
                st.buckets.add(ev)
                st.last_enq_ts = ts
                self._expire(inst_id, st, ts)
                return
            slot = ts // st.vp_sample_ms
            slot_end_ts = (slot + 1) * st.vp_sample_ms - 1
            key = (bucket_px, slot)
            # 已有同价阶×时间槽 → 合并
            if key in st.last_vp_evt:
                ev = st.last_vp_evt[key]
                # 更新事件累计
                ev.base += sz
                ev.quote += px * sz
                # 更新聚合桶累计（不新增 queue 项）
                b = st.buckets.agg.get(bucket_px)
                if b is None:
                    st.buckets.agg[bucket_px] = (sz, px * sz, 0)
                else:
                    st.buckets.agg[bucket_px] = (b[0] + sz, b[1] + px * sz, b[2])
                st.buckets.total_base += sz
                st.buckets.total_quote += px * sz
            else:
                # 用槽末时刻做事件时间，避免边界早退
                ev = _Event(ts=slot_end_ts, bucket=bucket_px, base=sz, quote=px * sz, tpo=0)
                st.buckets.add(ev)
                st.last_vp_evt[key] = ev
            st.last_enq_ts = ts
            self._expire(inst_id, st, ts)

    # ===== 作为 OKXClient 回调的适配层（保持签名一致，外部可直接传入） =====
    # on_trade(instId, trade_dict)
    def on_trade(self, inst_id: str, trade: dict):
        # 同步即可；OKXClient 会在是协程时才 await
        self.update_trade(inst_id, trade)

    # on_tpo_sample(instId, mid_px, ts_ms)
    def on_tpo_sample(self, inst_id: str, mid_px: float, ts_ms: int):
        self.update_l1(inst_id, mid_px, ts_ms)
    # --------------------- 输入：L1/TPO 采样 ---------------------

    def update_l1(self, inst_id: str, mid_px: float, ts_ms: Optional[Ms] = None) -> None:
        """
        用 L1 中价按固定间隔采样生成 TPO 近似。
        每个采样槽（tpo_sample_ms）最多贡献 1 次 tpo 计数。
        """
        ts = int(ts_ms or _now_ms())
        if mid_px <= 0:
            return

        with self._lock:
            st = self._ensure_state(inst_id)
            self._maybe_rotate_session(st, ts)

            slot = ts // st.tpo_sample_ms
            if st.last_tpo_sample_slot == slot:
                return
            st.last_tpo_sample_slot = slot

            bucket_px = _floor_to_step(mid_px, st.step)
            ev = _Event(ts=ts, bucket=bucket_px, base=0.0, quote=0.0, tpo=1)
            st.buckets.add(ev)
            st.last_enq_ts = ts
            self._expire(inst_id, st, ts)

    # --------------------- 计算与导出 ---------------------

    def snapshot(self, inst_id: str, now_ms: Optional[Ms] = None) -> Optional[dict]:
        now = int(now_ms or _now_ms())
        with self._lock:
            st = self._states.get(inst_id)
            if not st:
                return None
            self._expire(inst_id, st, now)

            # Volume profile
            px_v, vol = st.buckets.to_arrays(self.use_quote)
            # TPO profile（可为空）
            px_tpo, tpo = st.buckets.to_arrays_tpo()

            anchors_vol = self._compute_anchors(px_v, vol, self.value_area_pct)
            anchors_tpo = self._compute_anchors(px_tpo, tpo, self.value_area_pct) if tpo.size else None
           
            if px_v.size > 0:
                last_log = st.last_diag_ms or 0
                if (now - last_log) >= 3000:
                    total_vol = float(st.buckets.total_quote if self.use_quote else st.buckets.total_base)
                    hvn_px = [float(x[0]) for x in anchors_vol.get("hvn", [])] if self.emit_nodes else []
                    lvn_px = [float(x[0]) for x in anchors_vol.get("lvn", [])] if self.emit_nodes else []

                    # 分别计算：队列总体、仅成交(Volume)、仅TPO 的时间覆盖（min）
                    def _span_min(pred) -> float:
                        q = st.buckets.q
                        if not q:
                            return 0.0
                        mn = None; mx = None
                        for e in q:
                            if pred(e):
                                if (mn is None) or (e.ts < mn): mn = e.ts
                                if (mx is None) or (e.ts > mx): mx = e.ts
                        if mn is None or mx is None or mx < mn:
                            return 0.0
                        return (mx - mn) / 60000.0
                    used_min_all = _span_min(lambda e: True)
                    used_min_vol = _span_min(lambda e: (e.base > 0.0 or e.quote > 0.0))
                    used_min_tpo = _span_min(lambda e: (e.tpo > 0))
                    target_min = int((st.rolling_ms or 0) / 60000) if st.rolling_ms else None
                    logging.info(
                        "[PROFILE][VOL] inst=%s used_all=%.1f%s used_vol=%.1f used_tpo=%.1f bins=%d total=%.6f POC=%s VAH=%s VAL=%s HVN=%s LVN=%s",
                        inst_id,
                        used_min_all,
                        (f"/{target_min}(min)" if target_min else "(min)"),
                        used_min_vol, used_min_tpo,
                        int(px_v.size), total_vol,
                        anchors_vol.get("poc"), anchors_vol.get("vah"), anchors_vol.get("val"),
                        hvn_px, lvn_px
                    )
                    st.last_diag_ms = now
            out = {
                "instId": inst_id,
                "ts": now,
                "cfg": {
                    "step": st.step,
                    "use_quote": self.use_quote,
                    "value_area_pct": self.value_area_pct,
                    "rolling_ms": st.rolling_ms,
                    "session_mode": st.session_mode,
                    "tpo_sample_ms": st.tpo_sample_ms,
                    "vp_sample_ms": st.vp_sample_ms,
                },
                "profile": {
                    "volume": {
                        "bins": int(px_v.size),
                        "poc": anchors_vol.get("poc"),
                        "vah": anchors_vol.get("vah"),
                        "val": anchors_vol.get("val"),
                        "hvn": anchors_vol.get("hvn", []),   # 恢复输出
                        "lvn": anchors_vol.get("lvn", []),   # 恢复输出
                        "total": float(st.buckets.total_quote if self.use_quote else st.buckets.total_base),
                    },
                    "tpo": {
                        "bins": int(px_tpo.size),
                        "poc": anchors_tpo.get("poc") if anchors_tpo else None,
                        "vah": anchors_tpo.get("vah") if anchors_tpo else None,
                        "val": anchors_tpo.get("val") if anchors_tpo else None,
                        "hvn": anchors_tpo.get("hvn", []) if anchors_tpo else [],  # 可选
                        "lvn": anchors_tpo.get("lvn", []) if anchors_tpo else [],  # 可选
                        "total": int(st.buckets.total_tpo),
                    } if tpo.size else None
                }
            }
            # 按需移除 HVN/LVN 字段（允许通过 emit_nodes 关闭）
            if not self.emit_nodes:
                vol = out["profile"]["volume"]
                vol.pop("hvn", None); vol.pop("lvn", None)
                if out["profile"]["tpo"]:
                    tpo_dict = out["profile"]["tpo"]
                    tpo_dict.pop("hvn", None); tpo_dict.pop("lvn", None)
            return out
    # --------------------- 内部：锚点计算 ---------------------

    def _compute_anchors(self, px: np.ndarray, v: np.ndarray, va_pct: float) -> dict:
        res = {"poc": None, "vah": None, "val": None, "hvn": [], "lvn": []}
        if v.size == 0:
            return res

        # POC：最大体量价位
        poc_idx = int(np.argmax(v))
        res["poc"] = float(px[poc_idx])

        # 价值区（约 70%）——自 POC 向两侧扩张
        total = float(np.nansum(v))
        if total <= 0:
            return res
        target = total * va_pct

        l = r = poc_idx
        acc = float(v[poc_idx])

        # 从 POC 向两边逐步纳入“体量更大”的一侧
        while acc < target and (l > 0 or r < v.size - 1):
            left_val = v[l - 1] if l > 0 else -1.0
            right_val = v[r + 1] if r < v.size - 1 else -1.0
            if right_val > left_val:
                r = min(v.size - 1, r + 1)
                acc += float(v[r])
            else:
                l = max(0, l - 1)
                acc += float(v[l])

        res["val"] = float(px[l])
        res["vah"] = float(px[r])

        # HVN/LVN：基于局部极大/极小 + 显著性（相对均值）
        if v.size >= self.hvn_lvn_min_bins:
            mean_v = float(np.nanmean(v)) if v.size else 0.0
            hvn = []
            lvn = []
            for i in range(v.size):
                if _is_local_max(v, i) and v[i] >= mean_v * (1.0 + self.hvn_prom / 100.0):
                    hvn.append((float(px[i]), float(v[i])))
                if _is_local_min(v, i) and v[i] <= mean_v * (1.0 - self.lvn_prom / 100.0):
                    lvn.append((float(px[i]), float(v[i])))
            # 体量降序/升序取前 K
            hvn.sort(key=lambda x: x[1], reverse=True)
            lvn.sort(key=lambda x: x[1])
            res["hvn"] = hvn[: self.max_nodes]
            res["lvn"] = lvn[: self.max_nodes]

        return res

    # --------------------- 内部：会话/窗口维护 ---------------------

    def _ensure_state(self, inst_id: str) -> _InstState:
        st = self._states.get(inst_id)
        if st:
            return st
        # 支持按 instId 覆盖更细步长（未提供则使用全局 step）
        step = self._steps_by_inst.get(inst_id, self.price_step)
        st = _InstState(
            step=step,
            rolling_ms=self.rolling_ms,
            session_mode=self.session_mode,
            session_tz_offset_min=self.session_tz_offset_min,
            tpo_sample_ms=self.tpo_sample_ms,
        )
        # 让实例态沿用当前引擎设置的合并粒度
        st.vp_sample_ms = self.vp_sample_ms
        self._states[inst_id] = st
        return st

    def _expire(self, inst_id: str, st: _InstState, now_ms: Ms):
        # 1) 滚动窗口
        if st.rolling_ms:
            cutoff = now_ms - st.rolling_ms
            _before_q = len(st.buckets.q)
            dropped = st.buckets.expire_until(cutoff)
            # 清理“最后事件索引”（仅在开启合并时才有意义）
            if dropped and st.vp_sample_ms > 0:
                for ev in dropped:
                    slot = ev.ts // st.vp_sample_ms
                    st.last_vp_evt.pop((ev.bucket, slot), None)
            _after_q = len(st.buckets.q)
            _dropped = _before_q - _after_q
            if _dropped > 0:
                logging.info(
                    "[PROFILE][EXPIRE] inst=%s dropped=%d window_ms=%d q=%d->%d "
                    "total_base=%.6f total_quote=%.6f total_tpo=%d cutoff=%d now=%d",
                    inst_id,
                    _dropped, st.rolling_ms, _before_q, _after_q,
                    st.buckets.total_base, st.buckets.total_quote, st.buckets.total_tpo,
                    cutoff, now_ms,
                )

    def _maybe_rotate_session(self, st: _InstState, ts: Ms):
        if st.session_mode != "day":
            return
        # 会话边界：当地午夜（按 tz 偏移）
        tz = st.session_tz_offset_min
        # 将 ts（ms）换算到“本地分钟”
        local_min = (ts // 60000) + tz
        local_day = local_min // (60 * 24)
        # 缓存上一条事件的 day 索引在 _Buckets 上（用 total_tpo 的符号位不合适，单独记录）
        # 简化：在 _InstState 上记录
        last_day = getattr(st, "_last_session_day", None)
        if last_day is None:
            st._last_session_day = local_day
            return
        if local_day != last_day:
            # 新会话 → 直接清空旧桶
            st.buckets = _Buckets(step=st.step)
            st._last_session_day = local_day
            st.last_vp_evt.clear()
            st.last_tpo_sample_slot = None


# ============================= 便捷工厂 =============================

def build_from_config(cfg: dict) -> ProfileEngine:
    """
    从 config.yaml 读取：
    features:
      profile:
        session: "day"            # 或 "none"
        step: 0.1                 # 价阶（未给出则需外部传入）
        steps_by_inst:            # 可选：按 instId 覆盖更细的步长
          SOL-USDT-SWAP: 0.01
        window_max_minutes: 0     # 可选：当 windows.profile_ms 未配置时，作为“最多 N 分钟”的滚动窗
 
        use_quote: false          # 体量采用名义额
        value_area_pct: 0.7
        tpo_sample_ms: 30000
        hvn_prominence_pct: 12
        lvn_prominence_pct: 12
        max_nodes: 5
      windows:
        profile_ms: 0             # >0 则启用滚动窗口（覆盖 session）
    """
    feat = (cfg or {}).get("features", {}) or {}
    prof = (feat.get("profile") or {}) if isinstance(feat.get("profile"), dict) else {}
    win = (feat.get("windows") or {}) if isinstance(feat.get("windows"), dict) else {}

    step = float(prof.get("step", 0.0))
    if step <= 0:
        raise ValueError("features.profile.step 未配置或无效（必须 > 0）")

    session = str(prof.get("session", "day")).lower()
    use_quote = bool(prof.get("use_quote", False))
    va_pct = float(prof.get("value_area_pct", 0.80))
    tpo_ms = int(prof.get("tpo_sample_ms", 30_000))
    vp_ms  = int(prof.get("vp_sample_ms", 60_000))
    hvn_prom = float(prof.get("hvn_prominence_pct", 12.0))
    lvn_prom = float(prof.get("lvn_prominence_pct", 12.0))
    max_nodes = int(prof.get("max_nodes", 5))
    rolling_ms = int(win.get("profile_ms", 0) or 0) or None
    # 若 windows.profile_ms 未配置，可用 profile.window_max_minutes 作为“最多 N 分钟”的滚动窗
    if not rolling_ms:
        max_min = int((prof.get("window_max_minutes") or 0) or 0)
        if max_min > 0:
            rolling_ms = max_min * 60_000

    tz_off_min = int(prof.get("session_tz_offset_min", 0))
    steps_by_inst = prof.get("steps_by_inst") or {}
    return ProfileEngine(
        price_step=step,
        use_quote=use_quote,
        value_area_pct=va_pct,
        hvn_lvn_min_bins=3,
        hvn_prominence_pct=hvn_prom,
        lvn_prominence_pct=lvn_prom,
        max_nodes=max_nodes,
        rolling_ms=rolling_ms,
        session_mode=session,
        session_tz_offset_min=tz_off_min,
        tpo_sample_ms=tpo_ms,
        vp_sample_ms=vp_ms,
        steps_by_inst=steps_by_inst,
        emit_nodes=bool(prof.get("emit_nodes", True)),
    )

# ============================= 新增：多尺度并行管理器 =============================
class MultiProfile:
    """
    以多个滚动窗口并行维护 ProfileEngine：
      - minutes_list: 例如 [480, 1440, 4320, 10080] → 8h/24h/72h/7d
      - labels: 与 minutes_list 对应的标签；不提供则用 "480m" 等
      - 其余配置与 ProfileEngine 一致（step/use_quote/value_area_pct/steps_by_inst 等）
    用法：把 on_trade/on_tpo_sample 直接接到 OKXClient 回调即可。
    """
    def __init__(
        self,
        minutes_list: List[int],
        labels: Optional[List[str]],
        *,
        price_step: float,
        use_quote: bool = False,
        value_area_pct: float = 0.70,
        tpo_sample_ms: int = 30_000,
        vp_sample_ms: int = 60_000,
        steps_by_inst: Optional[Dict[str, float]] = None,
        emit_nodes: bool = True,
        session_tz_offset_min: int = 0,
    ):
        assert minutes_list, "minutes_list 不能为空"
        self._engines: Dict[str, ProfileEngine] = {}
        for i, m in enumerate(minutes_list):
            label = (labels[i] if labels and i < len(labels) else f"{int(m)}m")
            self._engines[label] = ProfileEngine(
                price_step=price_step,
                use_quote=use_quote,
                value_area_pct=value_area_pct,
                hvn_lvn_min_bins=3,
                hvn_prominence_pct=12.0,
                lvn_prominence_pct=12.0,
                max_nodes=5,
                rolling_ms=int(m) * 60_000,
                session_mode="none",  # 多尺度滚动窗下禁用会话切换
                session_tz_offset_min=session_tz_offset_min,
                tpo_sample_ms=tpo_sample_ms,
                vp_sample_ms=vp_sample_ms,
                steps_by_inst=steps_by_inst,
                emit_nodes=emit_nodes,
            )

    # 直接对接 OKXClient 回调
    def on_trade(self, inst_id: str, trade: dict):
        for e in self._engines.values():
            e.on_trade(inst_id, trade)

    def on_tpo_sample(self, inst_id: str, mid_px: float, ts_ms: int):
        for e in self._engines.values():
            e.on_tpo_sample(inst_id, mid_px, ts_ms)

    def snapshot_all(self, inst_id: str) -> Dict[str, Optional[dict]]:
        return {label: e.snapshot(inst_id) for label, e in self._engines.items()}

    def aggregate_nodes(self, inst_id: str) -> Dict[str, List[Tuple[float, float, str]]]:
        """
        汇总所有窗口的 HVN/LVN（仅 volume profile），返回：
          {"hvn": [(price, vol, label), ...], "lvn": [(price, vol, label), ...]}
        """
        out: Dict[str, List[Tuple[float, float, str]]] = {"hvn": [], "lvn": []}
        for label, e in self._engines.items():
            snap = e.snapshot(inst_id)
            if not snap:
                continue
            vol = (snap.get("profile", {}) or {}).get("volume") or {}
            for p, v in (vol.get("hvn") or []):
                out["hvn"].append((float(p), float(v), label))
            for p, v in (vol.get("lvn") or []):
                out["lvn"].append((float(p), float(v), label))
        return out

    def nearest_nodes(
       self,
        inst_id: str,
        ref_price: float,
        *,
        max_per_side: int = 2,
        dedup_bins: float = 1.0,
    ) -> Dict[str, List[Tuple[float, float, str]]]:
        """
        跨全部窗口查找距离 ref_price 最近的上方 HVN 与下方 LVN（用于分批止盈/止损）：
          - max_per_side: 每侧最多返回几个价位
          - dedup_bins: 合并阈值（以“价阶”的倍数”度量），1.0 表示同一价阶内去重
        返回：
          {"up_hvn": [(price, vol, label), ...], "down_lvn": [(price, vol, label), ...]}
        """
        nodes = self.aggregate_nodes(inst_id)
        # 估算价阶：从任一窗口 snapshot 的 cfg.step 读取
        step = None
        for e in self._engines.values():
            snap = e.snapshot(inst_id)
            if snap:
                step = float((snap.get("cfg") or {}).get("step") or 0.0) or step
        step = step or 1e-8

        def _pick(seq: List[Tuple[float, float, str]], side: str) -> List[Tuple[float, float, str]]:
            seq = sorted(seq, key=lambda x: (abs(x[0] - ref_price), -x[1]))  # 先按距离、再按体量
            kept: List[Tuple[float, float, str]] = []
            for p, v, label in seq:
                if side == "up" and p < ref_price:
                    continue
                if side == "down" and p > ref_price:
                    continue
                if not kept or all(abs(p - kp) > dedup_bins * step for kp, _, _ in kept):
                    kept.append((p, v, label))
                if len(kept) >= max_per_side:
                    break
            return kept

        return {
            "up_hvn": _pick(nodes["hvn"], "up"),
            "down_lvn": _pick(nodes["lvn"], "down"),
        }

def build_multi_from_config(cfg: dict) -> MultiProfile:
    """
    新增工厂：从 config.yaml 读取多尺度窗口。
    示例：
      features:
        profile:
          step: 0.1
          use_quote: false
          value_area_pct: 0.7
          tpo_sample_ms: 30000
          emit_nodes: false
          multi_windows_minutes: [480, 1440, 4320, 10080]
          labels: ["8h", "24h", "72h", "7d"]
          steps_by_inst:
            SOL-USDT-SWAP: 0.01
    """
    feat = (cfg or {}).get("features", {}) or {}
    prof = (feat.get("profile") or {}) if isinstance(feat.get("profile"), dict) else {}
    step = float(prof.get("step", 0.0))
    if step <= 0:
        raise ValueError("features.profile.step 未配置或无效（必须 > 0）")
    windows = prof.get("multi_windows_minutes") or []
    if not windows:
        raise ValueError("features.profile.multi_windows_minutes 不能为空")
    labels = prof.get("labels") or None
    return MultiProfile(
        minutes_list=[int(x) for x in windows],
        labels=labels,
        price_step=step,
        use_quote=bool(prof.get("use_quote", False)),
        value_area_pct=float(prof.get("value_area_pct", 0.70)),
        tpo_sample_ms=int(prof.get("tpo_sample_ms", 30_000)),
        vp_sample_ms=int(prof.get("vp_sample_ms", 60_000)),
        steps_by_inst=prof.get("steps_by_inst") or {},
        emit_nodes=bool(prof.get("emit_nodes", True)),
        session_tz_offset_min=int(prof.get("session_tz_offset_min", 0)),
    )