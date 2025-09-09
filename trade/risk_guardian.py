# -*- coding: utf-8 -*-
"""
risk_guardian.py
——————————————
可直接被 trade.py 导入调用的 **Volume Profile 风控助手**：

主要功能（纯函数/无外部依赖）：
- 基于 VP 的入场价修正（使用 VOL/VOH/POC + Single Lines）
- **仅生成 3 个止损点（SL1/2/3）**；止盈由上层基于 HVN/LVN 节点（`nodes.tp_nodes`）执行，
  若节点不满足条件则回退到 ATR 的利润保护（由本模块提供 `profit_protect_ref`）。
- 价格步进与最小间距控制（tick / ratio）
- 在 VP 不完整时，尽力从给定的 L2 档位近似推断（可选）
典型用法（在你的 trade.py 里）：
--------------------------------------------------------------------
from risk_guardian import RiskGuardian

rg = RiskGuardian()  # 可传参微调：min_tick、min_level_gap_ratio 等

levels = rg.eval(
    side="LONG",                         # 或 "SHORT"
    mid=mid_px,                          # 标记价/中间价
    best_bid=best_bid, best_ask=best_ask,# 顶档
    base_entry=desired_px,               # 你希望的入场价（可不传）
    vp={                                 # Volume Profile（字段名兼容）
        "poc": 62850.5,
        "vah": 63120.0,  # == voh
        "val": 62480.0,  # == vol
        "hvn": [62580, 62850, 63090],
        "lvn": [62690, 62940],
        "single_lines": [[62670, 62695], {"low":62920,"high":62935}]
    },
    atr_pct=0.8                          # 可选：ATR%（用于兜底扩展 TP/SL）
)

print(levels)
# {
#   "entry_px":  desired_px(or mid),
#   "entry_adj": 62960.0,
#   "sl": [62810.0, 62695.0, 62480.0],
#   "anchors": {"poc":..., "vah":..., "val":..., "nearest_support":..., "nearest_resist":...}
# }
--------------------------------------------------------------------
"""

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple, Iterable
import math
import time
import logging

# =========================
# 数据结构
# =========================
@dataclass
class VolumeProfile:
    poc: Optional[float] = None
    vah: Optional[float] = None  # == voh
    val: Optional[float] = None  # == vol
    hvn: List[float] = field(default_factory=list)  # 高成交密集节点
    lvn: List[float] = field(default_factory=list)  # 低成交/薄弱区
    single_lines: List[Tuple[float, float]] = field(default_factory=list)  # 区间(low, high)
    ts: int = 0

    # —— 以某参考价 px 视角得到“下方支撑/上方阻力”候选 —— #
    def supports_below(self, px: float) -> List[float]:
        cands: List[float] = []
        if self.val: cands.append(float(self.val))
        cands += [float(x) for x in self.hvn if _is_pos(x) and float(x) <= px]
        # Single 区间：下沿视为支撑
        for lo, hi in self.single_lines:
            if _is_pos(lo) and lo <= px:
                cands.append(float(lo))
        # LVN 也可视为“易穿透后变支撑/阻力”的薄弱区边缘，这里不默认纳入（避免过度拥挤）
        return sorted(set([x for x in cands if x < px]))

    def resistances_above(self, px: float) -> List[float]:
        cands: List[float] = []
        if self.poc: cands.append(float(self.poc))
        if self.vah: cands.append(float(self.vah))
        cands += [float(x) for x in self.hvn if _is_pos(x) and float(x) >= px]
        # Single 区间：上沿视为阻力
        for lo, hi in self.single_lines:
            if _is_pos(hi) and hi >= px:
                cands.append(float(hi))
        return sorted(set([x for x in cands if x > px]))


# =========================
# 主类
# =========================
class RiskGuardian:
    def __init__(
        self,
        *,
        min_tick: float = 0.0,                 # 价格精度（0=自动推断）
        min_level_gap_ratio: float = 0.001,    # 相邻级别最小间距（占 mid）
        level_pad_ratio: float = 0.0005,       # 入场锚点边缘垫步
        max_spread_ratio: float = 0.003,       # 保护：entry_adj 不偏离 mid 超过 ~0.3% 的半幅
        # ===== 新增：按 ROE% 约束最小间距 =====
        min_roe_gap_pct: float = 30.0,         # 最小 ROE 距离（%）
        leverage: float = 10.0,                # 评估用杠杆（用于把 ROE% 换算成价格间距）
        contract_type: str = "linear",          # "linear"（USDT本位，默认）；保留扩展位
        # ===== 新增：多时段节点计划（HVN/LVN） =====
        node_min_roe_pct: float = 30.0,        # 仅当达到该 ROE% 的 HVN 才纳入“盈利保护”
        node_dedup_ticks: int = 2              # 节点去重，距离 <= N*tick 视为同一节点
    ):
        self.min_tick = float(min_tick)
        self.min_level_gap_ratio = float(min_level_gap_ratio)
        self.level_pad_ratio = float(level_pad_ratio)
        self.max_spread_ratio = float(max_spread_ratio)
        self.min_roe_gap_pct = float(min_roe_gap_pct)
        self.leverage = max(1e-6, float(leverage))
        self.contract_type = str(contract_type).lower()
        self.node_min_roe_pct = float(node_min_roe_pct)
        self.node_dedup_ticks = int(node_dedup_ticks)
        self.log = logging.getLogger("risk_guardian")

    # --------- 对外主入口（纯计算，不落单） --------- #
    def eval(
        self,
        *,
        side: str,                   # "LONG" / "SHORT"
        mid: float,                  # 中间价/标记价
        best_bid: float,
        best_ask: float,
        base_entry: Optional[float] = None,   # 你希望的入场价（可不传）
        vp: Optional[Dict[str, Any] | VolumeProfile] = None,
        vp_multi: Optional[Dict[str, Any]] = None,         # 新增：多时段 VP {"480":{...},"960":{...},...}
 
        l2: Optional[Dict[str, Iterable[Iterable[float]]]] = None,  # {"bids":[[px,sz],...], "asks":[[px,sz],...]}
        atr_pct: Optional[float] = None,      # 若传入，将用于兜底扩展 TP/SL
        tick: Optional[float] = None
    ) -> Dict[str, Any]:
        """
        返回：
        {
          "entry_px": base_entry 或 mid,
          "entry_adj": float,
          "sl": [sl1,sl2,sl3],
          "guards": {"profit_protect_ref":..., "loss_protect_ref":...},
          "nodes": {"tp_nodes":[...], "sl_nodes":[...]}
        }
        """
        side = (side or "").upper().strip()
        if side not in ("LONG", "SHORT"):
            raise ValueError("side 必须为 'LONG' 或 'SHORT'")
        mid = float(mid);  best_bid = float(best_bid);  best_ask = float(best_ask)
        if not (mid > 0 and best_ask > best_bid > 0):
            raise ValueError("需要有效的 mid / best_bid / best_ask")

        tick = float(tick) if tick else (self.min_tick or _infer_tick(mid))
        sp = best_ask - best_bid
        self.log.info("[eval.start] side=%s mid=%.6f bid=%.6f ask=%.6f tick=%.6f base_entry=%s",
                      side, mid, best_bid, best_ask, tick, str(base_entry))
        # —— 解析/构造 VolumeProfile —— #
        vp_obj = self._ensure_vp(vp, l2=l2, mid=mid)
        if vp_obj:
            self.log.info("[vp] poc=%s vah(voh)=%s val(vol)=%s hvn=%d lvn=%d singles=%d ts=%s",
                          str(vp_obj.poc), str(vp_obj.vah), str(vp_obj.val),
                          len(vp_obj.hvn), len(vp_obj.lvn), len(vp_obj.single_lines), str(vp_obj.ts))
 
        # —— 选锚点（入场修正）—— #
        supports = vp_obj.supports_below(mid) if vp_obj else []
        resists  = vp_obj.resistances_above(mid) if vp_obj else []
        self.log.debug("[vp.cands] supports_below@mid(%s)=%s", f"{mid:.6f}", _fmt_levels(supports))
        self.log.debug("[vp.cands] resists_above@mid(%s)=%s",  f"{mid:.6f}", _fmt_levels(resists))
 
        if side == "LONG":
            anchor = _nearest_leq(supports + ([vp_obj.val] if getattr(vp_obj, "val", None) else []), best_bid)
            entry_adj = (anchor + self._pad(-1, tick, mid)) if anchor else min(best_bid, base_entry or best_bid)
            entry_adj = min(entry_adj, best_bid)  # maker 友好
        else:
            anchor = _nearest_geq(resists + ([vp_obj.vah] if getattr(vp_obj, "vah", None) else []), best_ask)
            entry_adj = (anchor + self._pad(+1, tick, mid)) if anchor else max(best_ask, base_entry or best_ask)
            entry_adj = max(entry_adj, best_ask)
        self.log.info("[entry.anchor] side=%s anchor=%s pad=%.6f raw_adj=%.6f",
                      side, str(anchor), (self._pad(-1 if side=='LONG' else +1, tick, mid)), float(entry_adj))
 
        entry_adj = _clip(entry_adj, mid, sp, self.max_spread_ratio)
        entry_adj_rounded = _round_tick(entry_adj, tick, side)
        self.log.info("[entry.final] clipped=%.6f rounded=%.6f spread=%.6f max_spread_ratio=%.5f",
                      float(entry_adj), float(entry_adj_rounded), float(sp), self.max_spread_ratio)
        entry_adj = entry_adj_rounded

        # —— 止损（SL）三级 —— #
        if side == "LONG":
            sl_candidates = [x for x in supports if x < entry_adj]
            if getattr(vp_obj, "val", None):
                sl_candidates.append(float(vp_obj.val))
            sl_candidates = sorted(set(sl_candidates))
        else:
            sl_candidates = [x for x in resists if x > entry_adj]
            if getattr(vp_obj, "vah", None):
                sl_candidates.append(float(vp_obj.vah))
            sl_candidates = sorted(set(sl_candidates))

        sl = self._take_levels(sl_candidates, entry_adj, side, forward=False, mid=mid, tick=tick)
        self.log.info("[levels.raw] sl=%s", _fmt_levels(sl))
        # —— SL 候选不足 → 用 VA 宽度 / ATR% 作兜底扩展 —— #

        # —— 候选不足 → 用 VA 宽度 / ATR% 作兜底扩展 —— #
        va_w = 0.0
        if vp_obj and vp_obj.vah and vp_obj.val and vp_obj.vah > vp_obj.val:
            va_w = float(vp_obj.vah) - float(vp_obj.val)

        sl = self._fill_if_short(sl, entry_adj, side, forward=False, mid=mid, tick=tick, va_w=va_w, atr_pct=atr_pct)
        self.log.info("[levels.final] sl=%s via_fallback(step≈VA/ATR if needed)", _fmt_levels(sl))
        # —— SL：最小 ROE 距离强制（避免在一个价带反复止损）—— #
        min_gap_px = self._min_gap_px_from_roe(entry_adj)
        sl_before = list(sl)
        sl = self._enforce_min_roe_gap(sl, entry_adj, side, forward=False, tick=tick, min_gap_px=min_gap_px)
        if sl != sl_before:
            self.log.info("[roe.min_gap] entry=%.6f lev=%.2f roe%%=%.1f min_gap_px=%.6f | sl:%s→%s",
                          entry_adj, self.leverage, self.min_roe_gap_pct, min_gap_px,
                          _fmt_levels(sl_before), _fmt_levels(sl))
        # —— 新增：多时段 HVN/LVN 计划（盈利/亏损保护的“节点”序列）—— #
        nodes = {}
        if isinstance(vp_multi, dict) and vp_multi:
            try:
                nodes = self._plan_nodes(entry_adj, side, vp_multi, tick)
                self.log.info("[nodes.multi] side=%s entry=%.6f hvn_tp=%s lvn_sl=%s",
                              side, entry_adj, _fmt_levels(nodes.get("tp_nodes", [])), _fmt_levels(nodes.get("sl_nodes", [])))
            except Exception as e:
                self.log.warning("[nodes.multi][skip] %s", e)
        # —— 盈利保护：优先用 HVN/LVN 节点；若无则回退到 ATR/VA 步长 —— #
        # 回退步长：取 max(VA/3, ATR%*mid, 5*tick)
        step = max(va_w / 3.0 if va_w > 0 else 0.0, (atr_pct or 0.0) / 100.0 * mid, tick * 5)
        sgn = +1.0 if side == "LONG" else -1.0
        # 向盈利方向取整：LONG 用 "SHORT"(向上)、SHORT 用 "LONG"(向下)
        profit_round_side = "SHORT" if side == "LONG" else "LONG"
        profit_fallback = _round_tick(entry_adj + sgn * step, tick, profit_round_side)
        tp_nodes = nodes.get("tp_nodes") or []
        profit_protect_ref = (tp_nodes[0] if tp_nodes else profit_fallback)
        loss_protect_ref   = (sl[0] if sl else None)
        self.log.info("[protect] profit_ref=%s (fallback=%s) loss_ref=%s",
                      str(profit_protect_ref), str(profit_fallback), str(loss_protect_ref))
        out = {
            "entry_px": float(base_entry if base_entry else mid),
            "entry_adj": float(entry_adj),
            "sl": [float(x) for x in sl],
            "anchors": {
                "poc": getattr(vp_obj, "poc", None),
                "vah": getattr(vp_obj, "vah", None),
                "val": getattr(vp_obj, "val", None),
                "nearest_support": float(anchor) if (side == "LONG" and anchor) else None,
                "nearest_resist": float(anchor) if (side == "SHORT" and anchor) else None
            },
            "guards": {
                "profit_protect_ref": profit_protect_ref,
                "loss_protect_ref": loss_protect_ref
            },
            # 新增：多时段节点计划（供 trade.py 在 INFO 中按节点分级减仓/止损）
            "nodes": nodes
        }
        return out

    # =========================
    # 内部：构造/解析 VP
    # =========================
    def _ensure_vp(self, vp: Optional[Dict[str, Any] | VolumeProfile], *, l2: Optional[Dict[str, Any]], mid: float) -> VolumeProfile:
        if isinstance(vp, VolumeProfile):
            return vp
        if isinstance(vp, dict) and vp:
            return self._parse_vp_dict(vp)
        # 没有明确 vp，尝试从 L2 粗略估计（可选）
        if isinstance(l2, dict) and (l2.get("bids") or l2.get("asks")):
            return _infer_vp_from_l2(l2, mid) or VolumeProfile(ts=int(time.time()*1000))
        # 最少给一份空壳，避免分支判断
        return VolumeProfile(ts=int(time.time()*1000))

    def _parse_vp_dict(self, raw: Dict[str, Any]) -> VolumeProfile:
        poc = _get_float(raw, "poc")
        vah = _get_float(raw, "vah") or _get_float(raw, "voh")
        val = _get_float(raw, "val") or _get_float(raw, "vol")
        hvn = _as_float_list(raw.get("hvn") or raw.get("hvns") or [])
        lvn = _as_float_list(raw.get("lvn") or raw.get("lvns") or [])
        singles = _to_pairs(raw.get("single_lines") or raw.get("single") or [])
        return VolumeProfile(
            poc=poc, vah=vah, val=val, hvn=sorted(set(hvn)), lvn=sorted(set(lvn)),
            single_lines=singles, ts=int(raw.get("ts") or time.time()*1000)
        )

    # =========================
    # 内部：构建级别
    # =========================
    def _take_levels(
        self, cands: List[float], entry: float, side: str, *, forward: bool, mid: float, tick: float
    ) -> List[float]:
        """
        forward=True 表示朝“盈利方向”挑级别：
          - LONG：>entry 的阻力
          - SHORT：<entry 的支撑
        forward=False 表示朝“止损方向”挑级别。
        """
        out: List[float] = []
        min_gap = max(self.min_level_gap_ratio * mid, tick * 2)
        seq = cands if forward else list(reversed(cands))
        for px in seq:
            if side == "LONG":
                ok = (px > entry) if forward else (px < entry)
            else:
                ok = (px < entry) if forward else (px > entry)
            if not ok:
                continue
            if out and abs(px - out[-1]) < min_gap:
                continue
            out.append(_round_tick(px, tick, side if forward else _opp(side)))
            if len(out) == 3:
                break
        return out

    def _fill_if_short(
        self, arr: List[float], entry: float, side: str, *, forward: bool, mid: float, tick: float,
        va_w: float, atr_pct: Optional[float]
    ) -> List[float]:
        if len(arr) >= 3:
            return arr
        res = list(arr)
        # 用 VA 宽度 / ATR% 推出步长（取更大的那个）
        step = max(va_w / 3.0 if va_w > 0 else 0.0, (atr_pct or 0.0) / 100.0 * mid)
        step = max(step, tick * 5)
        last = res[-1] if res else entry
        while len(res) < 3:
            last = last + (step if (forward == (side == "LONG")) else -step)
            res.append(_round_tick(last, tick, side if forward else _opp(side)))
        return res[:3]
    # ===== 新增：多时段 HVN/LVN 计划 =====
    def _plan_nodes(self, entry: float, side: str, vp_multi: Dict[str, Any], tick: float) -> Dict[str, Any]:
        """
        汇总各时间窗（键：如 "480"/"960"/"1440"/"4320"）里的 hvn/lvn，
        生成两条序列：
          - tp_nodes：多单↑(空单↓) 的 HVN 触发价（仅保留达到 node_min_roe_pct 的节点）
          - sl_nodes：多单↓(空单↑) 的 LVN 触发价（多单要求“先跌破入场价再触发”留给 trade.py 判断）
        """
        def _grab_list(d: dict, key: str) -> List[float]:
            arr = d.get(key) or []
            vals: List[float] = []
            for it in arr:
                try:
                    if isinstance(it, (list, tuple)) and len(it) >= 1:
                        vals.append(float(it[0]))
                    else:
                        vals.append(float(it))
                except Exception:
                    continue
            return [x for x in vals if _is_pos(x)]

        # 节点的最小间距按“节点专用的 ROE 门槛”换算（默认 30% ROE），
        # 若未设置则退回到全局 min_roe_gap_pct
        roe_pct = self.node_min_roe_pct if self.node_min_roe_pct > 0 else self.min_roe_gap_pct
        # 线性合约近似：Δpx_min = entry * (roe% / (100 * leverage))
        min_gap_px = max(self.min_tick or 0.0, abs(entry) * (roe_pct / (100.0 * self.leverage)))
        min_gap_px = max(min_gap_px, tick)
        # 汇总各窗
        hvn_all: List[float] = []
        lvn_all: List[float] = []
        for _, raw in vp_multi.items():
            try:
                hvn_all += _grab_list(raw, "hvn")
                lvn_all += _grab_list(raw, "lvn")
            except Exception:
                continue

        hvn_all = sorted(set(hvn_all))
        lvn_all = sorted(set(lvn_all))

        # 去重：相邻 <= node_dedup_ticks * tick 视为同一节点
        def _dedup(arr: List[float]) -> List[float]:
            if not arr: return arr
            out = [arr[0]]
            tol = max(tick * max(1,self.node_dedup_ticks), 1e-12)
            for x in arr[1:]:
                if abs(x - out[-1]) > tol:
                    out.append(x)
            return out
        hvn_all = _dedup(hvn_all)
        lvn_all = _dedup(lvn_all)

        # 方向筛选 + ROE 门槛（仅对盈利方向的 HVN 应用）
        if side.upper()=="LONG":
            hvn_dir = [x for x in hvn_all if x > entry and (x - entry) >= min_gap_px]
            lvn_dir = [x for x in lvn_all if x < entry]
            tp_nodes = [_round_tick(px, tick, "LONG") for px in hvn_dir][:3]
            sl_nodes = [_round_tick(px, tick, "SHORT") for px in sorted(lvn_dir, reverse=True)][:3]  # 先近后远
        else:
            hvn_dir = [x for x in hvn_all if x < entry and (entry - x) >= min_gap_px]
            lvn_dir = [x for x in lvn_all if x > entry]
            tp_nodes = [_round_tick(px, tick, "SHORT") for px in hvn_dir[::-1]][:3]  # 空单盈利向下：更低的先触发
            sl_nodes = [_round_tick(px, tick, "LONG") for px in sorted(lvn_dir)][:3]

        return {"tp_nodes": tp_nodes, "sl_nodes": sl_nodes}
    def _pad(self, direction: int, tick: float, mid: float) -> float:
        # direction: +1 向上，-1 向下
        return direction * max(tick, self.level_pad_ratio * mid)
    # ===== 新增：把 ROE% 转成价格最小间距（默认线性合约近似） =====
    def _min_gap_px_from_roe(self, entry: float) -> float:
        if self.contract_type == "linear":
            # ROE% ≈ ((px - entry)/entry) * leverage * 100
            # → Δpx_min = entry * (roe% / (100 * leverage))
            return max(self.min_tick or 0.0, abs(entry) * (self.min_roe_gap_pct / (100.0 * self.leverage)))
        # 其它类型（如逆合约）先用同一近似，避免过拟合；需要时可按实际公式扩展
        return max(self.min_tick or 0.0, abs(entry) * (self.min_roe_gap_pct / (100.0 * self.leverage)))

    # ===== 新增：对 TP/SL 强制最小 ROE 间距 =====
    def _enforce_min_roe_gap(
        self, levels: List[float], entry: float, side: str, *, forward: bool, tick: float, min_gap_px: float
    ) -> List[float]:
        """
        forward=True: 朝盈利方向（LONG↑ / SHORT↓）；forward=False: 朝止损方向（LONG↓ / SHORT↑）。
        规则：
          1) 第一个点到入场 ≥ min_gap_px
          2) 相邻点彼此 ≥ min_gap_px
          3) 若候选不足，按 min_gap_px 从上/下外推补齐到 3 个
        """
        if not levels:
            levels = []
        # 方向排序：确保从“离 entry 最近的有效点”开始往外挑
        if side.upper() == "LONG":
            levels = sorted(levels, reverse=not forward)  # 盈利↑ / 止损↓
        else:  # SHORT
            levels = sorted(levels, reverse=forward)      # 盈利↓ / 止损↑

        res: List[float] = []
        # 判定与推进方向
        def valid_first(px: float) -> bool:
            if side.upper() == "LONG":
                return (px >= entry + min_gap_px) if forward else (px <= entry - min_gap_px)
            else:
                return (px <= entry - min_gap_px) if forward else (px >= entry + min_gap_px)

        def step_from(px: float) -> float:
            sgn = +1.0
            if side.upper() == "LONG":
                sgn = +1.0 if forward else -1.0
            else:
                sgn = -1.0 if forward else +1.0
            return px + sgn * min_gap_px

        # 1) 过滤并挑选满足条件的候选
        for px in levels:
            if not res:
                if not valid_first(px):
                    continue
                res.append(px)
            else:
                if abs(px - res[-1]) + 1e-12 >= min_gap_px:
                    res.append(px)
            if len(res) == 3:
                break

        # 2) 不足则按 min_gap_px 外推补齐
        cur = None
        if not res:
            # 从 entry 起步
            cur = step_from(entry)
        else:
            cur = step_from(res[-1])
        while len(res) < 3:
            px = _round_tick(cur, tick, side if forward else _opp(side))
            res.append(px)
            cur = step_from(px)
        return res[:3]

# =========================
# 工具函数
# =========================
def _is_pos(x: Any) -> bool:
    try:
        return float(x) > 0
    except Exception:
        return False


def _get_float(d: Dict[str, Any], *keys: str) -> Optional[float]:
    for k in keys:
        if k in d and _is_pos(d[k]):
            try:
                return float(d[k])
            except Exception:
                pass
    return None


def _as_float_list(arr: Iterable[Any]) -> List[float]:
    out: List[float] = []
    for x in arr:
        try:
            fx = float(x)
            if fx > 0:
                out.append(fx)
        except Exception:
            continue
    return out


def _to_pairs(items: Any) -> List[Tuple[float, float]]:
    """
    接受：
      - [[lo,hi], ...]
      - [{"low":..,"high":..}, ...]
      - [px1, px2, ...]  # 单点 → (px, px)
    """
    out: List[Tuple[float, float]] = []
    if items is None:
        return out
    for it in (items if isinstance(items, (list, tuple)) else [items]):
        try:
            if isinstance(it, dict):
                lo = float(it.get("low", it.get("lo")))
                hi = float(it.get("high", it.get("hi")))
            elif isinstance(it, (list, tuple)) and len(it) >= 2:
                lo, hi = float(it[0]), float(it[1])
            else:
                lo = hi = float(it)
            if lo > 0 and hi > 0 and hi >= lo:
                out.append((lo, hi))
        except Exception:
            continue
    return out


def _nearest_leq(arr: List[float], x: float) -> Optional[float]:
    arr = [v for v in arr if _is_pos(v) and v <= x]
    return max(arr) if arr else None


def _nearest_geq(arr: List[float], x: float) -> Optional[float]:
    arr = [v for v in arr if _is_pos(v) and v >= x]
    return min(arr) if arr else None


def _round_tick(px: float, tick: float, side: str) -> float:
    if tick <= 0:
        return float(px)
    if side.upper() == "LONG":
        return math.floor(px / tick) * tick
    return math.ceil(px / tick) * tick


def _infer_tick(mid: float) -> float:
    m = abs(float(mid))
    if m >= 100000: return 1.0
    if m >= 10000:  return 0.5
    if m >= 1000:   return 0.1
    if m >= 100:    return 0.01
    if m >= 10:     return 0.001
    if m >= 1:      return 0.0001
    return 0.00001


def _clip(entry_px: float, mid: float, spread: float, max_spread_ratio: float) -> float:
    # 限制 entry_adj 不要离 mid 太远。使用“半幅”保护：min(0.9×spread, mid×ratio)
    cap = max(min(0.9 * spread, abs(mid) * max_spread_ratio), 1e-12)
    return max(min(entry_px, mid + cap), mid - cap)


def _opp(side: str) -> str:
    return "SHORT" if side.upper() == "LONG" else "LONG"


def _infer_vp_from_l2(l2: Dict[str, Any], mid: float) -> Optional[VolumeProfile]:
    """
    近似从 L2 (前 10 档) 估计 VP（效果弱于真实 VP，但可兜底）：
      - POC = 最大 volume 档位
      - 价值区 ~ 70% 体量，近似给出 vah/val
      - HVN/LVN 以均值阈值粗分
    """
    bids = l2.get("bids") or []
    asks = l2.get("asks") or []
    if not bids and not asks:
        return None
    bins: Dict[float, float] = {}
    def add(side):
        for row in side[:10]:
            try:
                p = float(row[0]); v = float(row[1])
                if p > 0 and v > 0:
                    bins[p] = bins.get(p, 0.0) + v
            except Exception:
                continue
    add(bids); add(asks)
    if not bins:
        return None
    poc = max(bins.items(), key=lambda kv: kv[1])[0]
    total = sum(bins.values())
    target = total * 0.7
    # 按距离 POC 由近到远累加，覆盖至 70%，并据此确定 VA 边界
    ordered = sorted(bins.items(), key=lambda kv: abs(kv[0] - poc))
    acc = 0.0
    hi_side = [poc]   # ≥ POC
    lo_side = [poc]   # ≤ POC
    for px, vol in ordered:
        acc += vol
        if px >= poc:
            hi_side.append(px)
        else:
            lo_side.append(px)
        if acc >= target:
            break
    vah = max(hi_side) if hi_side else poc
    val = min(lo_side) if lo_side else poc
    mean = total / max(1, len(bins))
    hvn = [px for px, v in bins.items() if v >= 1.3 * mean]
    lvn = [px for px, v in bins.items() if v <= 0.6 * mean]
    return VolumeProfile(poc=poc, vah=vah, val=val, hvn=sorted(hvn), lvn=sorted(lvn), single_lines=[], ts=int(time.time()*1000))


# =========================
# —— 方便：从原始事件快速计算 —— #
# =========================
def quick_eval_from_event(
    evt: Dict[str, Any],
    *,
    side_field: str = "side",            # 期望 "LONG"/"SHORT"
    price_fields: Tuple[str, ...] = ("midPx", "last", "px"),
    bid_field: str = "best_bid",
    ask_field: str = "best_ask",
    vp_field: str = "vp",
    l2_fields: Tuple[str, str] = ("bids", "asks"),
    strict_prices: bool = True,
    **rg_kwargs
) -> Optional[Dict[str, Any]]:
    """
    便捷封装：直接把 MQ/回调里的事件字典扔进来（若字段名一致）。
    你也可以不用这个函数，直接实例化 RiskGuardian().eval(...) 更清晰。
    """
    side = (evt.get(side_field) or "").upper()
    # 读取 instId/交易对（尽量兼容多种字段名）
    inst = (evt.get("instId") or evt.get("symbol") or evt.get("instrument") 
            or evt.get("pair") or evt.get("ccyPair"))
    # 价格：先尝试 mid/last/px；若无则看 bbo.mid；Bid/Ask 优先字段/再看 bbo/l1
    mid = None
    for k in price_fields:
        if evt.get(k) not in (None, "", 0, "0"):
            try:
                mid = float(evt[k]); break
            except Exception:
                continue
    if mid is None and isinstance(evt.get("bbo"), dict):
        try:
            mid = float(evt["bbo"].get("mid"))
        except Exception:
            pass
    if mid is None:
        if strict_prices:
            raise ValueError(
                f"事件中缺少 mid/last/px 价格字段 "
                f"(inst={inst or '?'}, evt_keys={sorted(list(evt.keys()))})"
            )
        return None


    # 顶档
    best_bid = evt.get(bid_field)
    best_ask = evt.get(ask_field)
    # 从 bbo 兜底
    if (best_bid is None or best_ask is None) and isinstance(evt.get("bbo"), dict):
        try:
            best_bid = best_bid if best_bid is not None else evt["bbo"].get("best_bid")
            best_ask = best_ask if best_ask is not None else evt["bbo"].get("best_ask")
        except Exception:
            pass
    if (best_bid is None or best_ask is None) and isinstance(evt.get("l1"), dict):
        l1 = evt["l1"]
        try:
            if isinstance(l1.get("bid"), (list, tuple)) and l1.get("bid"):
                best_bid = l1["bid"][0]
            if isinstance(l1.get("ask"), (list, tuple)) and l1.get("ask"):
                best_ask = l1["ask"][0]
        except Exception:
            pass
    if best_bid is None or best_ask is None:
        # 若仍为空，尝试从 L2 提示里兜底
        bids, asks = evt.get(l2_fields[0]) or [], evt.get(l2_fields[1]) or []
        if bids and asks:
            try:
                best_bid, best_ask = float(bids[0][0]), float(asks[0][0])
            except Exception:
                pass
    if best_bid is None or best_ask is None:
        if strict_prices:
            # 把上下文一次性打印全：是否带 bbo/l1、l2 档位长度等
            has_bbo = isinstance(evt.get("bbo"), dict)
            has_l1  = isinstance(evt.get("l1"), dict)
            bids_len = len(evt.get(l2_fields[0]) or [])
            asks_len = len(evt.get(l2_fields[1]) or [])
            raise ValueError(
                "事件缺少 best_bid / best_ask "
                f"(inst={inst or '?'}, has_bbo={has_bbo}, has_l1={has_l1}, "
                f"bids_len={bids_len}, asks_len={asks_len}, evt_keys={sorted(list(evt.keys()))})"
            )
        return None


    # VP / L2：优先从 indicator 的 profile.volume 扁平化；否则维持原兼容逻辑
    vol_prof = (evt.get("profile") or {}).get("volume") if isinstance(evt.get("profile"), dict) else None
    if isinstance(vol_prof, dict) and vol_prof:
        hvn = []
        lvn = []
        try:
            # indicator 的 hvn/lvn 是 [(px, val), ...]；这里只取价格
            hvn = [float(px) for px, _ in (vol_prof.get("hvn") or []) if _is_pos(px)]
            lvn = [float(px) for px, _ in (vol_prof.get("lvn") or []) if _is_pos(px)]
        except Exception:
            pass
        vp = {
            "poc": vol_prof.get("poc"),
            "vah": vol_prof.get("vah"),
            "val": vol_prof.get("val"),
            "hvn": hvn,
            "lvn": lvn,
            "single_lines": vol_prof.get("single_lines") or vol_prof.get("single") or [],
            "ts": vol_prof.get("ts") or evt.get("ts")
        }
    else:
        vp = evt.get(vp_field) or {
            "poc": evt.get("poc"),
            "vah": evt.get("vah") or evt.get("voh"),
            "val": evt.get("val") or evt.get("vol"),
            "hvn": evt.get("hvn") or evt.get("hvns"),
            "lvn": evt.get("lvn") or evt.get("lvns"),
            "single_lines": evt.get("single_lines") or evt.get("single")
        }
    # 合并 TPO 的 single prints（若提供）
    tpo = (evt.get("profile") or {}).get("tpo") if isinstance(evt.get("profile"), dict) else None
    if isinstance(tpo, dict):
       vp["single_lines"] = (vp.get("single_lines") or []) + _to_pairs(tpo.get("single_prints") or [])

    # ===== 新增：多时段 VP（来自 MQ 的 profile_multi） =====
    vp_multi = None
    try:
        # 兼容：顶层 profile_multi 或 profile.multi / profile.volume_multi
        vp_multi = (evt.get("profile_multi")
                    or ((evt.get("profile") or {}).get("multi"))
                    or ((evt.get("profile") or {}).get("volume_multi")))
        if isinstance(vp_multi, dict):
            # 仅保留有用字段，保证是 {win:{poc,vah,val,hvn,lvn}} 结构
            clean = {}
            for k, v in vp_multi.items():
                if not isinstance(v, dict): continue
                # 兼容 {"volume": {...}} 这种嵌套结构
                vol = v.get("volume") if isinstance(v.get("volume"), dict) else v
                def _take_prices(arr):
                    out = []
                    for it in (arr or []):
                        try:
                            if isinstance(it, (list, tuple)) and len(it) >= 1:
                                px = float(it[0])
                            else:
                                px = float(it)
                            if px > 0:
                                out.append(px)
                        except Exception:
                            continue
                    return out
                clean[k] = {
                    "poc": (vol.get("poc")),
                    "vah": (vol.get("vah") or vol.get("voh")),
                    "val": (vol.get("val") or vol.get("vol")),
                    "hvn": _take_prices(vol.get("hvn") or vol.get("hvns")),
                    "lvn": _take_prices(vol.get("lvn") or vol.get("lvns")),
                }
            vp_multi = clean if clean else None
        else:
            vp_multi = None
    except Exception:
        vp_multi = None

    l2 = None
    if evt.get(l2_fields[0]) or evt.get(l2_fields[1]):
        l2 = {"bids": evt.get(l2_fields[0]) or [], "asks": evt.get(l2_fields[1]) or []}

    rg = RiskGuardian(**rg_kwargs)
    res = rg.eval(
        side=side, mid=float(mid), best_bid=float(best_bid), best_ask=float(best_ask),
        base_entry=float(evt.get("entry_px") or evt.get("desired_px") or mid),
        vp=vp, vp_multi=vp_multi, l2=l2,
        atr_pct=float(((evt.get("vol") or {}).get("atr_pct") if isinstance(evt.get("vol"), dict) else evt.get("atr_pct")) or 0.0) or None

    )
    # —— 可选：基于 MQ 的流动性/毒性线索进行“拦截反向开单”建议 —— #
    # 读取 ofi / vpin / microprice_delta_bps（indicator 的快照里一般都带）
    ofi_val = _read_ofi(evt)
    vpin_toxic = bool(((evt.get("vpin_risk") or {}).get("toxic")) or ((evt.get("vpin") or {}).get("risk", {}) or {}).get("toxic") or False)
    mp_dbps = _read_mp_delta_bps(evt)
    block, reason = _decide_block_reverse(side, ofi_val, vpin_toxic, mp_dbps)
    logging.getLogger("risk_guardian").info("[block_check] side=%s ofi=%s vpin_toxic=%s mp_delta_bps=%s => block=%s reason=%s",
                                            side, str(ofi_val), vpin_toxic, str(mp_dbps), block, reason)
    res["policy"] = {"block": block, "reason": reason}
    return res

def eval_with_features_snapshot(snap: Dict[str, Any], *, side: str, base_entry: float | None = None, **rg_kwargs) -> Dict[str, Any]:
    """
    从 MQ 的 features.snapshot 直接评估（不依赖 md_feed）：
      side: "LONG"/"SHORT"
      base_entry: 期望基础入场价（可选，缺省用 mid）
    """
    args = adapt_indicator_snap_to_rg_args(snap, side=side, base_entry=base_entry)
    # 将 snapshot 原文附在 evt 里，沿用上面的 quick_eval_from_event 路径做拦截判定与日志
    evt = dict(snap)
    evt.update({
        "side": side,
        "entry_px": args.get("base_entry") or args.get("mid"),
        "bbo": {"mid": args["mid"], "best_bid": args["best_bid"], "best_ask": args["best_ask"]},
        "profile": snap.get("profile"),
        "vol": snap.get("vol")
    })
    return quick_eval_from_event(evt, **rg_kwargs)

def adapt_indicator_snap_to_rg_args(snap: Dict[str, Any], *, side: str, base_entry: float | None = None) -> Dict[str, Any]:
    """
    便捷适配器：把 indicator 的 features.snapshot 直接转换为 RiskGuardian.eval 的参数集合。
    用法： RiskGuardian().eval(**adapt_indicator_snap_to_rg_args(snap, side="LONG", base_entry=...))
    """
    bbo = snap.get("bbo") or {}
    inst = (snap.get("instId") or snap.get("symbol") or snap.get("instrument") 
            or snap.get("pair") or snap.get("ccyPair"))
    mid = bbo.get("mid") or ((snap.get("features") or {}).get("microprice") or {}).get("value")
    best_bid = bbo.get("best_bid")
    best_ask = bbo.get("best_ask")
    if not (mid and best_bid and best_ask):
        raise ValueError(
            f"snapshot 缺少 bbo.mid/best_bid/best_ask "
            f"(inst={inst or '?'}, has_bbo={bool(bbo)}, mid={mid}, bid={best_bid}, ask={best_ask})"
        )
    vol_prof = (snap.get("profile") or {}).get("volume") if isinstance(snap.get("profile"), dict) else None
    hvn = [float(px) for px, _ in (vol_prof.get("hvn") or [])] if vol_prof else []
    lvn = [float(px) for px, _ in (vol_prof.get("lvn") or [])] if vol_prof else []
    vp = {
        "poc": vol_prof.get("poc") if vol_prof else None,
        "vah": vol_prof.get("vah") if vol_prof else None,
        "val": vol_prof.get("val") if vol_prof else None,
        "hvn": hvn,
        "lvn": lvn,
        "single_lines": [],
        "ts": snap.get("ts")
    }
    vol = snap.get("vol") or {}
    atr_pct = vol.get("atr_pct")

    return dict(
        side=side.upper(),
        mid=float(mid),
        best_bid=float(best_bid),
        best_ask=float(best_ask),
        base_entry=float(base_entry) if base_entry is not None else None,
        vp=vp,
        atr_pct=float(atr_pct) if atr_pct is not None else None,
    )

# =========================
# 日志/决策辅助（仅 MQ 场景）
# =========================
def _fmt_levels(arr: List[float]) -> str:
    return "[" + ", ".join(f"{x:.2f}" for x in arr) + "]"

def _read_ofi(evt: Dict[str, Any]) -> Optional[float]:
    # 支持多种结构：features.ofi.sum.w1s / ofi['1000'] / ofi.w1s / evt['ofi']
    ofi = None
    try:
        feats = (evt.get("features") or {})
        ofi_sum = (feats.get("ofi") or {}).get("sum") if isinstance(feats.get("ofi"), dict) else None
        if isinstance(ofi_sum, dict):
            ofi = ofi_sum.get("w1s") or ofi_sum.get("1000")
        if ofi is None:
            ofi = ((evt.get("ofi") or {}).get("w1s")
                   or (evt.get("ofi") or {}).get("1000")
                   or (evt.get("ofi")))
        if ofi is not None:
            return float(ofi)
    except Exception:
        pass
    return None

def _read_mp_delta_bps(evt: Dict[str, Any]) -> Optional[float]:
    try:
        feats = (evt.get("features") or {})
        mp = feats.get("microprice") or {}
        v = mp.get("delta_bps") or mp.get("deltaBps") or evt.get("microprice_delta_bps")
        if v is not None:
            return float(v)
    except Exception:
        pass
    return None

def _decide_block_reverse(side: str, ofi: Optional[float], vpin_toxic: bool, mp_delta_bps: Optional[float]) -> Tuple[bool, Optional[str]]:
    """
    简单启发式：
      - VPIN toxic 直接建议拦截
      - OFI 与下单方向强烈相反（|ofi|>=50）建议拦截
      - Microprice 偏离与方向强烈相反（|Δ|>=6bps）建议拦截
    留给 trade.py 最终裁决（这里只给出建议+日志）
    """
    sgn = +1 if side.upper()=="LONG" else -1
    if vpin_toxic:
        return True, "vpin_toxic"
    if ofi is not None and (ofi * sgn) < 0 and abs(ofi) >= 50.0:
        return True, "ofi_opposes_strong"
    if mp_delta_bps is not None and (mp_delta_bps * sgn) < 0 and abs(mp_delta_bps) >= 6.0:
        return True, "microprice_opposes"
    return False, None
