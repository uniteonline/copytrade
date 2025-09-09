import os
import yaml
import logging
import inspect
import math      
import time, random, functools
import re
import sys
import uuid
import pika
import threading
from decimal import Decimal, ROUND_DOWN, ROUND_UP, InvalidOperation
from math import isfinite
from typing import Dict, Any, DefaultDict, Tuple, Callable
from collections import defaultdict
from contextlib import suppress

# 新增：引入通用基础设施 Mixin
from core_infra import CoreInfraMixin
# -------------------------------------------------------------
# 屏蔽 python-okx SDK 打印的「HTTP Request: …」调试日志
# ＊要写在第一次 import okx.* 之前，否则已创建的 logger
#   级别不会被更新
# -------------------------------------------------------------
for noisy in ("okx", "httpx", "urllib3"):   # httpx / urllib3 视 SDK 版本而定
    logging.getLogger(noisy).setLevel(logging.WARNING)
# ===== 我们自己的日志级别（不改你的业务逻辑）=====
# 根 logger 至少 INFO，本模块 DEBUG，便于排查
root_logger = logging.getLogger()
if root_logger.level > logging.INFO:
    root_logger.setLevel(logging.INFO)
# 本模块更细：DEBUG
logging.getLogger(__name__).setLevel(logging.DEBUG)
# ================= 全局开关 =================
# "real"：真实下单；"test"：只打印最终参数，不发送订单
mode = "real"

import okx.Account as Account        # 账户与余额、杠杆设置接口
import okx.MarketData as MarketData  # 市场数据接口，包括交易对查询
import okx.PublicData  as PublicData 
from okx.Trade import TradeAPI
import json, pathlib
from risk_guardian import RiskGuardian, quick_eval_from_event
from utils import peek_features_snapshot,rpc_indicator_snapshot
# --- 新增：OPEN 幂等缓存（按币种/方向/规模+价格签名，短窗去重） ---

class OKXTradingClient(CoreInfraMixin):
    _px_state_lock = threading.RLock()
    OPEN_DEDUP_TTL = 8.0   # 秒；窗口内相同签名只处理一次
    _open_sig_cache: dict[tuple[str,str], dict] = {}
    def __init__(
        self,
        flag: str = "0",            # "0"=实盘；"1"=模拟
        margin_mode: str = "cross", # 全仓 cross；逐仓 isolated
        leverage: str = "100",      # 默认 100X（仅作兜底）
        pos_mode: str = "long_short",      # "net" | "long_short"
        leverage_mode: str = "max",        # max | normal | double | triple
    ):
        """
        初始化：从同目录下的 config.yaml 加载 OKX API 配置，
        并创建 Account、MarketData、Trade 三大 API 实例
        """
        # 1. 定位并读取 config.yaml
        config_path = os.path.join(os.path.dirname(__file__), "config.yaml")
        with open(config_path, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f)

        okx_cfg = cfg.get("okx", {})
        api_key     = okx_cfg.get("api_key")
        secret_key  = okx_cfg.get("secret_key")
        passphrase  = okx_cfg.get("passphrase")

        if not (api_key and secret_key and passphrase):
            raise ValueError("请在 config.yaml 中配置完整的 okx.api_key、secret_key 和 passphrase")
        self._api_key, self._secret_key, self._passphrase = api_key, secret_key, passphrase
        # 2) 保存模式参数（允许 config.yaml 覆盖）
        self.flag        = flag
        self.margin_mode = okx_cfg.get("margin_mode", margin_mode)
        self.leverage    = str(okx_cfg.get("leverage", leverage))
        _LEV_MODES = {"max", "normal", "double", "triple"}
        cfg_lev_mode = (okx_cfg.get("leverage_mode") or leverage_mode).lower()
        if cfg_lev_mode not in _LEV_MODES:
            raise ValueError(f"leverage_mode 必须为 {_LEV_MODES} 之一（got {cfg_lev_mode})")
        self.leverage_mode = cfg_lev_mode
        # 保存 flag 供 _rebuild_clients() 使用
        self.flag = flag
        # -------- local ↔︎ API 枚举映射 -------- #
        _LOCAL2API = {"net": "net_mode", "long_short": "long_short_mode"}
        _API2LOCAL = {v: k for k, v in _LOCAL2API.items()}

        pos_mode = (okx_cfg.get("pos_mode", pos_mode)).lower()
        if pos_mode not in _LOCAL2API:
            raise ValueError(f"pos_mode must be 'net' or 'long_short' (got {pos_mode})")
        self.pos_mode = pos_mode            # 本地语义
        # -------------------------------------------------------
        # 资金管理参数：从 config.yaml 的 risk 节读取，如无则用默认
        # -------------------------------------------------------
        risk_cfg = cfg.get("risk", {})
        self.RESERVE_RATIO    = float(risk_cfg.get("reserve_ratio",0.25))
        self.RESERVE_MIN_USDT = float(risk_cfg.get("reserve_min_usdt",25))
        self.BUDGET_RATIO     = float(risk_cfg.get("budget_ratio",0.25))
        # -------- 最大回撤（按 ROE 百分比）阈值（≤0 关闭） -------- #
        # 例如：40 → 触发于 ROE -40%
        self.MAX_DD_ROE_PCT = float(risk_cfg.get("max_drawdown_ratio", 40.0))

        # -------- 入场滑点（0 表示不用） -------- #
        # 支持在 config.yaml 根级或 risk 段里配置 entry_slip_ratio
        self.ENTRY_SLIP       = float(cfg.get("entry_slip_ratio",
                                    risk_cfg.get("entry_slip_ratio", 0)))
        # -------- 新增：收益保护（0 表示关闭） -------- #
        # profit_step_pct：每满多少 % 收益触发一次（如 80 = 80%）
        # profit_cut_pct ：触发减仓百分比（如 30 = 减 30%）
        self.PP_STEP_PCT      = float(risk_cfg.get("profit_step_pct", 0))
        self.PP_CUT_RATIO     = float(risk_cfg.get("profit_cut_pct", 0)) / 100.0
        # 触发容差：例如 1 表示达到目标阶梯的 99% 也触发
        self.PP_TOLERANCE_PCT = float(risk_cfg.get("profit_step_tolerance_pct", 0.0))
        # -------- 新增：止损保护（分档减仓，0 表示关闭） -------- #
        # loss_step_pct：每满多少 % 浮亏触发一次（如 10 = 每亏 10%）
        # loss_cut_pct ：触发减仓百分比（如 25 = 减 25%）
        self.LP_STEP_PCT      = float(risk_cfg.get("loss_step_pct", 0))
        self.LP_CUT_RATIO     = float(risk_cfg.get("loss_cut_pct", 0)) / 100.0
        self.LP_TOLERANCE_PCT = float(risk_cfg.get("loss_step_tolerance_pct", 0.0))
        # -------- 触发价格去重的相对容差（百分比，0=关闭） -------- #
        # 同一 instId/posSide 若 midPx 与上次触发价在此容差内，则视为“同一价格点”，不重复触发
        self.TRIGGER_PX_TOL_PCT = float(risk_cfg.get("trigger_px_tolerance_pct", 0.0))
        # 优化 9：tol% × f(level) → 线性/自定义因子
        self.PX_LVL_K = float(risk_cfg.get("trigger_px_level_k", 0.25))   # f(level)=1+K*(lvl-1)
        self.PX_LVL_FACTORS = list(map(float, risk_cfg.get("trigger_px_level_factors", [])))  # 可选：显式数组
        # ---- RiskGuardian：最小 ROE 距离（传实盘杠杆用来换算价格间距） ----
        self.RG_MIN_ROE_GAP_PCT = float(risk_cfg.get("min_roe_gap_pct", 25.0))
        # -------- 过滤币种：支持数组或逗号分隔字符串 -------- #
        raw_flt = cfg.get("filter_tokens", []) or []
        self.FILTER_TOKENS: set[str] = self._normalize_token_filter(raw_flt)
        logging.info("过滤币种源类型=%s → 生效：%s",
                     type(raw_flt).__name__, ",".join(sorted(self.FILTER_TOKENS)) or "(空)")
        logging.info("Leverage config → mode=%s, base=%sx, margin_mode=%s",
                     self.leverage_mode, self.leverage, self.margin_mode)
        # 3. 初始化各 API 客户端
        self.account_api = Account.AccountAPI(api_key, secret_key, passphrase, False, flag)
        self.market_api  = MarketData.MarketAPI(flag=flag)
        self.public_api  = PublicData.PublicAPI(flag=flag)
        self.trade_api   = TradeAPI(api_key, secret_key, passphrase, False, flag)

        # 3.5 先初始化通用基础设施（提供路径/缓存/原子落盘等能力）
        CoreInfraMixin.__init__(self, cfg)
        # 交易规格缓存（lotSz/minSz），10分钟 TTL
        self._inst_steps: dict[str, dict] = {}
        # —— MQ 消息去抖：同一 msg_id 在 TTL 内只处理一次 —— #
        self.MSG_TTL_SEC = float(cfg.get("mq_msg_ttl_sec", 8.0))  # 8 秒默认
        self._seen_msg: Dict[str, float] = {}
        # ===== 持仓权威同步：节流与时间戳 =====
        # 超过该秒数未同步，则在“钱包触发的实操”完成后做一次权威同步（含孤单）
        self.POSITION_SYNC_MAX_AGE_SEC = float(cfg.get("position_sync_max_age_sec", 60.0))
        self._last_pos_sync_ts: float = 0.0
        # 4. 本地缓存：订单 & 仓位
        self.orders: Dict[str, Dict[str, Any]] = {}
        self._bootstrap_open_orders()   # 未成交委托
        self._bootstrap_positions()     # 已持仓信息
        # —— 启动即做一次
        #    “权威对齐 → 计算偏差 → 基于 diff 重置风控/去重 → 覆盖缓存 → 最小自愈（仅降风险）” —— #
        try:
            if hasattr(self, "infra_fetch_authoritative_snapshot"):
                snap = self.infra_fetch_authoritative_snapshot(tries=2)
                diff = None
                if hasattr(self, "infra_compute_diff"):
                    diff = self.infra_compute_diff(snap)
                # 先用 diff 重置风控/去重，避免覆盖后丢失“权威突现的新仓位”判断依据
                if diff and hasattr(self, "_reset_guards_if_new_pos_from_authority"):
                    self._reset_guards_if_new_pos_from_authority(diff)
                # 覆盖本地缓存到权威状态
                if hasattr(self, "infra_override_local_caches"):
                    self.infra_override_local_caches(snap)
                # 仅做“降风险”的最小自愈
                # —— 覆盖成功后，记录持仓最近一次权威同步时间 —— #
                self._last_pos_sync_ts = time.time()
                logging.info("[POS_SYNC][BOOT] last_sync_ts=%.1f", self._last_pos_sync_ts)
                # 修复：第二次重启时，如存在 live/partially_filled 的挂单，不要缩减已挂单的持仓量
                # 因此在启动阶段检测到有效挂单则跳过最小自愈。
                if diff and hasattr(self, "_do_minimal_heal"):
                    try:
                        pending = []
                        try:
                            pending = [od for od in self.get_pending_orders()
                                       if (od.get("state") in {"live", "partially_filled"})]
                        except Exception:
                            pending = []
                        if pending:
                            logging.warning("[BOOT][AUTH_RESYNC][SKIP_HEAL] 检测到 %d 笔未成交挂单，跳过最小自愈以避免缩减挂单持仓量",
                                            len(pending))
                        else:
                            self._do_minimal_heal(diff)
                    except Exception as _e:
                        logging.warning("[BOOT][AUTH_RESYNC][HEAL_CHECK_FAIL] %s", _e)
        except Exception as e:
            logging.warning("[BOOT][AUTH_RESYNC][SKIP] %s", e)
        # 5. 运行时缓存：交易对 & 钱包端仓位价值
        self._wallet_value: DefaultDict[str, float] = defaultdict(float)  # coin -> abs(szi)*entryPx
        self._last_szi: Dict[str, float] = {}
        # 6. PP/LP 分档状态与价格去重持久化 —— 先有 CoreInfraMixin 的路径，再读取
        self._pp_hits: Dict[str, int] = self._load_profit_guard_state()
        self._lp_hits: Dict[str, int] = self._load_loss_guard_state()
        self._pp_last_px: Dict[str, float] = self._load_profit_guard_px_state()
        self._lp_last_px: Dict[str, float] = self._load_loss_guard_px_state()

        # —— 重启后确认分档/去重状态是否恢复 —— #
        logging.info("[LP][RESTORE] hits_keys=%d px_keys=%d",
                     len(self._lp_hits), len(self._lp_last_px))
        logging.debug("[LP][RESTORE][SAMPLE] %s",
                      dict(list(self._lp_hits.items())[:5]))
        # 特征缓存：保存上游推送或本地拉取的 ATR/VP/HVN/LVN 等特征（带轻量 TTL 清理）
        self._feat_cache: dict[str, dict] = {}
        # BBO 缓存：instId -> 最近一次完整 BBO/订单簿摘要
        self._bbo_cache: Dict[str, dict] = {}
        self._bbo_cache_ttl: float = float(cfg.get("book_cache_ttl_sec", 5.0))
        # 清理被过滤币种的历史缓存，避免继续参与权重/跟随
        for k in list(self._wallet_value.keys()):
            if str(k).upper() in self.FILTER_TOKENS:
                self._wallet_value.pop(k, None)
        for k in list(self._last_szi.keys()):
            if str(k).upper() in self.FILTER_TOKENS:
                self._last_szi.pop(k, None)


        # ─── 波动/ATR 配置（改为走 MQ 提供的 vol.atr_pct） ─── #
        self._vol_cfg = cfg.get("volatility", {}) or {}
        self._k_atr   = float(self._vol_cfg.get("trigger_px_k_atr", 0.0) or 0.0)
        self._pp_k    = float(self._vol_cfg.get("pp_atr_k", 3.0) or 3.0)
        self._pp_min  = float(self._vol_cfg.get("pp_step_min_roe", 45.0) or 45.0)
        self._pp_max  = float(self._vol_cfg.get("pp_step_max_roe", 140.0) or 140.0)
        self._vol_ttl = float(self._vol_cfg.get("cache_ttl_sec", 20.0) or 20.0)
   
        # —— ATR 动态亏损保护（LP）参数（未监控交易对时启用） —— #
        self._lp_k   = float(self._vol_cfg.get("lp_atr_k", 2.0) or 2.0)
        self._lp_min = float(self._vol_cfg.get("lp_step_min_roe", 35.0) or 35.0)
        self._lp_max = float(self._vol_cfg.get("lp_step_max_roe", 80.0) or 80.0)

    
        # 优化 4：组合维度“波动率配比”预算分配（risk-parity 近似）
        self.BUDGET_TARGET_ATR = float(self._vol_cfg.get("budget_target_atr_pct", 3.0))
        self.BUDGET_VOL_MIN    = float(self._vol_cfg.get("budget_vol_scale_min", 0.5))
        self.BUDGET_VOL_MAX    = float(self._vol_cfg.get("budget_vol_scale_max", 2.0))
        # ─── 急跌/急涨入场价保护（Crash-Adaptive Entry Protect） ─── #
        _ep = (cfg.get("entry_protect") or {})  # 缺省启用，默认参数见下
        self.EP_ENABLE            = bool(_ep.get("enable", True))
        self.EP_WINDOW_SEC        = int(_ep.get("window_sec", 480))          # 回看窗口：8 分钟
        self.EP_BAR               = str(_ep.get("bar", "1m"))                # K 线粒度
        self.EP_TRIGGER_K_ATR     = float(_ep.get("trigger_drop_k_atr", 2.5))# 触发阈值 = K×ATR%
        self.EP_SATURATE_K_ATR    = float(_ep.get("saturate_drop_k_atr", 5.0))# 饱和值 = K×ATR%
        self.EP_MAX_EXTRA_PCT     = float(_ep.get("max_extra_shift_pct", 1.6))# 基线最多再偏移 %
        self.EP_ANCHOR_BAND_K_ATR = float(_ep.get("anchor_band_k_atr", 0.2)) # 极值外保护带 = K×ATR%
        self.EP_BLEND_GAMMA       = float(_ep.get("blend_gamma", 1.4))       # 严重度凸性
        # 优化 2：PP/LP 递进 cut 序列（百分比小数，回退到单一比例）
        _pp_seq = list(map(float, risk_cfg.get("pp_cuts", [])))
        _lp_seq = list(map(float, risk_cfg.get("lp_cuts", [])))
        self.PP_CUTS = (_pp_seq if _pp_seq else ([self.PP_CUT_RATIO] if self.PP_CUT_RATIO>0 else []))[:10]
        self.LP_CUTS = (_lp_seq if _lp_seq else ([self.LP_CUT_RATIO] if self.LP_CUT_RATIO>0 else []))[:10]

        # 优化 6：触发冷却 + 双阈值滞回
        _hy = (cfg.get("hysteresis") or risk_cfg or {})
        self.TRIGGER_COOLDOWN_SEC = float(_hy.get("trigger_cooldown_sec", 5.0))
        self.HYS_BAND_PCT         = float(_hy.get("hysteresis_band_pct", 10.0))  # 低阈 = 高阈*(1-此比例)
        self._cooldown_ts: dict[str, float] = {}
        self._hys_state_pp: dict[str, str] = {}  # "armed"/"disarmed"
        self._hys_state_lp: dict[str, str] = {}

        # 优化 1：ROE 高水位拖尾止盈（MFE）
        self.MFE_START_PCT   = float(risk_cfg.get("mfe_trail_start_pct", 60.0))
        self.MFE_BACKOFF_PCT = float(risk_cfg.get("mfe_trail_backoff_pct", 20.0))
        self.MFE_CUTS        = list(map(float, risk_cfg.get("mfe_trail_cuts", [0.25, 0.35, 0.40])))[:10]
        self._mfe_peak: dict[str, float] = {}
        self._mfe_hits: dict[str, int] = {}
        # MFE A+B：A) ROE 去重；B) 新峰值再武装
        self.MFE_ROE_TOL_PCT      = float(risk_cfg.get("mfe_roe_tol_pct", 3.0))     # A：同位置容差（百分比点）
        self.MFE_REARM_DELTA_PCT  = float(risk_cfg.get("mfe_rearm_delta_pct", 2.0))  # B：再武装需要的新峰值增量
        self._mfe_last_roe: dict[str, float] = {}
        self._mfe_gated: dict[str, bool] = {}               # 触发后置 True，直到出现“新峰值≥last_peak+ε”
        self._mfe_last_peak_trig: dict[str, float] = {}     # 上一次触发时的峰值（用于 B 方案再武装）
        # ─── 引入 Volume Profile 风控助手 ─── #
        self._rg = RiskGuardian()
        # 缓存由 RG 计算出的入场修正与 SL/节点（按 instId+posSide 维度）
        # 结构：{"entry_adj":..., "sl":[...], "tp_nodes":[...], "sl_nodes":[...], "guards":{"profit_protect_ref":..., "loss_protect_ref":...}}
        self._vp_levels: dict[str, dict] = {}
        # ===== 新增：多时段节点（HVN/LVN）触发计数 =====
        self._node_tp_hits: dict[str, int] = {}   # HVN 盈利保护触发层级
        self._node_sl_hits: dict[str, int] = {}   # LVN 亏损保护触发层级
        # VP 分级减仓比例（可在 config.yaml.risk.vp_tp_cuts / vp_sl_cuts 覆盖）
        vp_cfg = (cfg.get("risk", {}) or {})
        # 允许最多 5 档
        self.VP_TP_CUTS = list(map(float, vp_cfg.get("vp_tp_cuts", [0.25, 0.35, 0.40])))[:5]
        self.VP_SL_CUTS = list(map(float, vp_cfg.get("vp_sl_cuts", [0.33, 0.50, 1.00])))[:5]
        # 节点侧分级减仓比例：
        # - 盈利(HVN) 默认走 lp_cuts（与你的约定一致）
        # - 亏损(LVN) 默认走 PP_CUTS
        self.NODE_TP_CUTS = list(map(float, vp_cfg.get("node_tp_cuts", self.LP_CUTS)))[:5]
        self.NODE_SL_CUTS = list(map(float, vp_cfg.get("node_sl_cuts", self.PP_CUTS)))[:5]
        # 优化 8：节点触发“保守确认”带宽（阈值需越过 HVN/LVN 一定带宽才视为命中）
        self.NODE_CONFIRM_K_ATR  = float(vp_cfg.get("node_confirm_k_atr", 0.5))  # × ATR%
        self.NODE_CONFIRM_MIN_PCT = float(vp_cfg.get("node_confirm_min_pct", 0.2))  # 最小额外百分比
        # 若 HVN/LVN 节点为空，是否回退到 ATR 分档保护（默认 True，可在 risk.use_atr_fallback_when_no_nodes 配置）
        self.USE_ATR_FALLBACK = bool(vp_cfg.get("use_atr_fallback_when_no_nodes", True))
        # === 新增：阶梯缺口合成（真实节点不足时，按 ROE 阶梯自动补点） ===
        self.NODE_SYNTH_ENABLE      = bool(vp_cfg.get("node_synth_enable", True))
        self.NODE_SYNTH_TOL_PCT     = float(vp_cfg.get("node_synth_tolerance_pct", 5.0))   # 允许差 <5pp 视为达到目标档
        self.NODE_SYNTH_MAX_LEVELS  = int(vp_cfg.get("node_synth_max_levels", 5))          # 最多 5 档
        self.NODE_SYNTH_CUT_RATIO   = float(vp_cfg.get("node_synth_cut_pct", 30.0)) / 100.0 # “补档”默认 30% 减仓

        # 入场滑点：改为优先从 risk.entry_slip 读取；为兼容老配置，根级 entry_slip 作回退
        _slip_cfg = (risk_cfg.get("entry_slip") or cfg.get("entry_slip") or {})
        self.SLIP_MODE       = str(_slip_cfg.get("mode", "atr")).lower()

        self.SLIP_ATR_K      = float(_slip_cfg.get("atr_k", 3.0))
        self.SLIP_MIN_PCT    = float(_slip_cfg.get("min_pct", 0.0002))  # ← 漏掉的关键一行
        self.SLIP_MAX_PCT    = float(_slip_cfg.get("max_pct", 0.10))    # ← 删掉重复/错误的第二次赋值
        self.SLIP_SPREAD_MULT= float(_slip_cfg.get("spread_mult", 0.0))
        self.SLIP_TICK_MULT  = float(_slip_cfg.get("tick_mult", 0.0))
        self.SLIP_ABS_FLOOR  = float(_slip_cfg.get("abs_floor_usdt", 0.0))
        self.SLIP_ATR_BOOTSTRAP_PCT = float(_slip_cfg.get("atr_bootstrap_pct", 0.0021))
        self.ENTRY_STRICT_ATR_ONLY  = True

        # ===== 跟随加仓：浮亏且对方加仓后，它的入场优于我 → 临时缩小滑点 =====
        # 说明：只影响 SIZE-INC（加仓）分支，OPEN/REVERSE 不动
        _fol = (cfg.get("follow") or {})
        self.FOLLOW_ENABLE           = bool(_fol.get("enable", True))
        # 允许从配置控制加仓的全局封顶比例（你在 INC 分支里已有使用 getattr(self, "INC_PCT_CAP", 1.0)）
        self.INC_PCT_CAP             = float((cfg.get("risk", {}) or {}).get("inc_pct_cap", 1.0))
        # ===== 新增：DEC 累计器（按 instId+posSide 维度缓存“待减张数”） =====
        # 设计目标：
        # 1) 多条小于 minSz 的减仓请求先累计；达到最小可下单量后再一次性下单；
        # 2) 最小持仓保护：保留 minSz（如 1 张或 0.1 张），不把仓位打穿；
        # 3) 由下一条 MQ 触发 flush（无需后台轮询）。
        self._dec_accum: Dict[tuple[str, str], Decimal] = {}
        self.DEC_MIN_HOLD_KEEP = True  # 置 True 表示启用“最小持仓量保留”
    # 规范化 filter_tokens（list / set / tuple / comma string / dict{TOKEN:bool}）
    def _normalize_token_filter(self, raw) -> set[str]:
        toks: set[str] = set()
        try:
            if isinstance(raw, str):
                parts = re.split(r"[,\s]+", raw)
                toks = {p.strip().upper() for p in parts if p.strip()}
            elif isinstance(raw, (list, tuple, set)):
                toks = {str(p).strip().upper() for p in raw if str(p).strip()}
            elif isinstance(raw, dict):
                toks = {str(k).strip().upper() for k, v in raw.items() if v and str(k).strip()}
        except Exception:
            pass
        return toks
    # ============================================================
    #  收益保护：加载 / 保存 / key 生成
    # ============================================================
    def _load_profit_guard_state(self) -> Dict[str, int]:
        if not getattr(self, "_pp_state_path", None) or not self._pp_state_path.exists():
            return {}
        try:
            with open(self._pp_state_path, "r", encoding="utf-8") as fp:
                raw = json.load(fp)
                # 统一成 {str:int}
                return {str(k): int(v) for k, v in raw.items()}
        except Exception:
            return {}

    def _save_profit_guard_state(self) -> None:
        try:
            self._atomic_json_dump(self._pp_state_path, self._pp_hits)
        except Exception as e:
            logging.warning("写入 profit_guard_state 失败: %s", e)

    @staticmethod
    def _pp_key(inst_id: str, pos_side: str) -> str:
        # pos_side: "long" / "short"
        return f"{inst_id}:{pos_side}"
    # ============================================================
    #  止损保护：加载 / 保存
    # ============================================================
    def _load_loss_guard_state(self) -> Dict[str, int]:
        if not getattr(self, "_lp_state_path", None) or not self._lp_state_path.exists():
            return {}
        try:
            with open(self._lp_state_path, "r", encoding="utf-8") as fp:
                raw = json.load(fp)
                return {str(k): int(v) for k, v in raw.items()}
        except Exception:
            return {}

    def _save_loss_guard_state(self) -> None:
        try:
            self._atomic_json_dump(self._lp_state_path, self._lp_hits)
        except Exception as e:
            logging.warning("写入 loss_guard_state 失败: %s", e)
    # ===================== 价格去重状态（加载/保存） =====================
    def _load_profit_guard_px_state(self) -> Dict[str, float]:
        try:
            if not getattr(self, "_pp_px_path", None) or not self._pp_px_path.exists():
                return {}
            with self._px_state_lock:
                with open(self._pp_px_path, "r", encoding="utf-8") as fp:
                    return {str(k): float(v) for k, v in json.load(fp).items()}
        except Exception:
            return {}

    def _save_profit_guard_px_state(self) -> None:
        try:
            # 写锁 + 合并落盘：降低并发覆盖概率（新值覆盖旧值）
            with self._px_state_lock:
                base = {}
                try:
                    if getattr(self, "_pp_px_path", None) and self._pp_px_path.exists():
                        with open(self._pp_px_path, "r", encoding="utf-8") as fp:
                            base = {str(k): float(v) for k, v in json.load(fp).items()}
                except Exception:
                    base = {}
                merged = {**base, **self._pp_last_px}
                self._atomic_json_dump(self._pp_px_path, merged)
        except Exception as e:
            logging.warning("写入 profit_guard_px 失败: %s", e)
    # --------- 更稳健的杠杆解析（容忍 "50x" / "100.0" / 空值） --------- #
    def _parse_lev(self, raw) -> int | None:
        if raw in (None, "", 0, "0"):
            return None
        try:
            return int(float(raw))
        except Exception:
            m = re.search(r"\d+(\.\d+)?", str(raw))
            try:
                return int(float(m.group())) if m else None
            except Exception:
                return None
    def _load_loss_guard_px_state(self) -> Dict[str, float]:
        try:
            # 属性存在性判断 + 读锁，避免初始化早期或并发读导致异常
            if not getattr(self, "_lp_px_path", None) or not self._lp_px_path.exists():
                return {}
            with self._px_state_lock:
                with open(self._lp_px_path, "r", encoding="utf-8") as fp:
                    return {str(k): float(v) for k, v in json.load(fp).items()}
        except Exception:
            return {}

    def _save_loss_guard_px_state(self) -> None:
        try:
            # 写锁 + 合并落盘：降低并发覆盖概率（新值覆盖旧值）
            with self._px_state_lock:
                base = {}
                try:
                    if getattr(self, "_lp_px_path", None) and self._lp_px_path.exists():
                        with open(self._lp_px_path, "r", encoding="utf-8") as fp:
                            base = {str(k): float(v) for k, v in json.load(fp).items()}
                except Exception:
                    base = {}
                merged = {**base, **self._lp_last_px}
                self._atomic_json_dump(self._lp_px_path, merged)
        except Exception as e:
            logging.warning("写入 loss_guard_px 失败: %s", e)
    # ============================================================
    #  工具：判断是否为被过滤的币种
    # ============================================================
    def _is_filtered_coin(self, coin: str) -> bool:
        return coin.upper() in getattr(self, "FILTER_TOKENS", set())
    # ===== MQ 去抖工具 =====
    def _seen_and_mark_msg(self, msg_id: str) -> bool:
        if not msg_id:
            return False
        now = time.time()
        ts = self._seen_msg.get(msg_id)
        if ts is not None and (now - ts) < max(0.0, float(self.MSG_TTL_SEC)):
            return True
        self._seen_msg[msg_id] = now
        # 轻量 GC：超过 20k 条时顺手清一把过期的
        if len(self._seen_msg) > 20000:
            ttl = float(self.MSG_TTL_SEC)
            cutoff = now - ttl
            self._seen_msg = {k: v for k, v in self._seen_msg.items() if v >= cutoff}
        return False
    # ============================================================
    #  负缓存：避免对不存在合约的币频繁打 API
    # ============================================================
    def _skip_miss_coin(self, coin: str) -> bool:
        ts = self._pair_miss.get(coin.upper())
        return (ts is not None) and (time.time() - ts < self.MISS_TTL)

    def _note_miss_coin(self, coin: str) -> None:
        coin = coin.upper()
        self._pair_miss[coin] = time.time()
        # 日志限流：每 5 分钟打一条提示
        last = self._miss_log_ts.get(coin, 0.0)
        if time.time() - last > 300:
            logging.info("未找到 %s 对应的 SWAP 交易对（将在 %ds 内不再查询）",
                         coin, int(self.MISS_TTL))
            self._miss_log_ts[coin] = time.time()
    def _in_warmup(self) -> bool: return (time.time() - self._start_ts) < self.WARMUP_SEC
 
    def _protected_entry_price(self, inst_id: str, side_flag: str,
                               baseline_px: float, last_px: float, slip: float) -> float:
        """
        急跌/急涨场景：基于最近窗口极值与 ATR%，把入场价从 baseline 再向极值侧“推一截”，
        并按严重度增加偏移，但不超过 max_extra_shift_pct 的上限。
        side_flag: "LONG" / "SHORT"
        """
        try:
            if (not self.EP_ENABLE) or baseline_px <= 0 or last_px <= 0:
                return float(baseline_px)
            # 钳制关键参数，避免配置异常
            mx = max(0.0, float(self.EP_MAX_EXTRA_PCT or 0.0))
            gamma = max(1.0, float(self.EP_BLEND_GAMMA or 1.0))
            st = self._get_atr_state(inst_id)
            atr_pct = float((st or {}).get("atr_pct") or 0.0)
            hi, lo = self._recent_high_low(inst_id, self.EP_WINDOW_SEC, self.EP_BAR)
            if not hi or not lo:
                logging.info("[EP][SKIP_NO_HILO] inst=%s side=%s base=%.8f last=%.8f atr%%=%.5f",
                             inst_id, side_flag, float(baseline_px), float(last_px), atr_pct)
                return float(baseline_px)

            if side_flag == "LONG":
                drop_pct = max(0.0, (hi - last_px) / hi * 100.0)   # 距最高点回撤百分比
            else:  # SHORT
                drop_pct = max(0.0, (last_px - lo) / lo * 100.0)   # 距最低点反弹百分比

            trigger  = (self.EP_TRIGGER_K_ATR  * atr_pct) if atr_pct > 0 else 3.0
            saturate = (self.EP_SATURATE_K_ATR * atr_pct) if atr_pct > 0 else 6.0
            if drop_pct <= trigger:
                logging.info("[EP][NO_TRIGGER] inst=%s side=%s base=%.8f last=%.8f hi=%.8f lo=%.8f drop%%=%.5f thr=%.5f",
                             inst_id, side_flag, float(baseline_px), float(last_px),
                             float(hi), float(lo), drop_pct, trigger)
                return float(baseline_px)
            if saturate <= trigger: 
                saturate = trigger + 1e-6
            sev = min(1.0, max(0.0, (drop_pct - trigger) / (saturate - trigger)))
            w   = sev ** gamma
            band = (self.EP_ANCHOR_BAND_K_ATR * atr_pct) / 100.0 if atr_pct > 0 else 0.0

            if side_flag == "LONG":
                anchor = float(lo) * (1.0 - max(0.0, band))
                anchor_slipped = anchor * (1.0 - abs(float(slip or 0.0)))
                extra = mx * w / 100.0
                floor_long = float(baseline_px) * (1.0 - extra)
                candidate  = max(anchor_slipped, floor_long)
                out_px = float(min(baseline_px, candidate))
                logging.info(
                    "[EP][APPLY] inst=%s side=LONG base=%.8f last=%.8f hi=%.8f lo=%.8f atr%%=%.5f "
                    "drop%%=%.5f trig=%.5f sat=%.5f sev=%.5f w=%.5f band=%.5f "
                    "anchor=%.8f anchor_slipped=%.8f extra_max%%=%.5f floor=%.8f "
                    "candidate=%.8f → out=%.8f",
                    inst_id, float(baseline_px), float(last_px), float(hi), float(lo), atr_pct,
                    drop_pct, trigger, saturate, sev, w, band*100.0,
                    anchor, anchor_slipped, mx, floor_long, candidate, out_px
                )
                return out_px
            else:
                anchor = float(hi) * (1.0 + max(0.0, band))
                anchor_slipped = anchor * (1.0 + abs(float(slip or 0.0)))
                extra = mx * w / 100.0
                ceil_short = float(baseline_px) * (1.0 + extra)
                candidate  = min(anchor_slipped, ceil_short)
                out_px = float(max(baseline_px, candidate))
                logging.info(
                    "[EP][APPLY] inst=%s side=SHORT base=%.8f last=%.8f hi=%.8f lo=%.8f atr%%=%.5f "
                    "rebound%%=%.5f trig=%.5f sat=%.5f sev=%.5f w=%.5f band=%.5f "
                    "anchor=%.8f anchor_slipped=%.8f extra_max%%=%.5f ceil=%.8f "
                    "candidate=%.8f → out=%.8f",
                    inst_id, float(baseline_px), float(last_px), float(hi), float(lo), atr_pct,
                    drop_pct, trigger, saturate, sev, w, band*100.0,
                    anchor, anchor_slipped, mx, ceil_short, candidate, out_px
                )
                return out_px
        except Exception:
            return float(baseline_px)
    # 优化 9：按分档 level 放大/缩小“价格去重”容差
    def _lvl_factor(self, lvl: int) -> float:
        try:
            if self.PX_LVL_FACTORS:
                i = max(0, min(int(lvl)-1, len(self.PX_LVL_FACTORS)-1))
                f = float(self.PX_LVL_FACTORS[i])
                return f if isfinite(f) and f>0 else 1.0
        except Exception:
            pass
        return 1.0 + max(0.0, float(self.PX_LVL_K)) * max(0, int(lvl)-1)

    # 优化 6：通用冷却
    def _under_cooldown(self, key: str, now: float | None = None) -> bool:
        now = time.time() if now is None else now
        last = float(self._cooldown_ts.get(key, 0.0) or 0.0)
        return (now - last) < max(0.0, float(self.TRIGGER_COOLDOWN_SEC))
    def _mark_cooldown(self, key: str) -> None:
        self._cooldown_ts[key] = time.time()

    def _pp_step_threshold(self, lev: float, need_lvl: int, inst_id: str) -> float | None:
        """
        动态阶梯（返回 ROE%）= clamp(lev × ATR% × k, pp_min, pp_max) × need_lvl
        无 ATR 时返回 None（上层回退静态参数）。
        """
        st = self._get_atr_state(inst_id)
        if not st:
            return None
        atr_pct = float(st.get("atr_pct") or 0.0)
        if atr_pct <= 0:
            return None
        step = max(self._pp_min, min(self._pp_max, float(lev) * atr_pct * self._pp_k))
        return step * max(1, int(need_lvl))

    def _is_monitored(self, inst_id: str | None = None, coin: str | None = None) -> bool:
        """
        返回该交易对是否处于生产端实时监控中。
        规则：
          - 若该币在 miss TTL（API 证实本交易所没有此 SWAP），直接判定未监控；
          - 其次看本地监控标记；
          - 默认 False（未监控）。
        """
        if coin and self._skip_miss_coin(coin):
            return False
        if inst_id is not None and inst_id in self._monitored_inst:
            return bool(self._monitored_inst[inst_id])
        if coin:
            c = coin.upper()
            if c in self._monitored_coin:
                return bool(self._monitored_coin[c])
        return False
    # ============================================================
    #  多时段节点筛选：保证“≥min_gap_pct 的 ROE 距离”（含当前杠杆）
    #  direction: "tp" (盈利方向) | "sl" (亏损方向)
    #  pos_side : "long" | "short"
    # ============================================================
    def _select_nodes_with_min_roe_gap(
        self,
        nodes: list[float],
        entry_px: float,
        pos_side: str,
        direction: str,
        lev: float,
        min_gap_pct: float
    ) -> list[float]:
        try:
            lev = float(lev or 1.0)
            entry_px = float(entry_px or 0.0)
            mg = float(min_gap_pct or 0.0)
        except Exception:
            return []
        if not nodes or entry_px <= 0 or lev <= 0:
            return []
        # 安全转换：忽略无法转成 float 的元素，避免 ValueError
        vals: list[float] = []
        for x in (nodes or []):
            try:
                vals.append(float(x))
            except (TypeError, ValueError):
                continue
        selected: list[float] = []
        # 方向与排序（从入场价往“预期方向”一步步筛）
        if direction == "tp":  # 盈利方向
            if pos_side == "long":
                cands = sorted([v for v in vals if v > entry_px])
                last = entry_px
                for v in cands:
                    roe = ((v - last) / last) * 100.0 * lev
                    if roe >= mg:
                        selected.append(v)
                        last = v
            else:  # short 盈利向下
                cands = sorted([v for v in vals if v < entry_px], reverse=True)
                last = entry_px
                for v in cands:
                    roe = ((last - v) / last) * 100.0 * lev
                    if roe >= mg:
                        selected.append(v)
                        last = v
        else:  # "sl" 亏损方向
            if pos_side == "long":  # 亏损向下
                cands = sorted([v for v in vals if v < entry_px], reverse=True)
                last = entry_px
                for v in cands:
                    roe = ((last - v) / last) * 100.0 * lev
                    if roe >= mg:
                        selected.append(v)
                        last = v
            else:  # short 亏损向上
                cands = sorted([v for v in vals if v > entry_px])
                last = entry_px
                for v in cands:
                    roe = ((v - last) / last) * 100.0 * lev
                    if roe >= mg:
                        selected.append(v)
                        last = v
        return selected
    def _clear_vp_state(self, inst_id: str) -> None:
        """关闭 VP/HVN/LVN 时，清理相关状态，避免误触发。"""
        for side in ("long", "short"):
            key = f"{inst_id}:{side}"
            self._vp_levels.pop(key, None)
            self._node_tp_hits.pop(key, None)
            self._node_sl_hits.pop(key, None)

    def _lp_step_threshold(self, lev: float, need_lvl: int, inst_id: str) -> float | None:
        """
        ATR 动态亏损分档阈值（单位：ROE%）= clamp(lev × ATR% × lp_k, lp_min, lp_max) × need_lvl
        无 ATR 时返回 None。
        """
        st = self._get_atr_state(inst_id)
        if not st:
            return None
        atr_pct = float(st.get("atr_pct") or 0.0)
        if atr_pct <= 0:
            return None
        step = max(self._lp_min, min(self._lp_max, float(lev) * atr_pct * self._lp_k))
        return step * max(1, int(need_lvl))

    def parse_wallet_event(self, evt: dict):
        """
        根据消息类型解析 wallet 事件，框架仅含分支，后续填充分支逻辑
        """
        # 统一入口总览（观测到底来了什么事件） hjj
        try:
            etype   = evt.get("etype")
            event   = evt.get("event")
            coin    = (evt.get("coin") or "").upper()
            side    = (evt.get("side") or "-").upper()
            midPx   = evt.get("midPx") or evt.get("last") or evt.get("px")
            has_atr = isinstance(evt.get("vol"), dict) and ("atr_pct" in evt["vol"])
            # logging.info("[EVT_IN] etype=%s event=%s coin=%s side=%s midPx=%s hasATR=%s msg_id=%s",
            #              etype, event, coin, side, str(midPx), has_atr, evt.get("msg_id"))
        except Exception:
            pass
        # —— 去抖：相同 msg_id 在 TTL 内只处理一次 —— #
        try:
            _mid = evt.get("msg_id") or evt.get("id") or evt.get("uid")
            if self._seen_and_mark_msg(_mid):
                logging.debug("[DEDUP][SKIP] msg_id=%s", _mid)
                return
        except Exception:
            pass
        # 入口诊断：确认 MQ 事件抵达与关键字段

        # 若配置了过滤币种，则这些币种的所有事件一律忽略
        coin_for_filter = (evt.get("coin") or evt.get("asset") or "").upper()
        if coin_for_filter and self._is_filtered_coin(coin_for_filter):
            # logging.info("[FILTER][DROP] coin=%s etype=%s event=%s",
            #              coin_for_filter, evt.get("etype"), evt.get("event"))
            return        
        # 若该币在 miss TTL（本交易所没有该 SWAP），丢弃除 MONITOR_STATUS 以外的事件
        if coin_for_filter and self._skip_miss_coin(coin_for_filter):
            etype = evt.get("etype")
            event = evt.get("event")
            with suppress(Exception):
                self._maybe_update_px_cache_from_evt(evt, evt.get("instId"))
            if event != "MONITOR_STATUS":
                logging.info("[MISS][DROP] coin=%s no SWAP on OKX (TTL) — skip %s/%s",
                             coin_for_filter, etype, event)
                return
            # 允许 MONITOR_STATUS 继续，以便把监控状态强制改为 false

        etype = evt.get("etype")
        event = evt.get("event")
        
        # === 新增：生产端监控状态 ===
        if event == "MONITOR_STATUS":
            self._handle_monitor_status(evt)
            return
        # 事件若携带 vol.atr_pct，先更新波动缓存（需 instId，稍后各 handler 内也会再次更新）
        try:
            if isinstance(evt.get("vol"), dict) and "atr_pct" in evt["vol"]:
                inst_id_try = evt.get("instId") or None
                if inst_id_try:
                    self._vol_cache[inst_id_try] = {"atr_pct": float(evt["vol"]["atr_pct"]), "ts": time.time()}
        except Exception:
            pass

        # 1. FILL 系列：OPEN / CLOSE
        if etype == "FILL":
            if event == "OPEN":
                # logging.info(
                #     "[PARSE_EVT] etype=%s event=%s side=%s keys=%s raw=%s",
                #     etype, event, evt.get("side"), list(evt.keys()), evt
                # )
                self._handle_fill_open(evt)
            elif event == "CLOSE":
                logging.info(
                     "[PARSE_CLOSE_EVT] raw=%s", evt
                )
                self._handle_fill_close(evt)

        # 2–5. SIZE 系列：OPEN / INC / DEC / REVERSE
        elif etype == "SIZE":
            if event == "OPEN":          # ← 新增：SIZE+OPEN 也当作开仓事件
                logging.info(
                     "[PARSE_SIZE_OPEN_EVT] raw=%s", evt
                )
                self._handle_fill_open(evt)
            elif event == "INC":
                logging.info(
                     "[PARSE_SIZE_INC_EVT] raw=%s", evt
                )
                self._handle_size_inc(evt)
            elif event == "DEC":
                logging.info(
                     "[PARSE_SIZE_DEC_EVT] raw=%s", evt
                )
                self._handle_size_dec(evt)
            elif event == "REVERSE":
                logging.info(
                     "[PARSE_SIZE_REVERSE_EVT] raw=%s", evt
                )
                self._handle_size_reverse(evt)

        # 6. INFO 系列：其他信息变化
        elif etype == "INFO":
            self._handle_info(evt)
        elif etype == "HEALTH":
            pass
        else:
            logging.info("event是none的信息=%s", evt)

    # ---------- 新增：FILL-OPEN 下单逻辑 ----------
    def _handle_fill_open(self, evt: dict):
        """
        MQ 事件 etype=="FILL" 且 event=="OPEN" 时触发本方法：
        1. 计算钱包该 coin 相对全仓价值占比
        2. 若本账户无该币种仓位，则按占比 * BUDGET_RATIO 的可支配余额开仓
        """
        positions = self.get_positions()  # 单次复用
        coin   = evt.get("coin", "").upper()
        szi    = float(evt.get("snapshot", {}).get("szi", 0) or 0)
        entry  = float(evt.get("snapshot", {}).get("entryPx", evt.get("px", 0)) or 0)
        side_flag = (evt.get("side") or "-").upper()  # 先取方向，供去重
        # ── 幂等：短时间窗口内，完全相同的 OPEN（币/方向/绝对仓位/入场价）只处理一次 ──
        if side_flag in ("LONG", "SHORT") and szi != 0 and entry > 0:
            key = (coin, side_flag)
            sig = (round(abs(szi), 8), round(entry, 8))
            prev = self._open_sig_cache.get(key)
            now  = time.time()
            if prev and prev.get("sig") == sig and (now - prev.get("ts", 0)) < self.OPEN_DEDUP_TTL:
                logging.warning(
                    "[OPEN_EVT][DEDUP] skip duplicate within %.1fs: coin=%s side=%s sig=%s evt_ts=%s msg_id=%s",
                    self.OPEN_DEDUP_TTL, coin, side_flag, sig, evt.get("ts"), evt.get("msg_id")
                )
                return
            self._open_sig_cache[key] = {"sig": sig, "ts": now}
            # 轻量 GC
            for k in list(self._open_sig_cache.keys()):
                if (now - self._open_sig_cache[k]["ts"]) >= self.OPEN_DEDUP_TTL:
                    self._open_sig_cache.pop(k, None)

        logging.info(
            "[OPEN_EVT] coin=%s side=%s szi=%s entryPx=%s msg_id=%s ts=%s",
            coin, evt.get("side"), szi, entry, evt.get("msg_id"), evt.get("ts")
        )

        if szi == 0 or entry == 0:
            logging.warning("OPEN event 缺少 szi/entryPx，跳过")
            return

        # 记录主钱包该币最新 szi，便于后续 INC/DEC 按比例跟随
        self._last_szi[coin] = abs(szi)
        # 2) 获取 / 缓存 instId（带负缓存）
        inst_id = self._get_inst_id(coin)
        if not inst_id:
            return
        # 先摄取事件自带的特征到缓存，再把缓存补并回事件（ATR / 多时段 VP / HVN/LVN）
        with suppress(Exception):
            self._ingest_features_from_evt(inst_id, evt)
        with suppress(Exception):
            self._merge_feat_into_event(inst_id, evt)
        # 更新本合约的 ATR%（如 MQ 带有 vol.atr_pct）
        try:
            if isinstance(evt.get("vol"), dict) and "atr_pct" in evt["vol"]:
                self._vol_cache[inst_id] = {"atr_pct": float(evt["vol"]["atr_pct"]), "ts": time.time()}
        except Exception:
            pass
        # 开新仓即重置收益/止损保护阶梯

        if side_flag in ("LONG","SHORT"):
            pos_side = "long" if side_flag=="LONG" else "short"
            self._pp_hits[self._pp_key(inst_id, pos_side)] = 0
            self._save_profit_guard_state()
            self._lp_hits[self._pp_key(inst_id, pos_side)] = 0
            self._save_loss_guard_state()
            # 新开仓：清理该方向的“价格去重”点，避免沿用上一轮的触发价
            self._pp_last_px.pop(self._pp_key(inst_id, pos_side), None)
            self._save_profit_guard_px_state()
            self._lp_last_px.pop(self._pp_key(inst_id, pos_side), None)
            self._save_loss_guard_px_state()
            # MFE/滞回/冷却状态重置
            mfe_key = f"{inst_id}:{pos_side}"
            self._mfe_peak.pop(mfe_key, None)
            self._mfe_hits.pop(mfe_key, None)
            self._hys_state_pp.pop(mfe_key, None)
            self._hys_state_lp.pop(mfe_key, None)
            self._cooldown_ts.pop(mfe_key, None)
            # MFE A+B 状态重置
            self._mfe_last_roe.pop(mfe_key, None)
            self._mfe_gated.pop(mfe_key, None)
            self._mfe_last_peak_trig.pop(mfe_key, None)
        max_lev, ct_val, ct_ccy, min_sz = self._get_inst_spec(inst_id)
        # 规格：lotSz/minSz（与 _get_inst_spec 的 min_sz 取较大者）
        lot_sz, min_sz2 = self._get_trade_steps(inst_id)
        min_sz = max(float(min_sz), float(min_sz2))

        # ---------- MQ 杠杆（可能为空） ----------
        mq_lev_raw = (evt.get("lev") or evt.get("leverage")
                      or evt.get("snapshot", {}).get("lev")
                      or evt.get("snapshot", {}).get("leverage"))
        mq_lev = self._parse_lev(mq_lev_raw)
        # 用与下单一致的规则选出“本次实际杠杆”，传给 RG 做 ROE↔价差换算
        selected_lev = self._select_leverage(max_lev, mq_lev)
        logging.info("[LEV] inst=%s max=%s mq=%s mode=%s → selected=%s",
                     inst_id, str(max_lev), str(mq_lev), self.leverage_mode, str(selected_lev))

        # ---------- 提前拿到方向 ----------
        # 更稳：消息里偶尔缺 side，直接丢弃该 OPEN
        side_flag = (evt.get("side") or "-").upper()  # "LONG"/"SHORT"
        if side_flag not in ("LONG","SHORT"):
            logging.warning("[OPEN_EVT] 缺少有效 side，跳过：%s", evt.get("side"))
            return

        # === 反向开单拦截：若已有本地相反方向持仓或挂单，直接跳过 ===
        opp_pos_side = "short" if side_flag == "LONG" else "long"
        has_opp = False
        for pos in positions:
            if pos.get("instId")==inst_id and self.pos_mode=="long_short" and pos.get("posSide","").lower()==opp_pos_side:
                if abs(float(pos.get("pos",0) or 0))>0: has_opp=True; break
        if not has_opp:
            for od in self.get_pending_orders():
                if od.get("instId")==inst_id and od.get("posSide","").lower()==opp_pos_side:
                    if od.get("state") in {"live","partially_filled"}: has_opp=True; break
        if has_opp:
            logging.warning("[OPEN_EVT][BLOCK] 拦截反向开单 inst=%s side=%s：已存在相反方向的本地暴露", inst_id, side_flag)
            return

        # 先准备兜底价与滑点，供 RG 失败或非法价时使用

        last_evt = self._fresh_evt_px(evt)
        last_rest = self._safe_last_price(inst_id)
        last_px = last_evt or last_rest
        logging.info("[PRICE][SOURCES] inst=%s coin=%s evt_last=%s rest_last=%s → use=%s",
                     inst_id, coin, str(last_evt), str(last_rest), str(last_px))
        if last_px is None:
            logging.warning("[TICKER][EMPTY] inst=%s 无可用最新价，本次跳过开仓", inst_id)
            return
        s = self._dynamic_entry_slip(inst_id, last_px)
        use_vp = self._is_monitored(inst_id=inst_id, coin=coin)
        st = self._get_atr_state(inst_id) or {}
        logging.info(
            "[CTX] inst=%s side=%s use_vp=%s slip=%.6f (mode=%s,k=%.3f,min=%.4f,max=%.4f,floor=$%.2f) atr%%=%.5f "
            "EP{enable=%s,win=%ss,bar=%s,trigK=%.2f,satK=%.2f,maxExtra%%=%.3f,bandK=%.2f,gamma=%.2f}",
            inst_id, side_flag, use_vp, float(s),
            self.SLIP_MODE, self.SLIP_ATR_K, self.SLIP_MIN_PCT, self.SLIP_MAX_PCT, self.SLIP_ABS_FLOOR,
            float(st.get('atr_pct') or 0.0),
            self.EP_ENABLE, self.EP_WINDOW_SEC, self.EP_BAR, self.EP_TRIGGER_K_ATR,
            self.EP_SATURATE_K_ATR, self.EP_MAX_EXTRA_PCT, self.EP_ANCHOR_BAND_K_ATR, self.EP_BLEND_GAMMA
        )
     
        # === 统一：无论是否监控，入场基价均按 entry/last 二择 + 动态滑点（复用现有工具） ===
        base_by = None
        if (side_flag == "LONG"  and entry > 0 and entry <  last_px) \
        or (side_flag == "SHORT" and entry > 0 and entry >  last_px):
            base_px = entry; base_by = "entryPx"
        else:
            base_px = last_px; base_by = "last_px"
        # 使用“动态滑点”作为本次入场滑点
        order_px = self._with_slip_for_side(
            "long" if side_flag == "LONG" else "short",
            float(base_px),
            slip=s
        )
        logging.info("[BASELINE][UNIFIED] inst=%s side=%s base_by=%s base=%.8f → preEP(with slip)=%.8f (slip≈%.6f, entry=%.8f last=%.8f)",
                     inst_id, side_flag, base_by, float(base_px), float(order_px), float(s), float(entry), float(last_px))

        if use_vp:
            # 监控分支：仅用来获取 HVN/LVN & guards，不改入场价
            try:
                rg_out = quick_eval_from_event(
                    evt,
                    leverage=float(selected_lev),
                    min_roe_gap_pct=self.RG_MIN_ROE_GAP_PCT,
                    node_min_roe_pct=self.RG_MIN_ROE_GAP_PCT,
                )
                key = f"{inst_id}:{'long' if side_flag=='LONG' else 'short'}"
                self._vp_levels[key] = {
                    # 仍记录 entry_adj 供风控参考（⚠ 不用于下单价）
                    "entry_adj": float(rg_out.get("entry_adj") or 0.0),
                    "sl": list(map(float, rg_out.get("sl", [])[:3])),
                    "tp_nodes": list(map(float, (rg_out.get("nodes", {}) or {}).get("tp_nodes", [])[:3])),
                    "sl_nodes": list(map(float, (rg_out.get("nodes", {}) or {}).get("sl_nodes", [])[:3])),
                    "guards": {
                        "profit_protect_ref": (rg_out.get("guards", {}) or {}).get("profit_protect_ref"),
                        "loss_protect_ref":   (rg_out.get("guards", {}) or {}).get("loss_protect_ref"),
                    },
                }
                self._node_tp_hits[key] = 0
                self._node_sl_hits[key] = 0
                logging.info("[RG][NODES][COLLECT] inst=%s side=%s entry_adj(ref)=%.8f hvn=%s lvn=%s guards=%s (⚠ 入场价已统一按滑点计算)",
                             inst_id, side_flag, float(self._vp_levels[key]["entry_adj"]),
                             self._vp_levels[key]["tp_nodes"], self._vp_levels[key]["sl_nodes"],
                             self._vp_levels[key]["guards"])
            except Exception as e:
                logging.warning("[RG][NODES][SKIP] inst=%s side=%s reason=%s — keep unified slip entry flow",
                                inst_id, side_flag, e)
        else:
            # 未监控：清理 VP/HVN/LVN 状态，保护规则走 ATR
            self._clear_vp_state(inst_id)
        # ②½ 入场价保护：无论是否使用 VP/RG，均根据急跌/急涨场景对入场价做保护性下/上移
        try:
            baseline_before_ep = float(order_px)
            # 统一：EP 使用同一套 slip（与未监控一致）
            slip_used = float(s)
            order_px_after_ep = self._protected_entry_price(
                inst_id=inst_id,
                side_flag=side_flag,                # "LONG" / "SHORT"
                baseline_px=baseline_before_ep,
                last_px=float(last_px),
                slip=slip_used,
            )
            logging.info("[EP][RESULT][UNIFIED] inst=%s side=%s preEP=%.8f → postEP=%.8f (last=%.8f, slip_used=%.6f)",
                         inst_id, side_flag, baseline_before_ep, float(order_px_after_ep), float(last_px), slip_used)
            order_px = order_px_after_ep
        except Exception as _e:
            logging.info("[EP][OPEN][SKIP] inst=%s side=%s reason=%s preEP=%.8f keep=%.8f",
                         inst_id, side_flag, _e, float(order_px), float(order_px))

        # ③ 保护：若最终价格非法则跳过
        if order_px <= 0:
            logging.warning("order_px 非法（entry=%.8f, last=%.8f, slip=%.6f）",
                            entry, last_px, s)
            return
        logging.info("[ORDER_PX_DECISION] inst=%s side=%s final_px=%.8f (entry=%.8f last=%.8f)",
                     inst_id, side_flag, float(order_px), float(entry), float(last_px))
        # === 使用生产端 portfolio_total_usdt 分摊预算（稳健：有则用，无则回退本地累计） ===
        coin_value = abs(szi) * float(entry)
        total_value_evt = float(evt.get("portfolio_total_usdt") or 0.0)
        if total_value_evt > 0:
            proportion = coin_value / total_value_evt
            # 仅用于观测的本地缓存（非分摊依据）
            self._wallet_value[coin] = coin_value
            total_value = total_value_evt
        else:
            # Fallback：evt 未携带组合总额，退化到本地累计（准确性略差）
            self._wallet_value[coin] = coin_value
            total_value = sum(self._wallet_value.values())
            if total_value <= 0:
                logging.warning("总仓位价值为 0（evt 未带 portfolio_total_usdt），跳过下单")
                return
            proportion = coin_value / total_value
        # 夹紧比例到 [0,1]
        proportion = max(0.0, min(1.0, float(proportion)))

        logging.info(
            "[OPEN_EVT] wallet_value[%s]=%.4f, total_from_evt=%.4f (fallback_total=%.4f), proportion=%.4f",
            coin, coin_value, total_value_evt, total_value if total_value_evt == 0 else 0.0, proportion
        )

        # 3) 统计同向「已持仓 + 未成交挂单」，后面只为 *缺口* 下单
        desired_side  = "buy" if side_flag == "LONG" else "sell"
        curr_contract = 0.0      # 现有仓位（张）
        pending_same  = 0.0      # 未成交挂单（张）

        # a) 当前已持仓（按方向聚合）
        expect_pos = "long" if side_flag == "LONG" else "short"
        for pos in positions:
            if pos.get("instId") != inst_id:
                continue
            if self.pos_mode == "long_short":
                if pos.get("posSide", "").lower() != expect_pos:
                    continue
            curr_contract += abs(float(pos.get("pos", 0) or 0))

        # b) 未成交同向委托
        for od in self.get_pending_orders():
            if od.get("instId") != inst_id:
                continue
            if od.get("side", "").lower() != desired_side:
                continue
            if self.pos_mode == "long_short":
                expect_pos = "long" if side_flag == "LONG" else "short"
                if od.get("posSide", "").lower() != expect_pos:
                    continue
            unfilled = float(od.get("sz", 0)) - float(od.get("accFillSz", 0) or 0)
            pending_same += max(unfilled, 0)

        logging.info("[OPEN_EVT] existing=%.4f  pending=%.4f (contracts)", curr_contract, pending_same)
     
        # —— 冷启动保护（严格）：暖身期内如已有同向“已成交仓位或未成交挂单”，一律不再新建 —— #
        if self._in_warmup() and (curr_contract > 0 or pending_same > 0):
            try:
                pos_side_for_lev = "long" if side_flag == "LONG" else "short"
                self._ensure_leverage(inst_id, pos_side_for_lev, mq_lev)
            except Exception as _e:
                logging.debug("[OPEN_EVT][warmup][ensure_lev][skip] %s", _e)
            logging.info(
                "[OPEN_EVT] warmup skip (strict): uptime=%.2fs, curr=%.4f pending=%.4f",
                time.time() - self._start_ts, curr_contract, pending_same
            )
            return

        # —— 全程去重：若已存在同 instId + 同方向 的 live 挂单，则不再创建新挂单 —— #
        if pending_same > 0:
            logging.info("[DEDUP][OPEN] inst=%s side=%s existing-pending=%.8f → skip new order",
                         inst_id, ("LONG" if side_flag=="LONG" else "SHORT"), pending_same)
            return
        # -------- 4) 计算当前可用 USDT 与本次预算（新公式） --------
        avail_usdt = self._get_available_usdt()
        logging.info("当前可用 USDT=%.8f", avail_usdt)
        if avail_usdt == 0:
            logging.error("USDT 余额为 0，无法下单")
            return
        # ==== 新资金规则： (余额 - 最低保底) * BUDGET_RATIO = 单次建仓总额 ====
        tradeable = max(0.0, float(avail_usdt) - float(self.RESERVE_MIN_USDT))
        build_total_budget = tradeable * float(self.BUDGET_RATIO)
        # 按 master 权重把“单次建仓总额”拆分到单币预算
        budget_usdt = build_total_budget * float(proportion)
        logging.info(
            "[WEIGHT_COPY] tradeable=%.4f USDT (avail=%.4f - reserve_min=%.4f)  "
            "build_total=%.4f  proportion=%.4f  → budget=%.4f USDT (evt_total=%.4f)",
            tradeable, avail_usdt, float(self.RESERVE_MIN_USDT),
            build_total_budget, proportion, budget_usdt, float(evt.get('portfolio_total_usdt') or 0.0)
        )
        # 预算按波动率缩放（atr 越大预算越小；risk-parity 近似）
        budget_usdt *= self._budget_scale_by_vol(inst_id)
        if budget_usdt <= 0:
            logging.warning("预算 ≤ 0（tradeable=%.4f），跳过下单", tradeable)
            return


        # 5) 计算下单张数（让保证金≈budget_usdt；按 lotSz 对齐）
        # 与下单保持一致的杠杆，避免“下单与预算计算”不一致
        lev = selected_lev
        # === 折扣系数：查询 tier，得到初始保证金比率 imr（走 PublicData）===
        # === 新：imr 走 TTL 缓存（常态不查 HTTP） ===
        eff_imr = self._get_imr_fast(inst_id, lev)

        # === 每张名义价值 → 预算能买几张 ===
        notional     = ct_val if ct_ccy.upper() == "USDT" else ct_val * order_px
        raw_contracts = budget_usdt / (notional * eff_imr)
        contracts = self._quantize_size(raw_contracts, lot_sz, "down")  # 不超过预算
        # 注意：此处 contracts 对应的保证金 ≈ budget_usdt
        # --- 如果保证金仍不足，按步长逐步递减 ---
    
        while contracts > 0:
            need_margin = contracts * notional * eff_imr
            # 双重约束：余额桶（tradeable）与预算桶（budget_usdt）
            if need_margin <= tradeable and need_margin <= budget_usdt:
                break
            contracts = self._quantize_size(max(0.0, contracts - lot_sz), lot_sz, "down")
        if contracts == 0:
            logging.info("保证金不足，跳过下单")
            return

        # 不足最小下单量 → 提升到 minSz；仍需同时约束【保证金 ≤ tradeable】且【保证金 ≤ budget_usdt】
        if contracts < float(min_sz):
            want = self._ensure_min_step(min_sz, lot_sz, min_sz, "up")
            need_margin = want * notional * eff_imr
            if need_margin > min(tradeable, budget_usdt):
                logging.info(
                    "保证金不足（minSz 提升后超限），need=%.4f  tradeable=%.4f  budget=%.4f → 跳过下单",
                    need_margin, tradeable, budget_usdt
                )
                return
            contracts = want

        # ⑥ 计算缺口：目标 contracts − 已持仓 − 未成交挂单
        shortage = contracts - curr_contract - pending_same
        if shortage <= 0:
            logging.info("目标=%s 已持=%.4f 挂单=%.4f ⇒ 无需再下单", contracts, curr_contract, pending_same)
            return

        # 缺口按步长对齐；若仍小于 minSz，则抬到 minSz
        shortage = self._quantize_size(shortage, lot_sz, "down")
        if shortage < float(min_sz):
            shortage = self._ensure_min_step(min_sz, lot_sz, min_sz, "up")
        # 以字符串形式传给下单接口，保留小数步长
        size = f"{shortage:.8f}".rstrip('0').rstrip('.')

        logging.info(
            "[SIZE_CALC] budget=%.4f USDT  lev=%s×  → contracts=%s  notional/ct=%.2f",
            budget_usdt, lev, contracts, notional
        )

        side = desired_side

        # ---------- 按全局 mode 决定是否真实下单 ----------
        if mode == "test":
            logging.info(
                "[TEST MODE] Would open position: inst_id=%s, price=%s, size=%s, side=%s",
                inst_id, order_px, size, side
            )
            resp = {
                "code": "0",
                "msg": "test mode - order not sent",
                "data": [{
                    "instId": inst_id,
                    "px": str(order_px),
                    "sz": str(size),
                    "side": side
                }]
            }
        else:  # mode == "real"
            try:
                pos_side_for_lev = "long" if side_flag=="LONG" else "short"
                self._ensure_leverage(inst_id, pos_side_for_lev, mq_lev)
                logging.info("[LEV][ENSURE] inst=%s posSide=%s set_to=%s", inst_id, pos_side_for_lev, str(selected_lev))
            except Exception as _e:
                logging.info("[LEV][ENSURE][SKIP] inst=%s reason=%s", inst_id, _e)
            # 首发下单
            extra = {}
            if getattr(self, "ENTRY_POST_ONLY", False):
                extra["ord_type"] = "post_only"
            resp_first = self._send_with_authority_fallback(
                self.open_position,
                inst_id=inst_id,
                price=order_px,
                size=size,
                side=side,
                mq_lev=mq_lev,
                size_unit="contracts",
                **extra
            )
            # 命中 51121 时，按 lotSz 向下对齐并重试一次
            resp = self._maybe_retry_51121(
                resp_first,
                send_again=lambda new_size: self._send_with_authority_fallback(
                    self.open_position,
                    inst_id=inst_id, price=order_px, size=new_size, side=side, mq_lev=mq_lev, size_unit="contracts"
                ),
                size_str=str(size), lot_sz=float(lot_sz), min_sz=float(min_sz),
                inst_id=inst_id, side_desc=side, px=float(order_px)
            )
            logging.info("自动开仓 resp=%s", resp)
        # —— 操作后若超过阈值则做一次权威同步（含孤单） —— #
        with suppress(Exception):
            self._maybe_sync_positions_after_operation(inst_id=inst_id, reason="OPEN")
        return resp

    # ============================================================
    #  SIZE-INC → 加仓
    # ============================================================
    def _handle_size_inc(self, evt: dict):
        coin   = evt.get("coin", "").upper()
        side   = evt.get("side", "-").upper()          # LONG / SHORT
        # MQ 里偶尔会出现 snapshot.szi 为 None/""，需做健壮处理
        positions = self.get_positions()  # 单次复用
        szi_raw = evt.get("snapshot", {}).get("szi")
        try:
            new_szi = abs(float(szi_raw))
        except (TypeError, ValueError):
            logging.debug("[INC] 无有效 szi（got=%s），跳过本条消息", szi_raw)
            return
        if new_szi == 0:
            return
        # 更新最近一次观测 szi
        prev_szi = self._last_szi.get(coin)
        inst_id = self._get_inst_id(coin)
        if not inst_id:
            # 诊断：记录 miss/filter/ttl（只日志，不改业务）
            logging.info("[LP][SKIP] no-instId coin=%s filtered=%s miss_cache=%s",
                         coin, self._is_filtered_coin(coin), self._skip_miss_coin(coin))
            return
  
        # 更新本合约的 ATR%（如 MQ 带 vol.atr_pct）
        try:
            if isinstance(evt.get("vol"), dict) and "atr_pct" in evt["vol"]:
                self._vol_cache[inst_id] = {"atr_pct": float(evt["vol"]["atr_pct"]), "ts": time.time()}
        except Exception:
            pass
        # 当前已持仓（按方向聚合）
        curr = 0.0
        expect_pos_side = "long" if side.upper() == "LONG" else "short"
        for pos in positions:
            if pos.get("instId") != inst_id:
                continue
            if self.pos_mode == "long_short":
                if pos.get("posSide", "").lower() != expect_pos_side:
                    continue
            qty = float(pos.get("pos", 0) or 0)
            curr += abs(qty)
        # ---------- Ⅰ-A. 获取最新价 & 合约规格 ---------- #
        last_px = self._fresh_evt_px(evt) or self._safe_last_price(inst_id)
        if last_px is None:
            logging.warning("[TICKER][EMPTY] inst=%s，跳过本次 INC", inst_id)
            return
        # 计算加仓限价：entryPx/lastPx 二择其一作基准，并按方向加滑点
        entry = float((evt.get("snapshot", {}) or {}).get("entryPx") or 0)
        if side == "LONG":
            base_px = entry if (entry > 0 and entry < last_px) else last_px
        else:
            base_px = entry if (entry > 0 and entry > last_px) else last_px
        # 给加仓也用“动态滑点”（若随后跟随逻辑 shrink 了 slip，会覆盖为更优价）
        try:
            s_dyn = self._dynamic_entry_slip(inst_id, last_px)
        except Exception:
            s_dyn = None
        order_px = self._with_slip_for_side(
            "long" if side == "LONG" else "short",
            float(base_px),
            slip=s_dyn
        )
        # —— 改为直接调用你提供的工具函数 —— #
        new_slip = None
        force_ioc = False
        shrink_ratio = 0.0
        try:
            if self.FOLLOW_ENABLE:
                new_slip, force_ioc, shrink_ratio = self._follow_shrink_entry_slip(
                    inst_id=inst_id,
                    pos_side=("long" if side == "LONG" else "short"),
                    peer_avg_px=float(entry or 0.0),   # 主钱包合并后均价（MQ snapshot.entryPx）
                    last_px=float(last_px),
                )
                if new_slip is not None:
                    order_px = (float(base_px) * (1.0 - float(new_slip))) if side == "LONG" else (float(base_px) * (1.0 + float(new_slip)))
                    logging.info(
                        "[FOLLOW][INC] helper shrink applied: inst=%s side=%s my_avg=%.8f peer_avg=%.8f last=%.8f "
                        "new_slip=%.6f shrink_ratio=%.3f → order_px=%.8f (force_ioc=%s)",
                        inst_id, side,
                        self._avg_entry_px(inst_id, "long" if side=="LONG" else "short"),
                        float(entry or 0.0), float(last_px),
                        float(new_slip), float(shrink_ratio), float(order_px), bool(force_ioc)
                    )
        except Exception as _e:
            logging.debug("[FOLLOW][INC][HELPER][SKIP] %s", _e)
        # 入场价保护：急跌/急涨下对加仓限价做保护性调整
        try:
            baseline_before_ep = float(order_px)
            s = abs(float(self.ENTRY_SLIP or 0.0))
            order_px = self._protected_entry_price(
                inst_id=inst_id,
                side_flag=side,                     # "LONG" / "SHORT"
                baseline_px=baseline_before_ep,
                last_px=float(last_px),
                slip=s,
            )
            logging.debug("[EP][INC] inst=%s side=%s baseline=%.8f → protected=%.8f (last=%.8f, slip=%.5f)",
                          inst_id, side, baseline_before_ep, float(order_px), float(last_px), s)
        except Exception as _e:
            logging.debug("[EP][INC][SKIP] %s", _e)
        max_lev, ct_val, ct_ccy, min_sz = self._get_inst_spec(inst_id)
        lot_sz, min_sz2 = self._get_trade_steps(inst_id)
        min_sz = max(float(min_sz), float(min_sz2))
        mq_lev_raw = (evt.get("lev") or evt.get("leverage")
                      or evt.get("snapshot", {}).get("lev")
                      or evt.get("snapshot", {}).get("leverage"))
        mq_lev = self._parse_lev(mq_lev_raw)

        # 把同向未成交挂单也计入“本地暴露”，并收集最优挂单价做价格去重
        pending_same = 0.0
        expected_side = "buy" if side.upper() == "LONG" else "sell"
        best_pending_px = None   # LONG 取最小价；SHORT 取最大价
        for od in self.get_pending_orders():
            if od.get("instId") != inst_id:
                continue
            if od.get("side", "").lower() != expected_side:
                continue
            # 只考虑有效状态的挂单
            if od.get("state") not in {"live", "partially_filled"}:
                continue
            if self.pos_mode == "long_short":
                expect_pos_side = "long" if side.upper() == "LONG" else "short"
                if od.get("posSide", "").lower() != expect_pos_side:
                    continue
            unfilled = float(od.get("sz", 0)) - float(od.get("accFillSz", 0) or 0)
            if unfilled > 0:
                pending_same += unfilled
            # 收集该方向“更优价格”
            try:
                px = float(od.get("px") or 0.0)
            except Exception:
                px = 0.0
            if px > 0:
                if side.upper() == "LONG":
                    best_pending_px = px if (best_pending_px is None or px < best_pending_px) else best_pending_px
                else:  # SHORT
                    best_pending_px = px if (best_pending_px is None or px > best_pending_px) else best_pending_px
        curr_total = curr + pending_same

        # —— 价格型去重：若已存在“更优价格”的同向挂单 ⇒ 跳过本次 INC —— #
        # 需求：LONG 已挂单价 < 新加仓“入场价” → 跳过；SHORT 已挂单价 > 新加仓“入场价” → 跳过
        # “入场价”优先使用 MQ 的 entryPx；缺失则回退到本次计算后的 order_px
        cmp_px = float((evt.get("snapshot", {}) or {}).get("entryPx") or 0.0)
        if cmp_px <= 0:
            cmp_px = float(order_px)
        if best_pending_px is not None:
            # 含容差的价格去重：使用 _px_tol_pct(instId)（若无则回退 trigger_px_tolerance_pct）
            tol_pct = 0.0
            try:
                _f = getattr(self, "_px_tol_pct", None)
                tol_pct = float(_f(inst_id) or 0.0) if callable(_f) else float(self.TRIGGER_PX_TOL_PCT or 0.0)
            except Exception:
                tol_pct = float(self.TRIGGER_PX_TOL_PCT or 0.0)
            tol_abs = abs(float(cmp_px)) * (float(tol_pct) / 100.0)
            if (side.upper() == "LONG"  and best_pending_px <= float(cmp_px) + tol_abs) or \
               (side.upper() == "SHORT" and best_pending_px >= float(cmp_px) - tol_abs):
                logging.info(
                    "[INC][SKIP][PRICE_DEDUP] inst=%s side=%s best_pending_px=%.8f cmp_px=%.8f tol%%=%.4f tol=%.8f "
                    "(pending_same=%.8f) → skip new limit",
                    inst_id, side, float(best_pending_px), float(cmp_px), float(tol_pct), float(tol_abs), float(pending_same)
                )
                # 同样更新主钱包规模观测，避免后续比例计算异常
                self._last_szi[coin] = new_szi
                return
        # ── 保护策略 B：仅当本地已有同向暴露（持仓或同向挂单）时才处理 INC ──
        # 解决“重启后第一条就是 INC 导致直接开仓”的问题
        if curr_total <= 0:
            logging.info("[INC][SKIP][B] no local exposure yet; ignore INC until OPEN or local pos/pending exists")
            # 仍然更新主钱包最新规模观测，后续比率计算不至于异常
            self._last_szi[coin] = new_szi
            return
        # ------- 新：按“主钱包 USDT 占比”计算加仓百分比 ------- #
        # inc_pct = (Δszi * px) / (master_portfolio_total_usdt)  （全局占比，避免大户绝对加仓拖累小户）
        delta_szi = max(0.0, float(new_szi) - float(prev_szi or 0.0))
        # 取用于换算 USDT 的价格：优先 entry，其次最新 last_px
        px_for_delta = float((evt.get("snapshot", {}) or {}).get("entryPx") or 0.0)
        if px_for_delta <= 0:
            px_for_delta = float(last_px)
        total_usdt_evt = float(evt.get("portfolio_total_usdt") or 0.0)
        # USDT 口径的加仓占比；缺总额时回退到币种自身占比 (new-prev)/prev
        if total_usdt_evt > 0 and px_for_delta > 0:
            inc_pct = (delta_szi * px_for_delta) / total_usdt_evt
        else:
            inc_pct = (delta_szi / float(prev_szi)) if (prev_szi and prev_szi > 0) else 0.0

        # 夹紧到 [0, INC_PCT_CAP]（默认 1.0，可配置）
        cap = float(getattr(self, "INC_PCT_CAP", 1.0))
        inc_pct = max(0.0, min(cap, float(inc_pct)))
        # 仅以“本地已持仓（不含挂单）”为基数
        desired_from_pct = max(0.0, float(curr)) * inc_pct
        diff_contracts = self._quantize_size(desired_from_pct, lot_sz, "down")
        logging.info(
            "[INC] pct-based(add by master-USDT) | prev_szi=%.8f new_szi=%.8f Δszi=%.8f px=%.8f "
            "totalUSDT=%.4f inc_pct=%.6f base_local=%.8f → desired=%.8f (contracts, pre-cap)",
            float(prev_szi or 0.0), float(new_szi), float(delta_szi), float(px_for_delta),
            float(total_usdt_evt), float(inc_pct), float(curr), float(diff_contracts)
        )


        if diff_contracts <= 0:
            logging.info("[INC] 无需加仓：base_local=%.4f，prev=%.4f → new=%.4f",
                         curr, prev_szi or -1.0, new_szi)
            self._last_szi[coin] = new_szi
            return

        # --- Ⅱ. 可支配余额 & 预算上限（与 OPEN 新规则一致） --- #
        avail_usdt   = self._get_available_usdt()
        tradeable    = max(0.0, float(avail_usdt) - float(self.RESERVE_MIN_USDT))
        if tradeable == 0:
            logging.info("INC but no tradeable USDT (avail=%.4f)", avail_usdt)
            return

        # --- Ⅲ. 预算上限：不按币种权重拆桶，直接用余额桶的比例上限 --- #
        # 预算：余额桶 × budget_ratio（夹在 0..1）
        br = max(0.0, min(1.0, float(self.BUDGET_RATIO)))
        budget_usdt = tradeable * br
        # 波动率缩放（risk-parity 近似，保持与 OPEN 一致，可令有效上限更保守）
        budget_usdt = budget_usdt * self._budget_scale_by_vol(inst_id)
        # ---------- 最新价格 & 合约规格 ---------- #

        notional = ct_val if ct_ccy.upper() == "USDT" else ct_val * last_px
        if notional <= 0:
            logging.info("[INC][SKIP] invalid notional: ct_val=%s last_px=%s ct_ccy=%s",
                         ct_val, last_px, ct_ccy)
            return
        lev = self._select_leverage(max_lev, mq_lev)

        # === 新：imr 走 TTL 缓存（常态不查 HTTP） ===
        eff_imr = self._get_imr_fast(inst_id, lev)
        if not (eff_imr and eff_imr > 0):
            logging.info("[INC][SKIP] invalid eff_imr=%s", eff_imr)
            return

        # 预算能买几张（按步长对齐）；把“按百分比算出的加仓量”夹在预算上限内
        max_by_budget = self._quantize_size(budget_usdt / (notional * eff_imr), lot_sz, "down")
        contracts     = self._quantize_size(diff_contracts, lot_sz, "down")
        if max_by_budget > 0:
            contracts = min(contracts, max_by_budget)
        # --- 保证金/预算再次校验，不行就递减（不再引用已删除的 reserve_usdt） ---
        while contracts > 0:
            need_margin = contracts * notional * eff_imr
            if need_margin <= tradeable:
                break
            contracts = self._quantize_size(max(0.0, contracts - lot_sz), lot_sz, "down")
        if contracts == 0:
            logging.info("保证金/预算不足，INC 放弃")
            return
        if contracts < float(min_sz):                        # 不足最小下单量 → 抬到 minSz
            want = self._ensure_min_step(min_sz, lot_sz, min_sz, "up")
            need_margin = want * notional * eff_imr
            if need_margin > min(tradeable, budget_usdt):
                logging.info("保证金不足（minSz），INC 放弃")
                return
            contracts = want



        diff_contracts = contracts
        side_param = "buy" if side == "LONG" else "sell"

        logging.info(
            "[INC] %s base=%.8f inc_pct=%.6f → add=%.8f (%s) | "
            "budget_cap=%.4fUSDT max_by_budget=%.8f eff_imr=%.6f",
            inst_id, curr, inc_pct, diff_contracts, side_param,
            budget_usdt, max_by_budget, eff_imr
        )
        if mode == "test":
            logging.info("[TEST] Would add_position inst=%s px=%.8f size=%.8f side=%s",
                         inst_id, order_px, diff_contracts, side_param)
        else:
            
            # 默认 post_only；需要吃单时由跟随逻辑置为 ioc
            extra = {"ord_type": "post_only"} if getattr(self, "ENTRY_POST_ONLY", False) else {}
            if force_ioc:
                extra = {"ord_type": "ioc"}
            size_str = f"{diff_contracts:.8f}".rstrip('0').rstrip('.')
            resp = self._send_with_authority_fallback(
                self.add_position,
                inst_id, order_px, size_str, side_param,
                mq_lev=mq_lev,
                size_unit="contracts",
                **extra
            )
            # 命中 51121 时，按 lotSz 向下对齐并重试一次
            resp = self._maybe_retry_51121(
                resp,
                send_again=lambda new_size: self._send_with_authority_fallback(
                    self.add_position,
                    inst_id, order_px, new_size, side_param,
                    mq_lev=mq_lev, size_unit="contracts", **extra
                ),
                size_str=size_str, lot_sz=float(lot_sz), min_sz=float(min_sz),
                inst_id=inst_id, side_desc=side_param, px=float(order_px)
            )
            logging.info("[INC] add_position resp=%s", resp)
        # 更新观测
        self._last_szi[coin] = new_szi
        # —— 操作后同步（超过阈值则触发） —— #
        with suppress(Exception):
            self._maybe_sync_positions_after_operation(inst_id=inst_id, reason="INC")
    # ============================================================
    #  SIZE-DEC → 减仓
    # ============================================================
    def _handle_size_dec(self, evt: dict):
        coin = (evt.get("coin") or "").upper()
        side = (evt.get("side") or "-").upper()  # "LONG" / "SHORT"
        # 1) 解析新规模（健壮转换）
        szi_raw = (evt.get("snapshot") or {}).get("szi")
        try:
            new_szi = max(0.0, abs(float(szi_raw)))
        except (TypeError, ValueError):
            logging.debug("[DEC] 无有效 szi（got=%s），跳过", szi_raw)
            return
        if not coin or side not in ("LONG", "SHORT"):
            return
        # == 1.1 若是“直接减到 0”事件，先以交易所为准做一次权威重建 ==
        if new_szi == 0.0:
            inst_id = self._get_inst_id(coin)
            if inst_id:
                self._authority_resync(reason="SIZE_DEC:snapshot.szi==0", inst_id=inst_id)
                self._log_exposure(inst_id)
            else:
                logging.info("[DEC][AUTH][SKIP] no instId for coin=%s", coin)
        # == 1.2 继续后续比例缩放逻辑（resync 只校准本地缓存，不改变下游语义） ==
        # 2) 优先“比例缩放”：ratio = new_szi / prev_szi
        prev_szi = self._last_szi.get(coin)
        ratio: float | None = None
        if prev_szi and prev_szi > 0:
            ratio = max(0.0, min(1.0, float(new_szi) / float(prev_szi)))
        else:
            # 3) 回退：用“绝对 szi→张数”的目标来倒推比例
            inst_id = self._get_inst_id(coin)
            if not inst_id:
               return
            last_px = self._fresh_evt_px(evt) or self._safe_last_price(inst_id)
            if last_px is None:
                logging.debug("[DEC][FALLBACK] 无可用价格，跳过本次")
                return
            _, ct_val, ct_ccy, _ = self._get_inst_spec(inst_id)
            target_ct = self._coin_to_contracts(float(new_szi), float(ct_val), str(ct_ccy), float(last_px))
            pos_side = "long" if side == "LONG" else "short"
            held_now = self._held_contracts_by_side(inst_id, pos_side)
            _, unfilled = self._pending_closes_by_side(inst_id, pos_side)
            curr_total = float(held_now) + float(unfilled)  # 注意：这里的 unfilled 为 pending_close；不开仓 pending 不计入
            if curr_total <= 0:
                # 本地无暴露，直接记录最新观测并返回
                self._last_szi[coin] = new_szi
                return
            ratio = max(0.0, min(1.0, float(target_ct) / float(curr_total)))

        # 4) 改为“累计到最小下单量再减仓”的逻辑（按 instId+posSide 聚合）
        try:
            inst_id = self._get_inst_id(coin)
            if not inst_id:
                return
            pos_side = "long" if side == "LONG" else "short"
            # 以“本地暴露”作为基数：已持仓 + 未成交平仓挂单
            held_now = self._held_contracts_by_side(inst_id, pos_side)
            _, pending_close = self._pending_closes_by_side(inst_id, pos_side)
            base_ct = Decimal(str(float(held_now) + float(pending_close)))
            if base_ct <= 0:
                self._last_szi[coin] = new_szi
                return
            # 本次应减张数（不对齐步长，直接累计）
            delta_raw = (Decimal("1") - Decimal(str(ratio))) * base_ct
            if delta_raw <= 0:
                return
            self._accumulate_dec_and_maybe_flush(inst_id, pos_side, delta_raw, evt)
            logging.info("[DEC][ACC] inst=%s pos=%s base=%.8f ratio=%.6f +Δ=%.8f acc=%.8f",
                         inst_id, pos_side, float(base_ct), float(ratio),
                         float(delta_raw), float(self._dec_accum.get((inst_id,pos_side), Decimal('0'))))
        except Exception as e:
            logging.exception("[DEC][ACC][FAIL] coin=%s side=%s ratio=%.6f err=%s", coin, side, float(ratio), e)
        finally:
            # 更新主钱包规模观测
            self._last_szi[coin] = new_szi
            # —— 操作后同步（超过阈值则触发） —— #
            with suppress(Exception):
                iid = None
                try: iid = self._get_inst_id(coin)
                except Exception: pass
                if iid: self._maybe_sync_positions_after_operation(inst_id=iid, reason="DEC")
        return
    # ============================================================
    #  SIZE-REVERSE → 反手
    # ============================================================
    def _handle_size_reverse(self, evt: dict):
        coin   = evt.get("coin", "").upper()
        side   = evt.get("side", "-").upper()          # 反手后的新方向
        # 钱包 szi 是“币数量” → 先记下来，后面统一换算成“张”
        target_coin = abs(float(evt.get("snapshot", {}).get("szi", 0)))
        positions = self.get_positions()  # 单次复用
        inst_id = self._get_inst_id(coin)
        if not inst_id:
            return

        # 1) 先把现有仓位全部平掉
        for pos in positions:
            if pos.get("instId") != inst_id:
                continue
            curr_sz = abs(float(pos.get("pos", 0)))
            if curr_sz == 0:
                continue

            last_px = float(self._safe_last_price(inst_id) or 0)
            # reduce_position 需要传“原方向”：优先 posSide，其次 qty 符号
            ps_now = (pos.get("posSide") or "").lower()
            if ps_now in ("long", "short"):
                orig_side = "buy" if ps_now == "long" else "sell"
            else:
                orig_side = "buy" if float(pos.get("pos", 0)) > 0 else "sell"
            pos_side_now = "long" if orig_side == "buy" else "short"
            px_red = float(last_px)
            if mode == "test":
                logging.info("[TEST] Would reduce_position inst=%s size=%.8f side=%s", inst_id, curr_sz, orig_side)
            else:
                self.reduce_position(inst_id, px_red, curr_sz, orig_side)

        if target_coin == 0:
            return

        # 2) 再按新方向开仓 target size
        # 优先事件里的热价格，再回退到缓存/REST
        last_px = self._fresh_evt_px(evt) or self._safe_last_price(inst_id)
        if last_px is None:
            logging.warning("[TICKER][EMPTY] inst=%s，跳过本次 REVERSE", inst_id)
            return
        last_px = float(last_px)
        max_lev, ct_val, ct_ccy, min_sz = self._get_inst_spec(inst_id)
        target_contracts                = self._coin_to_contracts(
                                            target_coin, ct_val, ct_ccy, last_px)
        side_param = "buy" if side == "LONG" else "sell"
        # 以 entryPx/lastPx 选基准价并按方向加滑点
        entry = float((evt.get("snapshot", {}) or {}).get("entryPx") or 0)
        if side == "LONG":
            base_px = entry if (entry > 0 and entry < last_px) else last_px
        else:
            base_px = entry if (entry > 0 and entry > last_px) else last_px
        # 反转开仓不加滑点
        order_px = float(base_px)
        # 入场价保护：反手同样应用保护（反手不加滑点 → slip=0）
        try:
            baseline_before_ep = float(order_px)
            order_px = self._protected_entry_price(
                inst_id=inst_id,
                side_flag=side,                     # "LONG" / "SHORT"
                baseline_px=baseline_before_ep,
                last_px=float(last_px),
                slip=0.0,
            )
            logging.debug("[EP][REVERSE] inst=%s side=%s baseline=%.8f → protected=%.8f (last=%.8f)",
                          inst_id, side, baseline_before_ep, float(order_px), float(last_px))
        except Exception as _e:
            logging.debug("[EP][REVERSE][SKIP] %s", _e)
        # === 反手后的开仓 ===
        # 估算/透传本次杠杆；与其他分支一致的选择规则会在 open_position 内部 ensure
        mq_lev_raw = (evt.get("lev") or evt.get("leverage")
                      or evt.get("snapshot", {}).get("lev")
                      or evt.get("snapshot", {}).get("leverage"))
        mq_lev = self._parse_lev(mq_lev_raw)

        # 最小下单量对齐规格
        max_lev, ct_val, ct_ccy, min_sz = self._get_inst_spec(inst_id)
        lot_sz, min_sz2 = self._get_trade_steps(inst_id)
        min_sz = max(float(min_sz), float(min_sz2))
        size_ct = self._ensure_min_step(target_contracts, lot_sz, min_sz, "up")
        size = f"{size_ct:.8f}".rstrip('0').rstrip('.')
        side_param = "buy" if side == "LONG" else "sell"

        if mode == "test":
            logging.info("[TEST][REVERSE] Would open_position inst=%s px=%.8f size=%s side=%s",
                         inst_id, float(order_px), size, side_param)
            return {
                "code": "0", "msg": "test mode - order not sent",
                "data": [{"instId": inst_id, "px": str(order_px), "sz": size, "side": side_param}]
            }
        else:
            # 首发下单
            resp_first = self._send_with_authority_fallback(
                self.open_position,
                inst_id=inst_id, price=float(order_px), size=size, side=side_param, mq_lev=mq_lev, size_unit="contracts"
            )
            # 命中 51121 时，按 lotSz 向下对齐并重试一次
            resp = self._maybe_retry_51121(
                resp_first,
                send_again=lambda new_size: self._send_with_authority_fallback(
                    self.open_position,
                    inst_id=inst_id, price=float(order_px), size=new_size, side=side_param, mq_lev=mq_lev, size_unit="contracts"
                ),
                size_str=str(size), lot_sz=float(lot_sz), min_sz=float(min_sz),
                inst_id=inst_id, side_desc=side_param, px=float(order_px)
            )
            logging.info("[REVERSE] open_position resp=%s", resp)
            # —— 操作后同步（超过阈值则触发） —— #
            with suppress(Exception):
                self._maybe_sync_positions_after_operation(inst_id=inst_id, reason="REVERSE")
            return resp
    # ---- 新增：仅撤同方向未成交挂单 ----
    def _cancel_pending_side(self, inst_id: str, pos_side: str | None = None, side: str | None = None) -> int:
        """
        仅撤该合约下、匹配方向的未成交挂单。
        pos_side: "long" / "short"（优先使用）
        side    : "buy"  / "sell"  （可选补充过滤）
        返回撤单数量。
        """
        cnt = 0
        need_resync = False
        for od in self.get_pending_orders():
            try:
                if od.get("instId") != inst_id:
                    continue
                if pos_side and (od.get("posSide", "").lower() != pos_side.lower()):
                    continue
                if side and (od.get("side", "").lower() != side.lower()):
                    continue
                res = self._cancel_okx(od.get("ordId"), inst_id)
                status = res.get("status")
                if res.get("ok"):
                    # 无论 cancelled / finished / not_found，都应从本地 pending 移除
                    with suppress(Exception):
                        self.orders.pop(od.get("ordId"), None)
                    cnt += 1
                    if status in ("finished", "not_found"):
                        need_resync = True  # 这两类意味着市场状态与本地可能不同步
                else:
                    logging.warning("[CANCEL][PENDING][FAIL] inst=%s ordId=%s code=%s raw=%s",
                                    inst_id, od.get("ordId"), res.get("code"), str(res.get("raw"))[:256])
            except Exception as e:
                logging.warning("[CANCEL][FAIL] inst=%s ordId=%s err=%s", inst_id, od.get("ordId"), e)
        if cnt:
            logging.info("[CANCEL][SIDE] inst=%s posSide=%s side=%s -> canceled=%d", inst_id, pos_side, side, cnt)
            # 撤单后做一次权威重建，确保缓存落地为“真相源”
            with suppress(Exception):
                self._authority_resync(reason="CANCEL:after", inst_id=inst_id if inst_id else None)
                self._mark_positions_synced()
     
        return cnt
    # ============================================================
    #  FILL-CLOSE → 全平
    # ============================================================
    def _handle_fill_close(self, evt: dict):
        coin = evt.get("coin", "").upper()
        inst_id = self._get_inst_id(coin)
        if not inst_id:
            return
        positions = self.get_positions()  # 单次复用
        # ★ 平仓：仅撤同方向未成交挂单（方向未知时退回撤该交易对全部）
        side_hint = (evt.get("side") or "").upper()  # "LONG"/"SHORT"/""
        try:
            if side_hint in ("LONG","SHORT") and self.pos_mode == "long_short":
                pos_side = "long" if side_hint == "LONG" else "short"
                side = "buy" if side_hint == "LONG" else "sell"
                self._cancel_pending_side(inst_id, pos_side=pos_side, side=side)
            else:
                self._cancel_all_pending(inst_id)
        except Exception as e:
            logging.warning("[CLOSE][CANCEL][FAIL] inst=%s err=%s", inst_id, e)
        # 若是“全撤单”路径，也立刻做一次权威同步，确保挂单/孤单落地为交易所真相
        with suppress(Exception):
            if side_hint not in ("LONG","SHORT"):
                self._authority_resync(reason="CLOSE:after-cancel-all", inst_id=inst_id)
                self._mark_positions_synced()
        # —— 撤单/平仓后：若超过阈值则权威同步，覆盖本地（含孤单） —— #
        with suppress(Exception):
            self._maybe_sync_positions_after_operation(inst_id=inst_id, reason="CLOSE-CANCEL")
        # 清理主钱包规模观测，避免用旧 szi 做比例跟随
        self._last_szi.pop(coin, None)
        # 清理 VP 级别与触发计数
        self._vp_levels.pop(f"{inst_id}:long",  None); self._vp_levels.pop(f"{inst_id}:short", None)
        self._node_tp_hits.pop(f"{inst_id}:long", None); self._node_tp_hits.pop(f"{inst_id}:short", None)
        self._node_sl_hits.pop(f"{inst_id}:long", None); self._node_sl_hits.pop(f"{inst_id}:short", None)

        # 清掉两个方向的阶梯记录（全平）
        self._pp_hits.pop(self._pp_key(inst_id, "long"),  None)
        self._pp_hits.pop(self._pp_key(inst_id, "short"), None)
        self._save_profit_guard_state()
        self._lp_hits.pop(self._pp_key(inst_id, "long"),  None)
        self._lp_hits.pop(self._pp_key(inst_id, "short"), None)
        self._save_loss_guard_state()
        # 清理价格去重点：合并一次性落盘，避免重复写盘
        for _ps in ("long", "short"):
            self._pp_last_px.pop(self._pp_key(inst_id, _ps), None)
            self._lp_last_px.pop(self._pp_key(inst_id, _ps), None)
        self._save_profit_guard_px_state()
        self._save_loss_guard_px_state()
        # 清理 MFE/滞回/冷却
        for k in (f"{inst_id}:long", f"{inst_id}:short"):
            self._mfe_peak.pop(k, None)
            self._mfe_hits.pop(k, None)
            self._hys_state_pp.pop(k, None); self._hys_state_lp.pop(k, None)
            self._cooldown_ts.pop(k, None)
            # 同时清理 MFE A+B 的状态
            self._mfe_last_roe.pop(k, None)
            self._mfe_gated.pop(k, None)
            self._mfe_last_peak_trig.pop(k, None)
   
        for pos in positions:
            if pos.get("instId") != inst_id:
                continue
            if side_hint in ("LONG","SHORT"):
                want = "long" if side_hint=="LONG" else "short"
                if self.pos_mode=="long_short" and (pos.get("posSide","") or "").lower()!=want:
                    continue
            curr = float(pos.get("pos", 0))
            if curr == 0:
                continue

            last_px = float(self._call_api(self.market_api.get_ticker, instId=inst_id)["data"][0]["last"])

            # 原仓方向：优先用 posSide，再回退 qty 符号
            ps_now = (pos.get("posSide") or "").lower()
            if ps_now in ("long", "short"):
                orig_side = "buy" if ps_now == "long" else "sell"
            else:
                orig_side = "buy" if curr > 0 else "sell"
            if mode == "test":
                logging.info("[TEST] Would reduce full position inst=%s size=%.8f side=%s", inst_id, abs(curr), orig_side)
            else:
                self.reduce_position(inst_id, last_px, abs(curr), orig_side)
            

    # ============================================================
    #  INFO → 风控（最大回撤/分档止损/收益保护）
    # ============================================================
    def _handle_info(self, evt: dict):
        """按 ROE 百分比执行：最大回撤硬止损、LP 分档止损，以及 PP 收益保护。"""
        positions = self.get_positions()  # 单次复用
        coin = (evt.get("coin") or "").upper()

        # RabbitMQ INFO 消息中常见字段：midPx / last / px
        mid_px = evt.get("midPx") or evt.get("last") or evt.get("px")
        if not (coin and mid_px):
            logging.info("[INFO_EVT][SKIP] missing coin/midPx: coin=%s midPx=%s", coin, str(mid_px))
            return
        try:
            mid_px = float(mid_px)
            if mid_px <= 0:
                logging.info("[INFO_EVT][SKIP] non-positive midPx=%.8f coin=%s", float(mid_px), coin)
                return
        except Exception:
            logging.info("[INFO_EVT][SKIP] midPx not float, raw=%s coin=%s", str(mid_px), coin)
            return

        inst_id = self._get_inst_id(coin)
        if not inst_id:
            return

        # 更新价格/特征缓存
        from contextlib import suppress
        with suppress(Exception):
            self._update_px_cache(inst_id, last=mid_px, ts=evt.get("ts"))
        with suppress(Exception):
            self._ingest_features_from_evt(inst_id, evt)
        with suppress(Exception):
            self._merge_feat_into_event(inst_id, evt)

        # 更新 vol.atr_pct（来自 MQ）
        try:
            if isinstance(evt.get("vol"), dict) and "atr_pct" in evt["vol"]:
                self._vol_cache[inst_id] = {"atr_pct": float(evt["vol"]["atr_pct"]), "ts": time.time()}
        except Exception:
            pass

        # === 只有存在“已成交持仓”时，才进入 PP/LP/监控等风控逻辑 ===
        has_filled = False
        try:
            for p in positions:
                if p.get("instId") != inst_id:
                    continue
                qty = abs(float(p.get("pos", 0) or 0))
                apx = float(p.get("avgPx", 0) or 0)
                if qty > 0 and apx > 0:
                    has_filled = True
                    break
        except Exception:
            has_filled = False
        if not has_filled:
            logging.debug("[INFO_EVT][SKIP][PENDING_ONLY] inst=%s coin=%s — no filled position (only pending/none); skip PP/LP/monitor",
                        inst_id, coin)
            return

        side_flag = (evt.get("side") or "-").upper()
        try:
            monitored_now = self._is_monitored(inst_id=inst_id, coin=coin)
            logging.info("[INFO_EVT] inst=%s coin=%s side=%s midPx=%.8f",
                        inst_id, coin, side_flag, float(mid_px))
        except Exception:
            pass

        # ===================== ① 最大回撤（ROE% 硬止损） ===================== #
        monitored = self._is_monitored(inst_id=inst_id, coin=coin)
        if self.MAX_DD_ROE_PCT > 0:
            for pos in positions:
                if pos.get("instId") != inst_id:
                    continue
                qty_raw = float(pos.get("pos", 0) or 0)
                qty = abs(qty_raw)
                entry_px = float(pos.get("avgPx", 0) or 0)
                if qty <= 0 or entry_px <= 0:
                    logging.info("[MAX_DD][ROE][SKIP] inst=%s qty=%.8f entry=%.8f", inst_id, qty, entry_px)
                    continue
                # 本交易所无对应 SWAP 不做强平
                if self._skip_miss_coin(coin):
                    logging.debug("[MAX_DD][ROE][SKIP] coin=%s marked as miss; skip forced reduce", coin)
                    continue
                # 方向：posSide 优先；其次 evt.side
                ps = (pos.get("posSide") or "").lower()
                if ps in ("long", "short"):
                    is_long = (ps == "long")
                else:
                    sf = (evt.get("side") or "").upper()
                    if sf in ("LONG", "SHORT"):
                        is_long = (sf == "LONG")
                    else:
                        logging.debug("[MAX_DD][ROE][SKIP] inst=%s cannot determine side via posSide/side", inst_id)
                        continue
                # 杠杆估计
                lev_est = (pos.get("lever") or pos.get("lev")
                        or evt.get("lev") or evt.get("leverage")
                        or (evt.get("snapshot", {}) or {}).get("lev")
                        or (evt.get("snapshot", {}) or {}).get("leverage") or 1)
                try:
                    lev_est = float(lev_est)
                    lev_est = 1.0 if lev_est <= 0 else lev_est
                except Exception:
                    lev_est = 1.0

                raw = (mid_px - entry_px) / entry_px
                roe_pct = (raw * 100.0 * lev_est) if is_long else (-raw * 100.0 * lev_est)
                loss_pct = max(0.0, -roe_pct)

                if loss_pct < self.MAX_DD_ROE_PCT:
                    logging.debug("[MAX_DD][ROE][SKIP] inst=%s side=%s loss=%.2f%% < thr=%.2f%%",
                                inst_id, ("LONG" if is_long else "SHORT"), loss_pct, self.MAX_DD_ROE_PCT)
                    continue

                side_param = "buy" if is_long else "sell"  # reduce_position 原向
                px = mid_px * (1 - self.ENTRY_SLIP) if is_long else mid_px * (1 + self.ENTRY_SLIP)
                logging.warning("[MAX_DD][ROE] %s %s loss=%.2f%% ≥ %.2f%% — 强制平仓 %.8f 张 @ %.4f (lev~%s)",
                                coin, "LONG" if is_long else "SHORT", loss_pct, self.MAX_DD_ROE_PCT, qty, px, lev_est)
                if mode == "test":
                    logging.info("[TEST] Would reduce_position inst=%s size=%.8f side=%s px=%.8f",
                                inst_id, qty, side_param, px)
                else:
                    self.reduce_position(inst_id, px, qty, side_param)

        # ===================== ② 止损保护（ATR 分档；仅未监控） ===================== #
        logging.debug("[LP][CHECK] monitored=%s coin=%s inst=%s midPx=%.8f", monitored, coin, inst_id, float(mid_px))
        lp_cuts_g = getattr(self, "LP_CUTS", [])
        lp_cut_ratio_g = getattr(self, "LP_CUT_RATIO", 0.0)
        if (not monitored) and self.LP_STEP_PCT > 0 and (lp_cuts_g or lp_cut_ratio_g > 0):
            for pos in positions:
                if pos.get("instId") != inst_id:
                    continue
                qty_raw = float(pos.get("pos", 0) or 0)
                qty = abs(qty_raw)
                entry_px = float(pos.get("avgPx", 0) or 0)
                if qty == 0 or entry_px == 0:
                    logging.debug("[LP][ROE][SKIP] inst=%s 无有效仓位/均价 qty=%.8f entry=%.8f", inst_id, qty, entry_px)
                    continue

                ps = (pos.get("posSide") or "").lower()
                if ps in ("long", "short"):
                    is_long = (ps == "long")
                else:
                    sf = (evt.get("side") or "").upper()
                    if sf in ("LONG", "SHORT"):
                        is_long = (sf == "LONG")
                    else:
                        logging.debug("[LP][ROE][SKIP] inst=%s cannot determine side via posSide/side", inst_id)
                        continue

                lev_est = (pos.get("lever") or pos.get("lev")
                        or evt.get("lev") or evt.get("leverage")
                        or (evt.get("snapshot", {}) or {}).get("lev")
                        or (evt.get("snapshot", {}) or {}).get("leverage") or 1)
                try:
                    lev_est = float(lev_est)
                    lev_est = 1.0 if lev_est <= 0 else lev_est
                except Exception:
                    lev_est = 1.0

                raw = (mid_px - entry_px) / entry_px
                roe_pct = (raw * 100.0 * lev_est) if is_long else (-raw * 100.0 * lev_est)
                loss_pct = max(0.0, -roe_pct)
                if loss_pct <= 0:
                    logging.debug("[LP][ROE][SKIP] non-loss inst=%s side=%s roe=%.2f%% entry=%.6f mid=%.6f",
                                inst_id, ("LONG" if is_long else "SHORT"), roe_pct, entry_px, mid_px)
                    continue

                pos_side = "long" if is_long else "short"
                key = self._pp_key(inst_id, pos_side)
                last_hit = int(self._lp_hits.get(key, 0))
                need_lvl = last_hit + 1
                logging.info("[LP][CHK] inst=%s pos=%s lossROE=%.2f%% last_hit=%d need_lvl=%d",
                            inst_id, ("LONG" if is_long else "SHORT"), loss_pct, last_hit, need_lvl)

                # 动态/固定阈值
                dyn_thr = self._lp_step_threshold(lev_est, need_lvl, inst_id)
                if dyn_thr is not None and dyn_thr > 0:
                    thr_hi = dyn_thr * (1 - self.LP_TOLERANCE_PCT / 100.0)
                else:
                    thr_hi = self.LP_STEP_PCT * need_lvl * (1 - self.LP_TOLERANCE_PCT / 100.0)
                hys_lo = thr_hi * (1 - self.HYS_BAND_PCT / 100.0)

                # 滞回 + 冷却
                hys_key = f"{key}:LP"
                state = self._hys_state_lp.get(hys_key, "armed")
                if state != "armed":
                    if loss_pct <= max(0.0, hys_lo):
                        self._hys_state_lp[hys_key] = "armed"
                    logging.info("[LP][HYS] disarmed (loss=%.2f%%, rearm<=%.2f%%)", loss_pct, hys_lo)
                    continue
                cd = self._under_cooldown(f"{hys_key}:lvl{need_lvl}")
                if loss_pct < thr_hi or cd:
                    logging.info("[LP][ROE][SKIP] below-threshold/cooldown inst=%s side=%s lossROE=%.2f%% thr=%.2f%% lvl=%d cd=%s",
                                inst_id, pos_side.upper(), loss_pct, thr_hi, need_lvl, cd)
                    continue

                if self._skip_miss_coin(coin):
                    logging.info("[LP][ROE][SKIP] coin=%s marked as miss; skip reduce", coin)
                    continue

                # 价格点去重
                last_px_trig = self._lp_last_px.get(key)
                tol_pct = (self._px_tol_pct(inst_id) or float(self.TRIGGER_PX_TOL_PCT or 0.0)) * self._lvl_factor(need_lvl)
                tol = abs(mid_px) * (tol_pct / 100.0)
                if last_px_trig is not None and abs(float(mid_px) - float(last_px_trig)) <= tol:
                    logging.info("[LP][SKIP] px-dedup inst=%s side=%s mid=%.8f last=%.8f tol=%.8f",
                                inst_id, pos_side.upper(), float(mid_px), float(last_px_trig), tol)
                    continue

                # 记录触发价
                self._lp_last_px[key] = mid_px
                self._save_loss_guard_px_state()

                # 减仓张数（≥ minSz，≤ 当前张数）
                _, _, _, min_sz = self._get_inst_spec(inst_id)
                lot_sz, min_sz2 = self._get_trade_steps(inst_id)
                min_sz = max(float(min_sz), float(min_sz2))
                curr = abs(qty)

                lp_cuts = getattr(self, "LP_CUTS", [])
                lp_cut_ratio = getattr(self, "LP_CUT_RATIO", 0.0)
                if lp_cuts and need_lvl > len(lp_cuts):
                    continue
                cut_ratio = float(lp_cuts[need_lvl - 1]) if (lp_cuts and 1 <= need_lvl <= len(lp_cuts)) else float(lp_cut_ratio)

                base_cut = self._quantize_size(curr * cut_ratio, lot_sz, "down")
                min_cut = self._quantize_size(min_sz, lot_sz, "up")
                cut_contracts = max(min_cut, base_cut)
                cut_contracts = min(cut_contracts, self._quantize_size(curr, lot_sz, "down"))

                # 保留仓位护栏
                if curr < 2.0 * float(min_sz):
                    logging.info("[LP][KEEP_MIN][SKIP] inst=%s side=%s curr=%.6f < 2*minSz=%.6f — skip reduce",
                                inst_id, pos_side.upper(), curr, 2.0 * float(min_sz))
                    self._hys_state_lp[hys_key] = "armed"
                    continue
                max_cut = self._quantize_size(max(0.0, curr - float(min_sz)), lot_sz, "down")
                if cut_contracts > max_cut:
                    cut_contracts = max_cut
                if cut_contracts < float(min_sz):
                    logging.info("[LP][KEEP_MIN][SKIP] inst=%s side=%s cut<minSz after guard (cut=%s, minSz=%.6f)",
                                inst_id, pos_side.upper(), cut_contracts, float(min_sz))
                    self._hys_state_lp[hys_key] = "armed"
                    continue
                if cut_contracts <= 0:
                    logging.debug("[LP][SKIP] cut<=0 inst=%s curr=%.8f minSz=%.8f cut_ratio=%.2f%% base_cut=%s",
                                inst_id, curr, min_sz, self.LP_CUT_RATIO * 100, base_cut)
                    continue

                side_param = "buy" if pos_side == "long" else "sell"
                px = mid_px
                self._hys_state_lp[hys_key] = "disarmed"
                self._mark_cooldown(f"{hys_key}:lvl{need_lvl}")

                logging.warning("[LP][ROE] %s %s lossROE=%.2f%% 达到第 %d 档(上次=%d) → 减仓 %s 张 (lev~%s)",
                                inst_id, pos_side.upper(), loss_pct, need_lvl, last_hit, cut_contracts, lev_est)
                logging.info("[LP][CUT] inst=%s side=%s curr=%.6f minSz=%.6f cut_ratio=%.2f%% -> cut=%s px=%.8f",
                            inst_id, pos_side.upper(), curr, min_sz, self.LP_CUT_RATIO * 100, cut_contracts, px)
                if mode == "test":
                    logging.info("[TEST][LP] Would reduce_position inst=%s size=%s side=%s px=%.8f",
                                inst_id, cut_contracts, side_param, px)
                else:
                    self.reduce_position(inst_id, px, cut_contracts, side_param)

                self._lp_hits[key] = need_lvl
                self._save_loss_guard_state()

        # ===================== ③ 收益保护（ATR 分档；仅未监控） ===================== #
        _pp_cuts_on = bool(self.PP_CUTS) or (self.PP_CUT_RATIO > 0)
        dyn_probe = None
        try:
            lev_est_probe = (evt.get("lev") or (evt.get("snapshot", {}) or {}).get("lev") or 1)
            dyn_probe = self._pp_step_threshold(float(lev_est_probe or 1), 1, inst_id)
        except Exception:
            dyn_probe = None

        if (not monitored) and _pp_cuts_on and (self.PP_STEP_PCT > 0 or (dyn_probe and dyn_probe > 0)):
            snap = evt.get("snapshot", {}) or {}
            roe_pct = None
            try:
                unreal = snap.get("unrealizedPnl")
                margin = snap.get("marginUsed")
                if unreal is not None and margin not in (None, "", 0, "0"):
                    roe_pct = 100.0 * float(unreal) / max(1e-12, float(margin))
                else:
                    roe_val = snap.get("returnOnEquity")
                    if roe_val is not None:
                        r = float(roe_val)
                        roe_pct = r * 100.0 if abs(r) <= 10 else r
            except Exception:
                roe_pct = None

            if roe_pct is None:
                try:
                    entry_px = float(snap.get("entryPx") or 0)
                    lev_est = snap.get("lev") or snap.get("leverage") or evt.get("lev")
                    lev_est = float(lev_est) if lev_est is not None else 1.0
                    side_f = (evt.get("side") or "-").upper()
                    if entry_px > 0 and mid_px > 0 and side_f in ("LONG", "SHORT"):
                        raw = (mid_px - entry_px) / entry_px
                        roe_pct = raw * 100.0 * (lev_est if side_f == "LONG" else -lev_est)
                except Exception:
                    roe_pct = None

            if roe_pct is None or roe_pct <= 0:
                logging.info("[PP][SKIP] roe<=0 or missing (coin=%s inst=%s side=%s)", coin, inst_id, side_flag)
                return

            side_flag = (evt.get("side") or "-").upper()
            pos_side = "long" if side_flag == "LONG" else ("short" if side_flag == "SHORT" else None)
            if not pos_side:
                return

            key = self._pp_key(inst_id, pos_side)
            last_hit = int(self._pp_hits.get(key, 0))
            need_lvl = last_hit + 1

            try:
                lev_est = (evt.get("lev") or (evt.get("snapshot", {}) or {}).get("lev") or 1)
                dyn_thr = self._pp_step_threshold(float(lev_est or 1), need_lvl, inst_id)
            except Exception:
                dyn_thr = None

            if dyn_thr and dyn_thr > 0:
                thr_hi = dyn_thr * (1 - self.PP_TOLERANCE_PCT / 100.0)
                logging.info("[PP][THR] mode=ATR thr=%.2f%% lvl=%d", thr_hi, need_lvl)
            elif self.PP_STEP_PCT > 0:
                thr_hi = self.PP_STEP_PCT * need_lvl * (1 - self.PP_TOLERANCE_PCT / 100.0)
                logging.info("[PP][THR] mode=FIXED thr=%.2f%% lvl=%d", thr_hi, need_lvl)
            else:
                logging.info("[PP][THR] mode=OFF reason=no-atr-and-fixed-step=0")
                return

            # 滞回 + 冷却
            hys_key = f"{key}:PP"
            hys_lo = thr_hi * (1 - self.HYS_BAND_PCT / 100.0)
            state = self._hys_state_pp.get(hys_key, "armed")
            if state != "armed":
                if roe_pct <= max(0.0, hys_lo):
                    self._hys_state_pp[hys_key] = "armed"
                logging.debug("[PP][HYS] disarmed (roe=%.2f%%, rearm<=%.2f%%)", roe_pct, hys_lo)
                return
            cd = self._under_cooldown(f"{hys_key}:lvl{need_lvl}")
            if roe_pct < thr_hi or cd:
                logging.info("[PP][SKIP] below-threshold/cooldown inst=%s side=%s roe=%.2f%% thr=%.2f%% lvl=%d cd=%s",
                            inst_id, side_flag, roe_pct, thr_hi, need_lvl, cd)
                return

            # 价格点去重
            last_px_trig = self._pp_last_px.get(key)
            tol_pct = (self._px_tol_pct(inst_id) or float(self.TRIGGER_PX_TOL_PCT or 0.0)) * self._lvl_factor(need_lvl)
            tol = abs(mid_px) * (tol_pct / 100.0)
            if last_px_trig is not None and abs(mid_px - last_px_trig) <= tol:
                logging.info("[PP][DEDUP] mid=%.8f last_px_trig=%.8f tol%%=%.3f tol_abs=%.8f",
                            float(mid_px), float(last_px_trig), tol_pct, tol)
                return

            # 当前方向仓位
            curr = 0.0
            for p in positions:
                if p.get("instId") != inst_id:
                    continue
                if self.pos_mode == "long_short" and (p.get("posSide", "") or "").lower() != pos_side:
                    continue
                curr = abs(float(p.get("pos", 0) or 0))
                break
            if curr <= 0:
                logging.info("[PP][SKIP][NO_FILLED] inst=%s side=%s no filled position; skip without updating hits/state",
                            inst_id, pos_side.upper())
                return

            # 记录触发价
            self._pp_last_px[key] = mid_px
            self._save_profit_guard_px_state()

            # 计算减仓
            _, _, _, min_sz = self._get_inst_spec(inst_id)
            lot_sz, min_sz2 = self._get_trade_steps(inst_id)
            min_sz = max(float(min_sz), float(min_sz2))
            cut_ratio = float(self.PP_CUTS[min(len(self.PP_CUTS) - 1, max(0, need_lvl - 1))]) if self.PP_CUTS else float(self.PP_CUT_RATIO)
            cut_contracts = max(
                self._quantize_size(min_sz, lot_sz, "up"),
                self._quantize_size(curr * cut_ratio, lot_sz, "down")
            )
            side_param = "buy" if pos_side == "long" else "sell"
            px = mid_px * (1 - self.ENTRY_SLIP) if pos_side == "long" else mid_px * (1 + self.ENTRY_SLIP)

            # 冷却 + 滞回出栈
            self._hys_state_pp[hys_key] = "disarmed"
            self._mark_cooldown(f"{hys_key}:lvl{need_lvl}")

            logging.info("[PP] %s %s ROE=%.2f%% 达到第 %d 档(>%d) → 减仓 %.8f 张",
                        inst_id, pos_side.upper(), roe_pct, need_lvl, last_hit, cut_contracts)
            if mode == "test":
                logging.info("[TEST][PP] Would reduce_position inst=%s size=%s side=%s px=%.8f",
                            inst_id, cut_contracts, side_param, px)
            else:
                self.reduce_position(inst_id, px, cut_contracts, side_param)

            self._pp_hits[key] = need_lvl
            self._save_profit_guard_state()

        # ===================== ⑤ 多时段 HVN/LVN 保护（仅已监控） ===================== #
        if monitored:
            for pos_side in self._pos_sides_with_exposure(inst_id, positions):
                key = f"{inst_id}:{pos_side}"
                lvl = self._vp_levels.get(key) or {}

                # 检测是否存在多时段 VP
                prof = evt.get("profile") or {}
                has_multi = bool(
                    evt.get("profile_multi")
                    or prof.get("multi")
                    or prof.get("volume_multi")
                    or ((evt.get("feat") or {}).get("profile_multi"))
                )
                logging.info("[NODE][HAS_MULTI] inst=%s pos=%s top_pm=%s feat_pm=%s",
                            inst_id, pos_side.upper(),
                            isinstance(evt.get("profile_multi"), dict),
                            isinstance((evt.get("feat") or {}).get("profile_multi"), dict))

                # 先尝试 RG 评估（失败也不影响后续兜底）
                tmp = {}
                if has_multi:
                    lev_est_rg = (evt.get("lev") or evt.get("leverage")
                                or (evt.get("snapshot", {}) or {}).get("lev")
                                or (evt.get("snapshot", {}) or {}).get("leverage") or 1)
                    evt2 = dict(evt)
                    evt2["side"] = "LONG" if pos_side == "long" else "SHORT"

                    # entry_for_side：snapshot 优先 → 本地持仓（无 posSide 用数量符号判方向）
                    try:
                        entry_for_side = float(((evt.get("snapshot") or {}).get("entryPx") or 0) or 0)
                    except Exception:
                        entry_for_side = 0.0
                    if entry_for_side <= 0:
                        for p in positions:
                            if p.get("instId") != inst_id:
                                continue
                            qty_raw = float(p.get("pos") or 0)
                            ps = (p.get("posSide") or "").lower()
                            if ps:
                                if ps != pos_side:
                                    continue
                            else:
                                if pos_side == "long" and qty_raw <= 0:
                                    continue
                                if pos_side == "short" and qty_raw >= 0:
                                    continue
                            apx = float(p.get("avgPx") or 0)
                            if apx > 0:
                                entry_for_side = apx
                                break
                    if entry_for_side > 0:
                        evt2["entry_px"] = entry_for_side
                        logging.info("[NODE][ENTRY][RESOLVE] inst=%s pos=%s entry_px=%.8f (source=%s)",
                                    inst_id, pos_side.upper(), entry_for_side,
                                    "snapshot" if ((evt.get('snapshot') or {}).get('entryPx') or 0) else "local")
                    try:
                        tmp = quick_eval_from_event(
                            evt2,
                            leverage=float(lev_est_rg or 1),
                            min_roe_gap_pct=self.RG_MIN_ROE_GAP_PCT,
                            node_min_roe_pct=self.RG_MIN_ROE_GAP_PCT,
                        ) or {}
                    except Exception as e:
                        logging.error("[NODE][RG][FAIL] inst=%s pos=%s err=%s", inst_id, pos_side.upper(), repr(e))
                        tmp = {}

                    # 打印 RG 结果（尽量不抛异常）
                    try:
                        g = (tmp or {}).get("guards") or {}
                        nodes_dbg = (tmp or {}).get("nodes") or {}
                        logging.info("[NODE][RG] inst=%s pos=%s entry_adj=%.8f tp=%d sl=%d profit_ref=%.8f loss_ref=%.8f",
                                    inst_id, pos_side.upper(),
                                    float((tmp or {}).get("entry_adj") or 0.0),
                                    len((nodes_dbg or {}).get("tp_nodes") or []),
                                    len((nodes_dbg or {}).get("sl_nodes") or []),
                                    float(g.get("profit_protect_ref") or 0.0),
                                    float(g.get("loss_protect_ref") or 0.0))
                    except Exception:
                        pass

                    nodes = ((tmp or {}).get("nodes") or {})
                    ntp = [float(x) for x in (nodes.get("tp_nodes") or [])]
                    nsl = [float(x) for x in (nodes.get("sl_nodes") or [])]
                    if ntp:
                        self._vp_levels.setdefault(key, {}).update({"tp_nodes": ntp})
                        self._node_tp_hits[key] = min(self._node_tp_hits.get(key, 0), len(ntp))
                    if nsl:
                        self._vp_levels.setdefault(key, {}).update({"sl_nodes": nsl})
                        self._node_sl_hits[key] = min(self._node_sl_hits.get(key, 0), len(nsl))
                    if isinstance((tmp or {}).get("guards"), dict) and (tmp or {}).get("guards"):
                        self._vp_levels.setdefault(key, {}).update({"guards": (tmp or {}).get("guards")})
                    entry_adj_new = float((tmp or {}).get("entry_adj") or 0.0)
                    if entry_adj_new > 0:
                        self._vp_levels.setdefault(key, {}).update({"entry_adj": entry_adj_new})

                # ✅ 无论 RG 是否成功，都兜底设置 entry_adj（这段不放 try 里，确保必执行）
                if not (self._vp_levels.get(key, {}) or {}).get("entry_adj"):
                    entry_px = float((evt.get("snapshot", {}) or {}).get("entryPx") or 0)
                    if entry_px > 0:
                        self._vp_levels.setdefault(key, {}).update({"entry_adj": entry_px})
                        logging.info("[NODE][ENTRY][SNAP] inst=%s pos=%s entry_adj=%.8f", inst_id, pos_side.upper(), entry_px)
                    else:
                        for p in positions:
                            if p.get("instId") != inst_id:
                                continue
                            ps = (p.get("posSide") or "").lower()
                            qty_raw = float(p.get("pos") or 0)
                            # 有 posSide 则精确匹配；无 posSide 用数量符号判方向
                            if ps:
                                if ps != pos_side:
                                    continue
                            else:
                                if pos_side == "long" and qty_raw <= 0:
                                    continue
                                if pos_side == "short" and qty_raw >= 0:
                                    continue
                            apx = float(p.get("avgPx") or 0)
                            if apx > 0:
                                self._vp_levels.setdefault(key, {}).update({"entry_adj": apx})
                                logging.info("[NODE][ENTRY][LOCAL] inst=%s pos=%s entry_adj=%.8f", inst_id, pos_side.upper(), apx)
                                break
                if not (self._vp_levels.get(key, {}) or {}).get("entry_adj"):
                    logging.warning("[NODE][ENTRY][MISSING] inst=%s pos=%s entry_adj still 0 (snap.entryPx=%s, local.avgPx tried)",
                                    inst_id, pos_side.upper(), (evt.get('snapshot') or {}).get('entryPx'))

                # 刷新后重新绑定 lvl
                lvl = self._vp_levels.get(key) or lvl

                # ---- 读取节点 & 执行风控 ----
                def _safe_floats(xs):
                    out = []
                    for x in (xs or []):
                        try:
                            out.append(float(x))
                        except (TypeError, ValueError):
                            pass
                    return out

                tp_nodes = _safe_floats(lvl.get("tp_nodes"))
                sl_nodes = _safe_floats(lvl.get("sl_nodes"))

                if tp_nodes or sl_nodes or self.USE_ATR_FALLBACK:
                    # 当前方向持仓张数
                    curr = 0.0
                    for p in positions:
                        if p.get("instId") != inst_id:
                            continue
                        if self.pos_mode == "long_short" and (p.get("posSide", "") or "").lower() != pos_side:
                            continue
                        curr = abs(float(p.get("pos", 0) or 0))
                        break
                    if curr > 0:
                        _, _, _, min_sz = self._get_inst_spec(inst_id)

                        entry_adj = float((lvl.get("entry_adj") or 0.0))
                        if entry_adj <= 0:
                            logging.info("[NODE] inst=%s pos=%s missing/zero entry_adj — skip HVN/LVN node triggers this tick; keep VA/ATR fallbacks",
                                        inst_id, pos_side.upper())
                            tp_nodes = []
                            sl_nodes = []

                        # 杠杆估计（持仓→evt/snapshot→1）
                        try:
                            lev_est = next(
                                (float(p.get("lever") or p.get("lev") or 0) or 0)
                                for p in positions
                                if p.get("instId") == inst_id and (self.pos_mode != "long_short" or (p.get("posSide", "") or "").lower() == pos_side)
                            )
                        except StopIteration:
                            lev_est = 0.0
                        except Exception:
                            lev_est = 0.0
                        if lev_est <= 0:
                            try:
                                lev_est = float(
                                    evt.get("lev") or evt.get("leverage")
                                    or (evt.get("snapshot", {}) or {}).get("lev")
                                    or (evt.get("snapshot", {}) or {}).get("leverage") or 1.0
                                )
                            except Exception:
                                lev_est = 1.0

                        # 节点最小 ROE 距离：
                        # - HVN(盈利)：用 min_roe_gap_pct
                        # - LVN(亏损)：用 loss_step_pct（未配置则回退到 min_roe_gap_pct）
                        min_gap_tp = float(self.RG_MIN_ROE_GAP_PCT or 30.0)
                        min_gap_sl = float(self.LP_STEP_PCT or self.RG_MIN_ROE_GAP_PCT or 30.0)
                        hvn_seq = self._select_nodes_with_min_roe_gap(tp_nodes, entry_adj, pos_side, "tp",  lev_est, min_gap_tp)
                        lvn_seq = self._select_nodes_with_min_roe_gap(sl_nodes, entry_adj, pos_side, "sl",  lev_est, min_gap_sl)
 
                        logging.info("[NODE][RISK_ZONE] inst=%s pos=%s monitored=%s entry_adj=%.8f lev~%.2f "
                                     "min_gap_tp=%.2f min_gap_sl=%.2f hvn_cnt=%d lvn_cnt=%d",
                                     inst_id, pos_side.upper(), True, entry_adj, lev_est,
                                     min_gap_tp, min_gap_sl, len(hvn_seq), len(lvn_seq))
                        # === 新增：阶梯缺口合成 ===
                        hvn_orig = list(hvn_seq)
                        lvn_orig = list(lvn_seq)
                        if self.NODE_SYNTH_ENABLE and entry_adj > 0 and lev_est > 0:
                            max_lvls = max(1, int(self.NODE_SYNTH_MAX_LEVELS or 5))
                            # 与入场价的 ROE（正=盈利，负=亏损）
                            raw_from_entry = (mid_px - entry_adj) / entry_adj
                            roe_from_entry = (raw_from_entry * 100.0 * lev_est) if pos_side == "long" else (-raw_from_entry * 100.0 * lev_est)
                            prof_roe = max(0.0, float(roe_from_entry))
                            loss_roe = max(0.0, -float(roe_from_entry))
                            # “差 < tol pp 即视为到达此档”
                            tp_reached = int(math.floor((prof_roe + float(self.NODE_SYNTH_TOL_PCT or 0.0)) / max(1e-9, float(min_gap_tp or 0.0)))) if min_gap_tp > 0 else 0
                            sl_reached = int(math.floor((loss_roe + float(self.NODE_SYNTH_TOL_PCT or 0.0)) / max(1e-9, float(min_gap_sl or 0.0)))) if min_gap_sl > 0 else 0
                            tp_reached = min(max_lvls, tp_reached)
                            sl_reached = min(max_lvls, sl_reached)
                            # 仅当“已有≥1个真实节点”且“数量小于已达档位”时才补点（贴合你的场景）
                            if len(hvn_orig) >= 1 and len(hvn_seq) < tp_reached:
                                for n in range(len(hvn_seq)+1, tp_reached+1):
                                    # 目标 ROE = n*gap - tol（保证“只差 <5pp 也触发”）
                                    roe_need = max(0.0, n * float(min_gap_tp) - float(self.NODE_SYNTH_TOL_PCT or 0.0))
                                    px = entry_adj * (1.0 + (roe_need/100.0)/float(lev_est)) if pos_side == "long" else entry_adj * (1.0 - (roe_need/100.0)/float(lev_est))
                                    hvn_seq.append(float(px))
                            if len(lvn_orig) >= 1 and len(lvn_seq) < sl_reached:
                                for n in range(len(lvn_seq)+1, sl_reached+1):
                                    roe_need = max(0.0, n * float(min_gap_sl) - float(self.NODE_SYNTH_TOL_PCT or 0.0))
                                    px = entry_adj * (1.0 - (roe_need/100.0)/float(lev_est)) if pos_side == "long" else entry_adj * (1.0 + (roe_need/100.0)/float(lev_est))
                                    lvn_seq.append(float(px))
                            # 最多 5 档
                            hvn_seq = hvn_seq[:max_lvls]
                            lvn_seq = lvn_seq[:max_lvls]
                            if len(hvn_seq) > len(hvn_orig) or len(lvn_seq) > len(lvn_orig):
                                logging.info("[NODE][SYNTH] inst=%s pos=%s hvn: %d -> %d  lvn: %d -> %d (tol=%.2fpp, max=%d)",
                                             inst_id, pos_side.upper(), len(hvn_orig), len(hvn_seq), len(lvn_orig), len(lvn_seq),
                                             float(self.NODE_SYNTH_TOL_PCT or 0.0), max_lvls)
                        if len(lvn_seq) == 0:
                            guards = (lvl.get("guards") or {})
                            loss_ref = float(guards.get("loss_protect_ref") or 0.0)
                            logging.info("[NODE][LVN][REASON] inst=%s pos=%s LVN=0 → 原因=无 LVN 回退 (use_atr_fallback=%s, loss_ref=%.8f)",
                                        inst_id, pos_side.upper(), self.USE_ATR_FALLBACK, loss_ref)

                        # 节点比例：真实节点按 NODE_*_CUTS；“补档”统一默认 30% 减仓
                        if len(hvn_seq) > 0:
                            base_len = min(len(hvn_orig), len(hvn_seq))
                            if self.NODE_TP_CUTS and base_len > 0:
                                base_rates = [float(self.NODE_TP_CUTS[min(i, len(self.NODE_TP_CUTS)-1)]) for i in range(base_len)]
                            elif base_len > 0:
                                base_rates = [float(self.PP_CUT_RATIO) / float(base_len)] * base_len if self.PP_CUT_RATIO > 0 else []
                            else:
                                base_rates = []
                            extra_len = max(0, len(hvn_seq) - base_len)
                            extra_rates = [float(self.NODE_SYNTH_CUT_RATIO)] * extra_len
                            tp_rates = base_rates + extra_rates
                        else:
                            tp_rates = []
                        if len(lvn_seq) > 0:
                            base_len_sl = min(len(lvn_orig), len(lvn_seq))
                            if self.NODE_SL_CUTS and base_len_sl > 0:
                                base_rates_sl = [float(self.NODE_SL_CUTS[min(i, len(self.NODE_SL_CUTS)-1)]) for i in range(base_len_sl)]
                            elif base_len_sl > 0:
                                base_rates_sl = [float(self.LP_CUT_RATIO) / float(base_len_sl)] * base_len_sl if self.LP_CUT_RATIO > 0 else []
                            else:
                                base_rates_sl = []
                            extra_len_sl = max(0, len(lvn_seq) - base_len_sl)
                            extra_rates_sl = [float(self.NODE_SYNTH_CUT_RATIO)] * extra_len_sl
                            sl_rates = base_rates_sl + extra_rates_sl
                        else:
                            sl_rates = []

                        # ---- HVN：盈利方向分级减仓 ----
                        if len(hvn_seq) > 0 and tp_rates:
                            tp_hits = min(int(self._node_tp_hits.get(key, 0)), len(hvn_seq))
                            while tp_hits < len(hvn_seq):
                                thr = float(hvn_seq[tp_hits])
                                st_atr = self._get_atr_state(inst_id) or {}
                                atr_pct = float(st_atr.get("atr_pct") or 0.0)
                                extra_pct = max(self.NODE_CONFIRM_MIN_PCT, self.NODE_CONFIRM_K_ATR * max(0.0, atr_pct))
                                hi_thr = thr * (1 + extra_pct / 100.0)
                                lo_thr = thr * (1 - extra_pct / 100.0)
                                hit = (mid_px >= hi_thr) if pos_side == "long" else (mid_px <= lo_thr)
                                if self._under_cooldown(f"NODE:TP:{key}:{tp_hits+1}"):
                                    break
                                if not hit:
                                    break
                                lot_sz, min_sz2 = self._get_trade_steps(inst_id)
                                min_sz = max(float(min_sz), float(min_sz2))
                                cut_contracts = max(
                                    self._quantize_size(curr * float(tp_rates[tp_hits]), lot_sz, "down"),
                                    self._quantize_size(min_sz, lot_sz, "up")
                                )

                                # 保留仓位护栏
                                if curr < 2.0 * float(min_sz):
                                    logging.info("[NODE][LVN][KEEP_MIN][SKIP] inst=%s %s curr=%.6f < 2*minSz=%.6f — skip reduce",
                                                inst_id, pos_side.upper(), curr, 2.0 * float(min_sz))
                                    break
                                max_cut = self._quantize_size(max(0.0, curr - float(min_sz)), lot_sz, "down")
                                cut_contracts = min(cut_contracts, max_cut)
                                if cut_contracts < float(min_sz):
                                    logging.info("[NODE][LVN][KEEP_MIN][SKIP] inst=%s %s cut<minSz after guard (cut=%s, minSz=%.6f)",
                                                inst_id, pos_side.upper(), cut_contracts, float(min_sz))
                                    break

                                side_param = "buy" if pos_side == "long" else "sell"
                                px = float(mid_px)  # 节点不加滑点
                                logging.info("[NODE][HVN][TP] inst=%s %s hit HVN%d/%d @%.8f → reduce %.8f (each=%.2f%%, lev~%.2f)",
                                            inst_id, pos_side.upper(), tp_hits + 1, len(hvn_seq), thr, cut_contracts, float(tp_rates[tp_hits]) * 100, lev_est)
                                if mode == "test":
                                    logging.info("[TEST][NODE][TP] Would reduce_position inst=%s size=%s side=%s px=%.8f",
                                                inst_id, cut_contracts, side_param, px)
                                else:
                                    self.reduce_position(inst_id, px, cut_contracts, side_param)
                                tp_hits += 1
                                self._mark_cooldown(f"NODE:TP:{key}:{tp_hits}")
                                self._node_tp_hits[key] = tp_hits
                                curr = max(0.0, curr - cut_contracts)
                                if curr <= 0:
                                    break

                        # ---- LVN：亏损方向分级减仓 ----
                        if len(lvn_seq) > 0 and sl_rates and curr > 0:
                            sl_hits = min(int(self._node_sl_hits.get(key, 0)), len(lvn_seq))
                            while sl_hits < len(lvn_seq):
                                thr = float(lvn_seq[sl_hits])
                                st_atr = self._get_atr_state(inst_id) or {}
                                atr_pct = float(st_atr.get("atr_pct") or 0.0)
                                extra_pct = max(self.NODE_CONFIRM_MIN_PCT, self.NODE_CONFIRM_K_ATR * max(0.0, atr_pct))
                                hi_thr = thr * (1 + extra_pct / 100.0)
                                lo_thr = thr * (1 - extra_pct / 100.0)
                                if pos_side == "long":
                                    guard_ok = (mid_px <= entry_adj) and (mid_px <= lo_thr)
                                else:
                                    guard_ok = (mid_px >= entry_adj) and (mid_px >= hi_thr)
                                if self._under_cooldown(f"NODE:SL:{key}:{sl_hits+1}"):
                                    break
                                if not guard_ok:
                                    break
                                lot_sz, min_sz2 = self._get_trade_steps(inst_id)
                                min_sz = max(float(min_sz), float(min_sz2))
                                cut_contracts = max(
                                    self._quantize_size(curr * float(sl_rates[sl_hits]), lot_sz, "down"),
                                    self._quantize_size(min_sz, lot_sz, "up")
                                )
                                side_param = "buy" if pos_side == "long" else "sell"
                                px = float(mid_px)
                                logging.warning("[NODE][LVN][SL] inst=%s %s hit LVN%d/%d @%.8f → reduce %.8f (each=%.2f%%, lev~%.2f)",
                                                inst_id, pos_side.upper(), sl_hits + 1, len(lvn_seq), thr, cut_contracts, float(sl_rates[sl_hits]) * 100, lev_est)
                                if mode == "test":
                                    logging.info("[TEST][NODE][SL] Would reduce_position inst=%s size=%s side=%s px=%.8f",
                                                inst_id, cut_contracts, side_param, px)
                                else:
                                    self.reduce_position(inst_id, px, cut_contracts, side_param)
                                sl_hits += 1
                                self._mark_cooldown(f"NODE:SL:{key}:{sl_hits}")
                                self._node_sl_hits[key] = sl_hits
                                curr = max(0.0, curr - cut_contracts)
                                if curr <= 0:
                                    break

                        # ---- VA/ATR 回退 ----
                        guards = (lvl.get("guards") or {})
                        profit_ref = float(guards.get("profit_protect_ref") or 0.0)
                        loss_ref = float(guards.get("loss_protect_ref") or 0.0)

                        # VA 盈利回退（无 HVN）
                        if curr > 0 and len(hvn_seq) == 0 and profit_ref > 0:
                            st_atr = self._get_atr_state(inst_id) or {}
                            atr_pct = float(st_atr.get("atr_pct") or 0.0)
                            extra_pct = max(self.NODE_CONFIRM_MIN_PCT, self.NODE_CONFIRM_K_ATR * max(0.0, atr_pct))
                            hi_thr = profit_ref * (1 + extra_pct / 100.0)
                            lo_thr = profit_ref * (1 - extra_pct / 100.0)
                            hit_ref = (mid_px >= hi_thr) if pos_side == "long" else (mid_px <= lo_thr)
                            if hit_ref and not self._under_cooldown(f"NODE:TP:VA-FB:{key}:1"):
                                k_px = self._pp_key(inst_id, pos_side)
                                tol_pct = (self._px_tol_pct(inst_id) or float(self.TRIGGER_PX_TOL_PCT or 0.0)) * self._lvl_factor(1)
                                tol = abs(mid_px) * (tol_pct / 100.0)
                                last_px_trig = self._pp_last_px.get(k_px)
                                if (last_px_trig is None) or abs(float(mid_px) - float(last_px_trig)) > tol:
                                    lot_sz, min_sz2 = self._get_trade_steps(inst_id)
                                    min_sz = max(float(min_sz), float(min_sz2))
                                    cut_contracts = max(
                                        self._quantize_size(curr * (self.PP_CUTS[0] if self.PP_CUTS else self.PP_CUT_RATIO), lot_sz, "down"),
                                        self._quantize_size(min_sz, lot_sz, "up")
                                    )
                                    side_param = "buy" if pos_side == "long" else "sell"
                                    px = float(mid_px)
                                    logging.info("[NODE][VA-FALLBACK][TP] inst=%s %s cross %.8f (ref=%.8f, band=±%.2f%%) → reduce %s",
                                                inst_id, pos_side.upper(), mid_px, profit_ref, extra_pct, cut_contracts)
                                    if mode == "test":
                                        logging.info("[TEST][NODE][VA-FB][TP] Would reduce_position inst=%s size=%s side=%s px=%.8f",
                                                    inst_id, cut_contracts, side_param, px)
                                    else:
                                        self.reduce_position(inst_id, px, cut_contracts, side_param)
                                    curr = max(0.0, curr - cut_contracts)
                                    self._pp_last_px[k_px] = mid_px
                                    self._save_profit_guard_px_state()
                                    self._mark_cooldown(f"NODE:TP:VA-FB:{key}:1")

                        # VA 亏损回退（无 LVN）
                        if self.USE_ATR_FALLBACK and curr > 0 and len(lvn_seq) == 0 and loss_ref > 0:
                            st_atr = self._get_atr_state(inst_id) or {}
                            atr_pct = float(st_atr.get("atr_pct") or 0.0)
                            extra_pct = max(self.NODE_CONFIRM_MIN_PCT, self.NODE_CONFIRM_K_ATR * max(0.0, atr_pct))
                            hi_thr = loss_ref * (1 + extra_pct / 100.0)
                            lo_thr = loss_ref * (1 - extra_pct / 100.0)
                            hit_ref = (mid_px <= lo_thr) if pos_side == "long" else (mid_px >= hi_thr)
                            if hit_ref and not self._under_cooldown(f"NODE:SL:VA-FB:{key}:1"):
                                k_px = self._pp_key(inst_id, pos_side)
                                tol_pct = (self._px_tol_pct(inst_id) or float(self.TRIGGER_PX_TOL_PCT or 0.0)) * self._lvl_factor(1)
                                tol = abs(mid_px) * (tol_pct / 100.0)
                                last_px_trig = self._lp_last_px.get(k_px)
                                if (last_px_trig is None) or abs(float(mid_px) - float(last_px_trig)) > tol:
                                    lot_sz, min_sz2 = self._get_trade_steps(inst_id)
                                    min_sz = max(float(min_sz), float(min_sz2))
                                    cut_contracts = max(
                                        self._quantize_size(curr * (self.LP_CUTS[0] if self.LP_CUTS else self.LP_CUT_RATIO), lot_sz, "down"),
                                        self._quantize_size(min_sz, lot_sz, "up")
                                    )
                                    # 保留仓位护栏
                                    if curr < 2.0 * float(min_sz):
                                        logging.info("[NODE][VA-FB][SL][KEEP_MIN][SKIP] inst=%s %s curr=%.6f < 2*minSz=%.6f",
                                                    inst_id, pos_side.upper(), curr, 2.0 * float(min_sz))
                                    else:
                                        max_cut = self._quantize_size(max(0.0, curr - float(min_sz)), lot_sz, "down")
                                        cut_contracts = min(cut_contracts, max_cut)
                                        if cut_contracts < float(min_sz):
                                            logging.info("[NODE][VA-FB][SL][KEEP_MIN][SKIP] inst=%s %s cut<minSz after guard (cut=%s, minSz=%.6f)",
                                                        inst_id, pos_side.upper(), cut_contracts, float(min_sz))
                                        else:
                                            side_param = "buy" if pos_side == "long" else "sell"
                                            px = float(mid_px)
                                            logging.warning("[NODE][VA-FALLBACK][SL] inst=%s %s cross %.8f (ref=%.8f, band=±%.2f%%) → reduce %s | 原因=无 LVN 回退",
                                                            inst_id, pos_side.upper(), mid_px, loss_ref, extra_pct, cut_contracts)
                                            if mode != "test":
                                                self.reduce_position(inst_id, px, cut_contracts, side_param)
                                            self._lp_last_px[k_px] = mid_px
                                            self._save_loss_guard_px_state()
                                            self._mark_cooldown(f"NODE:SL:VA-FB:{key}:1")

                        # ATR 回退（需要 entry_adj 才能估算 ROE）
                        if entry_adj > 0 and curr > 0:
                            try:
                                raw = (mid_px - entry_adj) / entry_adj
                                roe_est = raw * 100.0 * (lev_est if pos_side == "long" else -lev_est)
                            except Exception:
                                roe_est = 0.0
                        else:
                            roe_est = 0.0

                        # ATR PP 回退（无 HVN）
                        _pp_cuts_on_fb = bool(self.PP_CUTS) or (self.PP_CUT_RATIO > 0)
                        if len(hvn_seq) == 0 and _pp_cuts_on_fb and roe_est > 0:
                            kfb = self._pp_key(inst_id, pos_side)
                            last = int(self._pp_hits.get(kfb, 0))
                            need_lvl = last + 1
                            dyn_thr = self._pp_step_threshold(float(lev_est or 1), need_lvl, inst_id)
                            threshold = None
                            if dyn_thr and dyn_thr > 0:
                                threshold = dyn_thr * (1 - self.PP_TOLERANCE_PCT / 100.0)
                                logging.info("[NODE][ATR-FB][PP][THR] mode=ATR thr=%.2f%% lvl=%d", threshold, need_lvl)
                            elif self.PP_STEP_PCT > 0:
                                threshold = self.PP_STEP_PCT * need_lvl * (1 - self.PP_TOLERANCE_PCT / 100.0)
                                logging.info("[NODE][ATR-FB][PP][THR] mode=FIXED thr=%.2f%% lvl=%d", threshold, need_lvl)

                            if (threshold is not None) and roe_est >= threshold and not self._under_cooldown(f"{kfb}:PP:FB:lvl{need_lvl}"):
                                tol_pct = (self._px_tol_pct(inst_id) or float(self.TRIGGER_PX_TOL_PCT or 0.0)) * self._lvl_factor(need_lvl)
                                tol = abs(mid_px) * (tol_pct / 100.0)
                                last_px_trig = self._pp_last_px.get(kfb)
                                if (last_px_trig is None) or abs(float(mid_px) - float(last_px_trig)) > tol:
                                    lot_sz, min_sz2 = self._get_trade_steps(inst_id)
                                    min_sz = max(float(min_sz), float(min_sz2))
                                    cut_ratio_fb = float(self.PP_CUTS[min(len(self.PP_CUTS) - 1, max(0, need_lvl - 1))]) if self.PP_CUTS else float(self.PP_CUT_RATIO)
                                    cut_contracts = max(
                                        self._quantize_size(curr * cut_ratio_fb, lot_sz, "down"),
                                        self._quantize_size(min_sz, lot_sz, "up")
                                    )
                                    side_param = "buy" if pos_side == "long" else "sell"
                                    px = mid_px * (1 - self.ENTRY_SLIP) if pos_side == "long" else mid_px * (1 + self.ENTRY_SLIP)
                                    logging.info("[NODE][ATR-FALLBACK][TP] inst=%s %s ROE=%.2f%% ≥ %.2f%% → reduce %s",
                                                inst_id, pos_side.upper(), roe_est, threshold, cut_contracts)
                                    if mode == "test":
                                        logging.info("[TEST][NODE][TP] Would reduce_position inst=%s size=%s side=%s px=%.8f",
                                                    inst_id, cut_contracts, side_param, px)
                                    else:
                                        self.reduce_position(inst_id, px, cut_contracts, side_param)
                                    self._pp_hits[kfb] = need_lvl
                                    self._save_profit_guard_state()
                                    self._pp_last_px[kfb] = mid_px
                                    self._save_profit_guard_px_state()
                                    self._mark_cooldown(f"{kfb}:PP:FB:lvl{need_lvl}")
                                    curr = max(0.0, curr - cut_contracts)

                        # ATR LP 回退（无 LVN）
                        if len(lvn_seq) == 0 and self.LP_STEP_PCT > 0 and (self.LP_CUTS or self.LP_CUT_RATIO > 0) and curr > 0:
                            loss_est = max(0.0, -roe_est)
                            if loss_est > 0:
                                kfb = self._pp_key(inst_id, pos_side)
                                last = int(self._lp_hits.get(kfb, 0))
                                need_lvl = last + 1
                                dyn_thr = self._lp_step_threshold(float(lev_est or 1), need_lvl, inst_id)
                                threshold = (dyn_thr if (dyn_thr and dyn_thr > 0) else self.LP_STEP_PCT * need_lvl)
                                threshold *= (1 - self.LP_TOLERANCE_PCT / 100.0)
                                if loss_est >= threshold and not self._under_cooldown(f"{kfb}:LP:FB:lvl{need_lvl}"):
                                    tol_pct = (self._px_tol_pct(inst_id) or float(self.TRIGGER_PX_TOL_PCT or 0.0)) * self._lvl_factor(need_lvl)
                                    tol = abs(mid_px) * (tol_pct / 100.0)
                                    last_px_trig = self._lp_last_px.get(kfb)
                                    if (last_px_trig is None) or abs(float(mid_px) - float(last_px_trig)) > tol:
                                        lot_sz, min_sz2 = self._get_trade_steps(inst_id)
                                        min_sz = max(float(min_sz), float(min_sz2))
                                        lp_cuts_fb = getattr(self, "LP_CUTS", [])
                                        lp_cut_ratio_fb = getattr(self, "LP_CUT_RATIO", 0.0)
                                        cut_idx = min(len(lp_cuts_fb) - 1, max(0, need_lvl - 1)) if lp_cuts_fb else 0
                                        cut_ratio_fb = float(lp_cuts_fb[cut_idx]) if lp_cuts_fb else float(lp_cut_ratio_fb)
                                        cut_contracts = max(
                                            self._quantize_size(curr * cut_ratio_fb, lot_sz, "down"),
                                            self._quantize_size(min_sz, lot_sz, "up")
                                        )
                                        side_param = "buy" if pos_side == "long" else "sell"
                                        px = float(mid_px)  # LP 回退不加滑点
                                        logging.warning("[NODE][ATR-FALLBACK][SL] inst=%s %s lossROE=%.2f%% ≥ %.2f%% → reduce %s | 原因=无 LVN 回退",
                                                        inst_id, pos_side.upper(), loss_est, threshold, cut_contracts)
                                        if mode == "test":
                                            logging.info("[TEST][NODE][SL] Would reduce_position inst=%s size=%s side=%s px=%.8f",
                                                        inst_id, cut_contracts, side_param, px)
                                        else:
                                            self.reduce_position(inst_id, px, cut_contracts, side_param)
                                        self._lp_hits[kfb] = need_lvl
                                        self._save_loss_guard_state()
                                        self._lp_last_px[kfb] = mid_px
                                        self._save_loss_guard_px_state()
                                        self._mark_cooldown(f"{kfb}:LP:FB:lvl{need_lvl}")

        # ===================== ⑥ ROE 高水位拖尾止盈（MFE） ===================== #
        try:
            side_flag2 = (evt.get("side") or "-").upper()
            pos_side2 = "long" if side_flag2 == "LONG" else ("short" if side_flag2 == "SHORT" else None)
            if pos_side2:
                k = f"{inst_id}:{pos_side2}"
                snap = evt.get("snapshot", {}) or {}
                roe_now = None
                unreal = snap.get("unrealizedPnl")
                margin = snap.get("marginUsed")
                if unreal is not None and margin not in (None, "", 0, "0"):
                    roe_now = 100.0 * float(unreal) / max(1e-12, float(margin))
                else:
                    r = snap.get("returnOnEquity")
                    if r is not None:
                        r = float(r)
                        roe_now = r * 100.0 if abs(r) <= 10 else r
                if roe_now is None:
                    entry_px = float(snap.get("entryPx") or 0.0)
                    lev_est = float(snap.get("lev") or snap.get("leverage") or evt.get("lev") or 1.0)
                    if entry_px > 0 and mid_px > 0:
                        raw = (mid_px - entry_px) / entry_px
                        roe_now = raw * 100.0 * (lev_est if pos_side2 == "long" else -lev_est)

                if roe_now and roe_now > 0:
                    pk = max(self._mfe_peak.get(k, 0.0), float(roe_now))
                    self._mfe_peak[k] = pk
                    if pk >= self.MFE_START_PCT:
                        drop = pk - float(roe_now)
                        need_lvl = int(self._mfe_hits.get(k, 0)) + 1
                        backoff_need = float(self.MFE_BACKOFF_PCT)
                        gated = bool(self._mfe_gated.get(k, False))
                        last_peak_trig = float(self._mfe_last_peak_trig.get(k, 0.0))
                        if gated and pk < (last_peak_trig + self.MFE_REARM_DELTA_PCT):
                            return
                        elif gated and pk >= (last_peak_trig + self.MFE_REARM_DELTA_PCT):
                            self._mfe_gated[k] = False
                        if drop >= backoff_need and not self._under_cooldown(f"MFE:{k}:lvl{need_lvl}"):
                            tol_roe = self.MFE_ROE_TOL_PCT * self._lvl_factor(need_lvl)
                            last_roe = self._mfe_last_roe.get(k)
                            if (last_roe is not None) and (abs(float(roe_now) - float(last_roe)) <= tol_roe):
                                return
                            curr = 0.0
                            for p in positions:
                                if p.get("instId") != inst_id:
                                    continue
                                if self.pos_mode == "long_short" and (p.get("posSide", "") or "").lower() != pos_side2:
                                    continue
                                curr = abs(float(p.get("pos", 0) or 0))
                                break
                            if curr > 0:
                                _, _, _, min_sz = self._get_inst_spec(inst_id)
                                lot_sz, min_sz2 = self._get_trade_steps(inst_id)
                                min_sz = max(float(min_sz), float(min_sz2))
                                cut_ratio = float(self.MFE_CUTS[min(len(self.MFE_CUTS) - 1, max(0, need_lvl - 1))])
                                cut_contracts = max(
                                    self._quantize_size(min_sz, lot_sz, "up"),
                                    self._quantize_size(curr * cut_ratio, lot_sz, "down")
                                )
                                side_param = "buy" if pos_side2 == "long" else "sell"
                                px_exec = mid_px * (1 - self.ENTRY_SLIP) if pos_side2 == "long" else mid_px * (1 + self.ENTRY_SLIP)
                                logging.info("[MFE][TRAIL] inst=%s %s peak=%.2f%% now=%.2f%% drop=%.2f%% lvl=%d → reduce %s",
                                            inst_id, pos_side2.upper(), pk, roe_now, drop, need_lvl, cut_contracts)
                                if mode == "test":
                                    logging.info("[TEST][MFE] Would reduce_position inst=%s size=%s side=%s px=%.8f",
                                                inst_id, cut_contracts, side_param, px_exec)
                                else:
                                    self.reduce_position(inst_id, px_exec, cut_contracts, side_param)
                                self._mfe_hits[k] = need_lvl
                                self._mfe_last_roe[k] = float(roe_now)
                                self._mfe_last_peak_trig[k] = float(pk)
                                self._mfe_gated[k] = True
                                self._mark_cooldown(f"MFE:{k}:lvl{need_lvl}")
        except Exception as e:
            logging.debug("[MFE][SKIP] %s", e)

    # ============================================================
    #  持仓权威同步（含孤单）：工具函数
    # ============================================================
    def _mark_positions_synced(self) -> None:
        """记录最近一次成功的权威同步时间戳。"""
        try:
            self._last_pos_sync_ts = time.time()
        except Exception:
            pass

    def _maybe_sync_positions_after_operation(
        self,
        inst_id: str | None = None,
        reason: str = "post-op"
    ) -> None:
        """
        在“钱包触发的实操”（开/加/减/反手/撤单）后调用：
        若距离上次持仓同步时间超过 POSITION_SYNC_MAX_AGE_SEC，则触发一次权威同步，
        覆盖本地订单/持仓缓存（尽可能对齐孤单/挂单状态）。
        """
        try:
            last = float(getattr(self, "_last_pos_sync_ts", 0.0) or 0.0)
            max_age = float(getattr(self, "POSITION_SYNC_MAX_AGE_SEC", 60.0))
            now = time.time()
            if (now - last) >= max_age:
                logging.info("[POS_SYNC][CHECK] age=%.1fs ≥ %.1fs → authority_resync (reason=%s, inst=%s)",
                             now - last, max_age, reason, inst_id)
                self._authority_resync(reason=f"{reason}:stale>{int(max_age)}s",
                                       inst_id=inst_id if inst_id else None)
                self._mark_positions_synced()
        except Exception as e:
            logging.warning("[POS_SYNC][RESYNC][FAIL] reason=%s inst=%s err=%s", reason, inst_id, e)

    # ===== 新增：DEC 累计与刷单（达到最小下单量再执行） =====
    def _accumulate_dec_and_maybe_flush(self, inst_id: str, pos_side: str,
                                        delta_raw: Decimal, evt: dict | None = None) -> None:
        """
        将 delta_raw（张）累加到 (instId, pos_side) 维度的缓冲。
        当累计量达到最小可下单量且不会把仓位打穿“最小持仓量”时，一次性下单减仓。
        """
        key = (inst_id, pos_side)
        acc = self._dec_accum.get(key, Decimal("0"))
        acc += Decimal(delta_raw)
        if acc < 0:
            acc = Decimal("0")

        # 交易规格
        _, _, _, min_sz0 = self._get_inst_spec(inst_id)
        lot_sz, min_sz2 = self._get_trade_steps(inst_id)
        min_sz = Decimal(str(max(float(min_sz0), float(min_sz2))))
        lot = Decimal(str(float(lot_sz)))

        # 当前“本地暴露”（已持仓 + 未成交平仓挂单）
        held = Decimal(str(float(self._held_contracts_by_side(inst_id, pos_side))))
        _, pend_close = self._pending_closes_by_side(inst_id, pos_side)
        base = held + Decimal(str(float(pend_close)))

        # 最小持仓保护：保留 minSz，不触发继续减仓和平仓
        if base <= min_sz:
            cap = max(Decimal("0"), base - min_sz)  # 可继续累计的上限
            acc = min(acc, cap)
            self._dec_accum[key] = acc
            logging.info(
                "[DEC][KEEP_MIN][HOLD] inst=%s pos=%s base=%.8f<=minSz=%.8f → hold(acc→%.8f)",
                inst_id, pos_side, float(base), float(min_sz), float(acc)
            )
            return

        # 可发上限：不把仓位从 base 打到 minSz 以下
        def _floor_step(x: Decimal, step: Decimal) -> Decimal:
            if step <= 0:
                return x
            n = (x / step).to_integral_value(rounding=ROUND_DOWN)
            return n * step
        sendable_cap = _floor_step(base - min_sz, lot)
        flushable = min(_floor_step(acc, lot), sendable_cap)

        # 只有 flushable ≥ minSz 才下单（满足“达到最小减仓量才下单”）
        if flushable >= min_sz:
            # 拿执行价：事件 midPx/last/px → REST 兜底；方向加轻微滑点
            try:
                mid = float((evt or {}).get("midPx") or (evt or {}).get("last") or (evt or {}).get("px") or 0.0)
            except Exception:
                mid = 0.0
            if not mid or mid <= 0:
                mid = float(self._safe_last_price(inst_id) or 0.0)
            if not mid or mid <= 0:
                logging.warning("[DEC][FLUSH][SKIP] inst=%s pos=%s 无可用价格，暂缓", inst_id, pos_side)
                self._dec_accum[key] = acc
                return
            px = mid * (1 - self.ENTRY_SLIP) if pos_side == "long" else mid * (1 + self.ENTRY_SLIP)
            side_param = "buy" if pos_side == "long" else "sell"
            size = float(flushable)

            if mode == "test":
                logging.info("[TEST][DEC][FLUSH] inst=%s pos=%s px=%.8f size=%.8f side=%s (acc=%.8f, send_cap=%.8f)",
                             inst_id, pos_side, px, size, side_param, float(acc), float(sendable_cap))
            else:
                self.reduce_position(inst_id, px, size, side_param)
                # 操作后：条件满足时触发一次权威同步（由节流控制）
                with suppress(Exception):
                    self._maybe_sync_positions_after_operation(inst_id=inst_id, reason="DEC-FLUSH")
            acc -= flushable

        # 写回累计
        self._dec_accum[key] = max(Decimal("0"), acc)
        logging.info(
            "[DEC][ACC][STATE] inst=%s pos=%s acc=%.8f flushable=%.8f minSz=%.8f base=%.8f send_cap=%.8f",
            inst_id, pos_side, float(self._dec_accum[key]), float(flushable), float(min_sz), float(base), float(sendable_cap)
        )
