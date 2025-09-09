# core_infra.py
# 基础设施与执行：HTTP 重试、客户端重建、账户/订单/仓位、合约规格与杠杆缓存、
# 价格+波动缓存、下单/撤单、交易对解析、特征快照订阅、文件原子写等。
# 注意：不包含任何“是否触发/触发多少”的策略判断。

import os
import json
import uuid
import math
import itertools
import time
import random
import logging
import pathlib
import threading
from typing import Dict, Any, Tuple, Callable
from decimal import Decimal, ROUND_DOWN, ROUND_UP, InvalidOperation
import pika
import json as _json
# 在 import okx.* 之前抑制冗余日志（SDK/HTTP 客户端）
for noisy in ("okx", "httpx", "urllib3"):
    logging.getLogger(noisy).setLevel(logging.WARNING)

import okx.Account as Account
import okx.MarketData as MarketData
import okx.PublicData as PublicData
from okx.Trade import TradeAPI

from contextlib import suppress
from utils import peek_features_snapshot

mode = "real"
class CoreInfraMixin:
    # ---------- 价格/仓位查询等公共常量 ----------
    _POS_TTL = 1.0  # positions 查询 TTL，单飞避免惊群

    def __init__(self, cfg: dict | None = None):
        import pathlib, threading, os, time, math
        self._start_ts = time.time()

        # ---- 全局 TTL/窗口（可被 cfg 覆盖）----
        self.WARMUP_SEC = float((cfg or {}).get("warmup_sec", 8.0))
        self.WS_PX_TTL  = float((cfg or {}).get("ws_px_ttl_sec", 5.0))
        self.MISS_TTL   = int((cfg or {}).get("pair_miss_ttl_sec", 600))
        self.TIER_TTL   = float((cfg or {}).get("tier_ttl_sec", 900.0))
        self._vol_ttl   = 20.0
        self.KEEPALIVE_SEC = float(((cfg or {}).get("http") or {}).get("keepalive_sec", 0) or 0)
 
        # ---- 状态目录 ----
        base_dir = (cfg or {}).get("state_dir")
        if not base_dir:
            base_dir = os.path.join(os.path.dirname(__file__), ".state")
        self._state_dir = pathlib.Path(base_dir); self._state_dir.mkdir(parents=True, exist_ok=True)

        # ---- 文件路径 ----
        self._spec_cache_path = self._state_dir / "inst_spec_cache.json"
        self._lev_cache_path  = self._state_dir / "lev_cache.json"
        self._spec_lock       = self._state_dir / ".inst_spec.lock"
        self._pp_state_path   = self._state_dir / "profit_guard_hits.json"
        self._lp_state_path   = self._state_dir / "loss_guard_hits.json"
        self._pp_px_path      = self._state_dir / "profit_guard_px.json"
        self._lp_px_path      = self._state_dir / "loss_guard_px.json"

        # ---- 缓存与锁 ----
        self._spec_cache = self._load_spec_cache()
        self._lev_cache  = self._load_leverage_cache()
        self._tier_cache = {}
        self._applied_lev = {}
        self._px_cache   = {}
        self._vol_cache  = {}
        self._feat_cache = {}
        self._pair_cache = {}
        self._target_contracts = {}   # {(instId, posSide): target_contracts} — 用于对账/补单的“期望目标”
        # 订单缓存（_bootstrap_open_orders / get_pending_orders 会用到）
        if not hasattr(self, "orders"):
            self.orders = {}
        self._pair_miss  = {}
        self._miss_log_ts = {}
        self._pos_cache = {"ts": 0.0, "data": []}
        self._last_good_positions = []
        # ── NEW: 统一暴露与单位换算辅助缓存（可选）
        #   用于缓存最近一次的规格与价格，降低日志中的抖动
        self._expo_dbg = {}
        from collections import defaultdict
        self._io_locks     = defaultdict(threading.Lock)
        self._rebuild_lock = threading.Lock()
        self._side_op_locks = defaultdict(threading.Lock)   # 串行化 amend/cancel（按 inst+posSide）
        self._pos_lock     = threading.Lock()

        # 监控状态（供 _is_monitored / MONITOR_STATUS 使用）
        self._monitored_inst = {}
        self._monitored_coin = {}
        self._monitor_last   = {}

        # 可选启动特征订阅（默认启用）
        feat_cfg = (cfg or {}).get("features", {})
        if bool(feat_cfg.get("enable", True)):
            self._start_feature_feed()
        # ── DEC 参数（节流/重试/后退策略，可由 cfg['dec'] 覆盖） ──
        dec_cfg = (cfg or {}).get("dec", {}) if isinstance((cfg or {}).get("dec", {}), dict) else {}
        # 并发与节流
        self.DEC_THROTTLE_MIN_MS   = int(dec_cfg.get("throttle_min_ms", 150))   # 单边操作最小合并间隔
        self.DEC_THROTTLE_MAX_MS   = int(dec_cfg.get("throttle_max_ms", 300))   # 单边操作最大合并间隔
        # amend 失败重试
        self.DEC_AMEND_MAX_RETRY   = int(dec_cfg.get("amend_max_retry", 4))     # 超过后 cancel+（必要时）重下
        self.DEC_BACKOFF_BASE_MS   = int(dec_cfg.get("backoff_base_ms", 120))   # 指数退避基线
        self.DEC_BACKOFF_JITTER_MS = int(dec_cfg.get("backoff_jitter_ms", 80))  # 抖动
        # 允许“重新配比”时向下取整的步长容差（避免无限抖动）
        self.DEC_REALLOC_EPS       = float(dec_cfg.get("realloc_eps", 1e-12))
        # 内部 side-key → 下次可操作的时间戳（节流）
        self._side_next_ts         = defaultdict(float)
        # 用于重配时的最大单次循环，避免极端情况下的长时间占用
        self.DEC_REALLOC_MAX_PASSES = int(dec_cfg.get("realloc_max_passes", 3))
        # 用于重建客户端的节流时间戳（避免 AttributeError）
        self._last_rebuild = 0.0
        self._last_http_ts = 0.0
        if self.KEEPALIVE_SEC > 0:
            t = threading.Thread(target=self._keepalive_loop, name="okx-keepalive", daemon=True)
            t.start()
        # ─────────────────────────────────────────────────────────────
        # 极简对账 Reconciler（三步：拉快照 → 覆盖缓存/算偏差 → 降风险修复）
        # ─────────────────────────────────────────────────────────────
        rec_cfg = (cfg or {}).get("reconcile", {}) if isinstance((cfg or {}).get("reconcile", {}), dict) else {}
        self.RECON_ENABLE       = bool(rec_cfg.get("enable", True))
        self.RECON_INTERVAL_SEC = float(rec_cfg.get("interval_sec", 60.0))
        self.RECON_JITTER       = float(rec_cfg.get("jitter", 0.2))     # ±20% 抖动
        self.RECON_DOUBLE_READ  = bool(rec_cfg.get("double_read", False))
        self.RECON_TOL_FACTOR   = float(rec_cfg.get("tol_factor", 1.0)) # 容差= tol_factor × 步长

        self._reconciling = False
        self._recon_lock  = threading.Lock()
        self._recon_fail  = 0

        if self.RECON_ENABLE:
            threading.Thread(
                target=self._reconcile_loop, name="reconcile-loop", daemon=True
            ).start()
        # ───────── FOLLOW 跟单细化参数（仅在浮亏且对方均价更优时生效） ─────────
        fol_cfg = (cfg or {}).get("follow", {}) if isinstance((cfg or {}).get("follow", {}), dict) else {}
        # 目标“安全距离”，用 bps（万分比）表示；例如 50=0.50%
        self.FOLLOW_DIST_BPS        = float(fol_cfg.get("dist_bps", 50.0))
        # 本次加仓的滑点最多削减比例（0.33=最多削 1/3）
        self.FOLLOW_SLIP_SHRINK_MAX = float(fol_cfg.get("slip_shrink_max", 0.33))
        # 触发 IOC 的距离阈值（相对 FOLLOW_DIST_BPS 的比例），0.7 表示距离≥70%安全距离时用 IOC
        self.FOLLOW_IOC_THRESHOLD   = float(fol_cfg.get("ioc_threshold", 0.7))
        # 最低滑点地板（百分比的“分数”形态），默认复用 SLIP_MIN_PCT；没有就为 0
        self.FOLLOW_MIN_SLIP_PCT    = float(fol_cfg.get("min_slip_pct", getattr(self, "SLIP_MIN_PCT", 0.0)))
        # 盘口缓存（与 features 独立，TTL 更短，用于撮合/滑点基准）
        self._bbo_cache: dict[str, dict] = {}
        self._bbo_cache_ttl = float((cfg or {}).get("bbo_ttl_sec", 5.0))
    # ============================================================
    #  通用 HTTP 调用（带重试） & 客户端重建
    # ============================================================
    def _call_api(self, func: Callable, *args, **kwargs):
        """
        对 OKX SDK 的 HTTP 调用做 5 次以内指数退避重试。
        遇到连接断开等错误时自动重建客户端。
        """
        delay = 0.5
        for attempt in range(1, 6):
            try:
                ret = func(*args, **kwargs)
                self._last_http_ts = time.time()
                return ret
            except Exception as e:
                # —— 统一处理速率限制：429 + Retry-After —— #
                resp = getattr(e, "response", None)
                status = getattr(resp, "status_code", None) if resp is not None else None
                if status == 429:
                    ra_hdr = (resp.headers.get("Retry-After") or "").strip()
                    retry_after = None
                    # 1) 数字（秒）
                    try:
                        if ra_hdr:
                            retry_after = max(0, int(float(ra_hdr)))
                    except Exception:
                        pass
                    # 2) HTTP-Date
                    if retry_after is None and ra_hdr:
                        try:
                            from email.utils import parsedate_to_datetime
                            from datetime import datetime, timezone
                            dt = parsedate_to_datetime(ra_hdr)
                            if dt is not None:
                                if dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)
                                retry_after = max(0, int((dt - datetime.now(timezone.utc)).total_seconds()))
                        except Exception:
                            pass
                    # 3) 厂商扩展
                    if retry_after is None:
                        h = resp.headers
                        for k in ("X-RateLimit-Reset", "x-ratelimit-reset", "x-ratelimit-reset-ms"):
                            if k in h:
                                try:
                                    v = float(h[k]); retry_after = max(0, int(v/1000 if k.endswith("-ms") else v)); break
                                except Exception:
                                    pass
                    if retry_after is None: retry_after = 1
                    logging.warning("Rate-limit: 429 — sleep %ss then retry %s",
                                    retry_after, getattr(func, "__name__", repr(func)))
                    time.sleep(retry_after)
                    # 继续下一轮尝试（不增加退避倍数）
                    continue
                emsg = f"{type(e).__name__}: {e}"
                transient = (
                    "ConnectionTerminated" in emsg or
                    "Server disconnected" in emsg or
                    "RemoteProtocolError" in emsg or
                    "ConnectionResetError" in emsg or
                    "ReadTimeout" in emsg or
                    "ConnectError" in emsg
                )
                if transient:
                    self._rebuild_clients()
                if attempt == 5:
                    raise
                jitter = random.random() * 0.3
                logging.warning(
                    "[RETRY %s/5] %s failed: %s — next in %.2fs",
                    attempt, getattr(func, "__name__", repr(func)), e, delay
                )
                time.sleep(delay)
                delay *= 1.8 + jitter

    def _rebuild_clients(self) -> None:
        """在连接异常后重建各个 OKX 客户端（2s 内节流一次）"""
        with self._rebuild_lock:
            if time.time() - self._last_rebuild < 2.0:
                return
            self._last_rebuild = time.time()
            try:
                self.account_api = Account.AccountAPI(self._api_key, self._secret_key, self._passphrase, False, self.flag)
                self.market_api  = MarketData.MarketAPI(flag=self.flag)
                self.public_api  = PublicData.PublicAPI(flag=self.flag)
                self.trade_api   = TradeAPI(self._api_key, self._secret_key, self._passphrase, False, self.flag)
                logging.warning("[HTTP] Rebuilt OKX clients after connection error")
            except Exception as e:
                logging.exception("Rebuild clients failed: %s", e)
    # ------------------------------------------------------------
    # Keep-alive：空闲期间轻量请求，减少“首跳撞死”概率（可选）
    # ------------------------------------------------------------
    def _keepalive_loop(self) -> None:
        """
        周期性轻量请求公共只读端点；失败静默。
        仅在检测到 public_api 存在后才开始工作，避免早期启动顺序问题。
        配置：cfg.http.keepalive_sec > 0 时启用。
        """
        while True:
            try:
                if not getattr(self, "public_api", None):
                    time.sleep(min(5.0, max(1.0, self.KEEPALIVE_SEC)))
                    continue
                # 仅在“最近一段时间没有任何HTTP请求”时才保活
                if (time.time() - (self._last_http_ts or 0)) < self.KEEPALIVE_SEC:
                    time.sleep(self.KEEPALIVE_SEC)
                    continue
                # 走统一的 _call_api（能触发重建/重试逻辑）
                self._call_api(self.public_api.get_mark_price, instType="SWAP", instId="BTC-USDT-SWAP")

            except Exception:
                # 静默：连接可能被动回收，下一轮会由 _call_api 自愈
                pass
            # 间隔
            time.sleep(self.KEEPALIVE_SEC * (0.9 + 0.2 * random.random()))
    # ============================================================
    #  文件原子写（用于各类本地 JSON 状态持久化）
    # ============================================================
    def _atomic_json_dump(self, path: pathlib.Path, data: Any) -> None:
        path = pathlib.Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        lock = self._io_locks[str(path)]
        with lock:
            tmp = path.with_name(f"{path.name}.{uuid.uuid4().hex}.tmp")
            with open(tmp, "w", encoding="utf-8") as fp:
                json.dump(data, fp, ensure_ascii=False)
                fp.flush()
                os.fsync(fp.fileno())
            os.replace(tmp, path)  # POSIX 原子替换

    # ============================================================
    #  合约规格 / 杠杆缓存（inst_spec_cache.json / lev_cache.json）
    # ============================================================
    def _load_spec_cache(self) -> Dict[str, Dict[str, str]]:
        """加载本地规格缓存：{instId: {ctVal, ctValCcy, minSz}}"""
        if not self._spec_cache_path.exists():
            return {}
        try:
            with open(self._spec_cache_path, "r", encoding="utf-8") as fp:
                return json.load(fp)
        except Exception as e:
            logging.warning("读取 inst_spec_cache 失败: %s", e)
            return {}

    def _save_spec_cache(self) -> None:
        try:
            # 简易文件锁，保证并发安全
            with self._spec_lock.open("w") as lockfp:
                lockfp.write("1")
            try:
                tmp = self._spec_cache_path.with_suffix(self._spec_cache_path.suffix + ".tmp")
                with open(tmp, "w", encoding="utf-8") as fp:
                    json.dump(self._spec_cache, fp, ensure_ascii=False, indent=2)
                tmp.replace(self._spec_cache_path)
            finally:
                self._spec_lock.unlink(missing_ok=True)
        except Exception as e:
            logging.warning("写入 inst_spec_cache 失败: %s", e)

    def _load_leverage_cache(self) -> Dict[str, int]:
        """lev_cache.json → {instId: maxLever}"""
        if not self._lev_cache_path.exists():
            return {}
        try:
            with open(self._lev_cache_path, "r", encoding="utf-8") as fp:
                return {k: int(v) for k, v in json.load(fp).items()}
        except Exception:
            return {}

    def _save_leverage_cache(self) -> None:
        try:
            self._atomic_json_dump(self._lev_cache_path, self._lev_cache)
        except Exception as e:
            logging.warning("写入 lev_cache 失败: %s", e)

    def _get_max_leverage(self, inst_id: str) -> int:
        """
        查询最大可用杠杆优先级：
          ① account.get_leverage_estimated_info / get_adjust_leverage_info
          ② account.get_leverage / get_leverage_info
          ③ public.get_instruments 的 lever（理论上限）
          ④ 兜底：self.leverage
        """
        if inst_id in self._lev_cache:
            return self._lev_cache[inst_id]

        for _meth in ("get_leverage_estimated_info", "get_adjust_leverage_info"):
            meth = getattr(self.account_api, _meth, None)
            if not meth:
                continue
            try:
                est = self._call_api(
                    meth,
                    instType="SWAP",
                    mgnMode=self.margin_mode,
                    lever=str(int(float(self.leverage))),
                    instId=inst_id
                )
                data = (est or {}).get("data", [])
                if data:
                    max_lev = int(float(data[0].get("maxLever", 0)))
                    if max_lev > 0:
                        self._lev_cache[inst_id] = max_lev
                        self._save_leverage_cache()
                        return max_lev
            except Exception as e:
                logging.warning("%s 查询最大杠杆失败: %s", _meth, e)

        for _meth in ("get_leverage", "get_leverage_info"):
            meth = getattr(self.account_api, _meth, None)
            if not meth:
                continue
            try:
                info = self._call_api(meth, instId=inst_id, mgnMode=self.margin_mode)
                rows = (info or {}).get("data", [])
                cand = max((int(float(r.get("maxLever", r.get("lever", 0) or 0))) for r in rows), default=0)
                if cand > 0:
                    self._lev_cache[inst_id] = cand
                    self._save_leverage_cache()
                    return cand
            except Exception as e:
                logging.warning("account.%s 失败: %s", _meth, e)

        try:
            pub = self._call_api(self.public_api.get_instruments, instType="SWAP", instId=inst_id)
            if pub.get("code") == "0" and pub.get("data"):
                lev_str = (pub["data"][0].get("lever") or "").strip()
                max_lev = int(float(lev_str)) if lev_str else 0
                if max_lev > 0:
                    self._lev_cache[inst_id] = max_lev
                    self._save_leverage_cache()
                    return max_lev
        except Exception as e:
            logging.warning("public.get_instruments 读取 lever 失败: %s", e)

        logging.warning("最大杠杆查询失败，回退到本地配置 leverage=%s", self.leverage)
        return int(float(self.leverage))

    def _get_inst_spec(self, inst_id: str) -> Tuple[int, float, str, float]:
        """
        返回 (maxLever, ctVal, ctValCcy, minSz)；
        ctVal/minSz 缓存在 inst_spec_cache.json，maxLever 通过 _get_max_leverage 获取。
        """
        if inst_id in self._spec_cache:
            d = self._spec_cache[inst_id]
            need_keys = {"ctVal", "ctValCcy", "minSz"}
            if need_keys.issubset(d):
                max_lev = self._get_max_leverage(inst_id)
                return (max_lev, float(d["ctVal"]), d["ctValCcy"], float(d["minSz"]))
            self._spec_cache.pop(inst_id, None)  # 清除残缺缓存

        try:
            resp = self._call_api(self.public_api.get_instruments, instType="SWAP", instId=inst_id)
            if resp["code"] == '0' and resp["data"]:
                d = resp["data"][0]
                ct_val     = max(float(d["ctVal"]), 1e-8)
                ct_val_ccy = d.get("ctValCcy", "USDT")
                min_sz     = max(float(d["minSz"]), 1e-8)
                # NEW: 同步保存 lotSz / tickSz（若有）以供步长与价格刻度使用
                lot_sz_raw = d.get("lotSz")
                try:
                    lot_sz = float(lot_sz_raw) if lot_sz_raw is not None else 0.0
                except Exception:
                    lot_sz = 0.0
                tick_sz_raw = d.get("tickSz")
                try:
                    tick_sz = float(tick_sz_raw) if tick_sz_raw is not None else 0.0
                except Exception:
                    tick_sz = 0.0
                self._spec_cache[inst_id] = {
                    "ctVal": ct_val,
                    "ctValCcy": ct_val_ccy,
                    "minSz": min_sz,
                    "lotSz": lot_sz,
                    "tickSz": tick_sz,
                }
                self._save_spec_cache()
                max_lev = self._get_max_leverage(inst_id)
                return (max_lev, ct_val, ct_val_ccy, min_sz)
        except Exception as e:
            logging.warning("获取 %s 规格失败: %s", inst_id, e)

        return int(float(self.leverage)), 1.0, "USDT", 1e-8

    # ============================================================
    #  杠杆设置（选择/确保/校验）
    # ============================================================
    def _select_leverage(self, max_lev: int, mq_lev: int | None = None) -> int:
        """
        选定杠杆（不超过 max_lev）：
        • max     → max_lev
        • normal  → 1×
        • double  → 2×
        • triple  → 3×
        基准优先用 mq_lev（主钱包传来的），否则用本地 self.leverage
        """
        if self.leverage_mode == "max":
            return max_lev

        factor = {"normal": 1, "double": 2, "triple": 3}.get(self.leverage_mode, 1)
        base = (mq_lev * factor) if mq_lev else int(float(self.leverage)) * factor
        chosen = max(1, min(max_lev, int(base)))

        if mq_lev is None:
            logging.debug("[LEV] MQ 未提供 leverage → 使用本地基准 %sx (mode=%s, factor=%s, cap=%s)",
                        int(float(self.leverage)) * factor, self.leverage_mode, factor, max_lev)
        return chosen

    def _apply_and_verify_leverage(self, inst_id: str, pos_side: str | None, req_lev: int) -> dict:
        """
        set_leverage + 回读校验（get_leverage/_info），并记录被截断等情况。
        返回：{"ok": bool, "resp": dict|None, "applied": int|None, "max": int|None}
        """
        lev_kwargs = dict(instId=inst_id, lever=str(int(req_lev)), mgnMode=self.margin_mode)
        if self.margin_mode == "isolated" and self.pos_mode == "long_short" and pos_side:
            lev_kwargs["posSide"] = pos_side
        try:
            resp = self._call_api(self.account_api.set_leverage, **lev_kwargs)
            code = str((resp or {}).get("code", "")); msg = (resp or {}).get("msg", "")
            if code and code != "0":
                logging.warning("[LEV][SET][FAIL] inst=%s pos=%s req=%sx → code=%s msg=%s data=%s",
                                inst_id, pos_side or "-", req_lev, code, msg, (resp or {}).get("data"))
                return {"ok": False, "resp": resp, "applied": None, "max": None}
        except Exception as e:
            status = getattr(e, "response", None); sc = getattr(status, "status_code", None)
            logging.warning("[LEV][SET][EXC] inst=%s pos=%s req=%sx → %s%s",
                            inst_id, pos_side or "-", req_lev, type(e).__name__, f" http={sc}" if sc else "")
            return {"ok": False, "resp": None, "applied": None, "max": None}

        applied, max_seen = None, None
        for _meth in ("get_leverage", "get_leverage_info"):
            meth = getattr(self.account_api, _meth, None)
            if not meth:
                continue
            try:
                info = self._call_api(meth, instId=inst_id, mgnMode=self.margin_mode)
                rows = (info or {}).get("data", []) or []
                for r in rows:
                    if r.get("instId", inst_id) != inst_id:
                        continue
                    if self.margin_mode == "isolated" and self.pos_mode == "long_short":
                        ps = (r.get("posSide") or "").lower()
                        if pos_side and ps and ps != pos_side.lower():
                            continue
                    try:
                        applied = int(float(r.get("lever") or r.get("mgnLeverage") or 0))
                    except Exception:
                        applied = None
                    try:
                        max_seen = int(float(r.get("maxLever") or r.get("lever") or 0)) or max_seen
                    except Exception:
                        pass
                    break
                if applied is not None or max_seen is not None:
                    break
            except Exception as e:
                logging.debug("[LEV][VERIFY] %s failed: %s", _meth, e)

        if applied is not None and applied != int(req_lev):
            logging.warning("[LEV][VERIFY] %s/%s 请求 %sx，实际生效 %sx（可能被上限或风控截断；max≈%s）",
                            inst_id, pos_side or "-", int(req_lev), applied, max_seen)
        return {"ok": True, "resp": resp, "applied": applied, "max": max_seen}

    def _ensure_leverage(self, inst_id: str, pos_side: str, mq_lev: int | None) -> None:
        """若希望杠杆与上次生效不同，则调用 set_leverage 并写入 _applied_lev。"""
        try:
            max_lev, _, _, _ = self._get_inst_spec(inst_id)
            chosen = int(self._select_leverage(max_lev, mq_lev))
            key = (inst_id, pos_side or "")
            if self._applied_lev.get(key) == chosen:
                return
            res = self._apply_and_verify_leverage(inst_id, pos_side, chosen)
            if res.get("ok"):
                applied = int(res.get("applied") or chosen)
                self._applied_lev[key] = applied
            else:
                logging.warning("[LEV][ENSURE] set_leverage failed(see above): inst=%s pos=%s req=%sx",
                                inst_id, pos_side, chosen)
        except Exception as e:
            logging.warning("[LEV][ENSURE] set_leverage failed: %s", e)

    # ============================================================
    #  账户 / 订单 / 仓位
    # ============================================================
    def _bootstrap_open_orders(self):
        """启动时加载 *未成交* 挂单进缓存"""
        if hasattr(self.trade_api, "get_orders_pending"):
            resp = self._call_api(self.trade_api.get_orders_pending)
        else:
            resp = self._call_api(self.trade_api.get_order_list)
        pending_states = {"live", "partially_filled"}
        for od in resp.get("data", []):
            if od.get("state") in pending_states:
                self.orders[od["ordId"]] = od

    def _bootstrap_positions(self):
        """启动时加载当前持仓（只做存在性判断）"""
        resp = self._call_api(self.account_api.get_positions)
        for pos in resp.get("data", []):
            key = f"pos_{pos['instId']}_{pos['posSide']}"
            self.orders[key] = pos

    def _record_order(self, order_resp: Dict[str, Any]):
        """下单或查询后，把订单信息写入缓存"""
        if order_resp.get("code") == "0" and order_resp["data"]:
            self.orders[order_resp["data"][0]["ordId"]] = order_resp["data"][0]

    def _refresh_order_state(self, inst_id: str, ord_id: str) -> str:
        """查询订单最新状态并返回 state"""
        resp = self._call_api(self.trade_api.get_order, instId=inst_id, ordId=ord_id)
        if resp.get("code") == "0" and resp["data"]:
            self._record_order(resp)
            return resp["data"][0]["state"]
        return "unknown"

    def get_pending_orders(self) -> list[Dict[str, Any]]:
        """返回未成交挂单（live/partially_filled）"""
        self._bootstrap_open_orders()
        return [od for od in self.orders.values() if od.get("state") in {"live", "partially_filled"}]

    def _cancel_all_pending(self, inst_id: str, pos_side: str | None = None) -> int:
        """
        取消指定 instId 的所有未成交挂单；
        long_short 模式下若传 pos_side（'long'/'short'）则只取消该方向挂单。
        返回撤单数量。
        """
        canceled = 0
        for od in list(self.get_pending_orders()):
            if od.get("instId") != inst_id:
                continue
            # 关键：降风险只动“开仓/加仓”类挂单；保护性 reduceOnly 单不要动
            if str(od.get("reduceOnly", "")).lower() == "true":
                continue
            if self.pos_mode == "long_short" and pos_side:
                if (od.get("posSide") or "").lower() != pos_side.lower():
                    continue
            try:
                self._call_api(self.trade_api.cancel_order, instId=inst_id, ordId=od["ordId"])
                canceled += 1
                logging.info("[CANCEL][PENDING] inst=%s ordId=%s posSide=%s 已撤单",
                             inst_id, od.get("ordId"), od.get("posSide"))
            except Exception as e:
                logging.warning("[CANCEL][PENDING][FAIL] inst=%s ordId=%s err=%s",
                                inst_id, od.get("ordId"), e)
        if canceled > 0:
            logging.info("[CANCEL][SUMMARY] inst=%s 撤销挂单数量=%d (posSide=%s)", inst_id, canceled, pos_side or "ALL")
        return canceled

    def get_positions(self, force: bool = False) -> list[Dict[str, Any]]:
        """带 TTL/singleflight 的 positions 查询。"""
        now = time.monotonic()
        if not force and now - self._pos_cache["ts"] < self._POS_TTL:
            return self._pos_cache["data"]
        with self._pos_lock:
            now = time.monotonic()
            if not force and now - self._pos_cache["ts"] < self._POS_TTL:
                return self._pos_cache["data"]
            try:
                resp = self._call_api(self.account_api.get_positions)
                data = resp.get("data", [])
                if data:
                    self._last_good_positions = data[:]
            except Exception as e:
                logging.exception("get_positions failed (will return cache): %s", e)
                data = self._last_good_positions or self._pos_cache["data"]
                if not data:
                    time.sleep(0.4)
                    try:
                        resp = self._call_api(self.account_api.get_positions)
                        data = resp.get("data", [])
                        if data:
                            self._last_good_positions = data[:]
                    except Exception:
                        pass
            self._pos_cache = {"ts": now, "data": data}
            return data

    def get_active_trades(self) -> list[Dict[str, Any]]:
        """
        返回当前仍在进行中的委托或仓位：
        - 订单状态在 live / partially_filled / filled 之间
        - 剔除已 canceled、已平仓或已归档订单
        """
        self._bootstrap_open_orders()
        self._bootstrap_positions()
        active_states = {"live", "partially_filled", "filled"}
        return [
            od for od in self.orders.values()
            if od.get("state", "live") in active_states or od.get("pos") not in (None, "0")
        ]
 

    def get_balance(self) -> list:
        """查询账户余额"""
        resp = self._call_api(self.account_api.get_account_balance)
        return resp.get("data", [])

    def _get_available_usdt(self) -> float:
        """
        从 /account/balance 解析可用 USDT。
        """
        balances = self.get_balance()
        for row in balances:
            if row.get("ccy", "").upper() == "USDT":
                return float(row.get("availEq") or row.get("availBal") or 0)
            for det in row.get("details", []):
                if det.get("ccy", "").upper() == "USDT":
                    return float(det.get("availBal") or det.get("availEq") or 0)
        return 0.0

    # ============================================================
    #  下单/撤单/平仓 执行
    # ============================================================
    def open_position(
        self,
        inst_id: str,
        price: float,
        size: float,
        side: str,
        mq_lev: int | None = None,
        size_unit: str | None = "contracts",
        ord_type: str | None = None,   # NEW：允许本次指定 ordType（如 'limit' / 'ioc'）
    ) -> dict:
        """
        开仓（限价单）；side="buy" 开多，"sell" 开空。自动确保杠杆。
        参数：
          - size: 下单数量
          - size_unit: "contracts"（默认，按张）或 "coin"/"base"（按币数量，会自动换算成张）
        """
        pos_side = "long" if side.lower() == "buy" else "short"
        self._ensure_leverage(inst_id, pos_side, mq_lev)

        # 统一换算到“张数”
        unit = str(size_unit or "contracts").lower()
        if unit in ("coin", "coins", "base", "base_ccy"):
            sz_ct = self._contracts_from_coin(float(size), inst_id)
        else:
            sz_ct = float(size)

        # 步长 & 最小张数（使用稳健获取 + Decimal 量化）
        lot_sz, min_sz = self._get_trade_steps(inst_id)
        sz_ct = self._ensure_min_step(float(sz_ct), float(lot_sz), float(min_sz), mode="down")
        if sz_ct + 1e-12 < float(min_sz):
            raise ValueError(f"order size too small after step align: {sz_ct} < minSz {min_sz}")
        # 按 lotSz 的小数位格式化 sz，避免浮点尾差导致的 51121
        from decimal import Decimal, ROUND_DOWN
        step_dec = Decimal(str(lot_sz if lot_sz > 0 else 1))
        digits = -step_dec.as_tuple().exponent if step_dec.as_tuple().exponent < 0 else 0
        # 再次用 Decimal 落到步长网格，生成字符串
        _sz_dec = Decimal(str(sz_ct))
        _units = (_sz_dec / step_dec).to_integral_value(rounding=ROUND_DOWN)
        _q = _units * step_dec
        sz_str = (f"{_q:.{digits}f}" if digits > 0 else str(int(_q)))

        ot = (ord_type or "limit").lower()
        order_params = dict(instId=inst_id, tdMode=self.margin_mode, side=side, ordType=ot, px=str(price), sz=sz_str)
 
        if self.pos_mode == "long_short":
            order_params["posSide"] = pos_side
        # 先发单；若命中 51121，再按步长向下对齐一步重试一次
        resp = self._send_with_authority_fallback(self._call_api, self.trade_api.place_order, **order_params)
        if self._resp_has_code(resp, "51121"):
            def _send_again(new_size_str: str) -> dict:
                new_params = dict(order_params); new_params["sz"] = new_size_str
                return self._send_with_authority_fallback(self._call_api, self.trade_api.place_order, **new_params)
            resp = self._maybe_retry_51121(
                resp,
                send_again=_send_again,
                size_str=sz_str,
                lot_sz=float(lot_sz),
                min_sz=float(min_sz),
                inst_id=inst_id,
                side_desc=side,
                px=float(price),
            )
        self._record_order(resp)
        return resp

    def add_position(self, inst_id: str, price: float, size: float, side: str, mq_lev: int | None = None, size_unit: str | None = "contracts") -> dict:
        """加仓：再次开仓（可携带 mq_lev）；支持 size_unit/ord_type 同 open_position"""
        return self.open_position(inst_id, price, size, side, mq_lev=mq_lev, size_unit=size_unit)

    def reduce_position(self, inst_id: str, price: float, size: float, side: str) -> dict:
        """
        减仓：以相反方向限价单平掉指定方向的仓位
        side：原方向（buy=long / sell=short），函数内部会下反向单
        """
        opp_side = "sell" if side.lower() == "buy" else "buy"
        pos_side = "long" if side.lower() == "buy" else "short"
        order_params = dict(
            instId=inst_id, tdMode=self.margin_mode, side=opp_side, ordType="limit",
            px=str(price), sz=str(size), reduceOnly="true"
        )
        if self.pos_mode == "long_short":
            order_params["posSide"] = pos_side
        resp = self._call_api(self.trade_api.place_order, **order_params)
        self._record_order(resp)
        return resp

    def close_position(self, inst_id: str, ord_id: str) -> dict:
        """
        智能平仓：
          • 挂单未成交 -> 撤单
          • 订单已成交 -> reduceOnly 限价反向对冲
        """
        state = self._refresh_order_state(inst_id, ord_id)
        if state in {"live", "partially_filled"}:
            return self._call_api(self.trade_api.cancel_order, instId=inst_id, ordId=ord_id)
        if state == "filled":
            origin = self.orders[ord_id]
            opp_side = "sell" if origin["side"].lower() == "buy" else "buy"
            last_px = str(self._safe_last_price(inst_id) or 0)
            order_params = dict(
                instId=inst_id, tdMode=self.margin_mode, side=opp_side,
                ordType="limit", px=last_px, sz=origin["sz"], reduceOnly="true"
            )
            if self.pos_mode == "long_short":
                order_params["posSide"] = origin.get("posSide")
            resp = self._call_api(self.trade_api.place_order, **order_params)
            self._record_order(resp)
            return resp
        raise ValueError(f"订单 {ord_id} 状态未知，无法平仓")

    def _with_slip_for_side(self, pos_side: str, base_px: float, slip: float | None = None) -> float:        
        """
        根据方向加滑点：
          - 优先使用“本次传入的 slip”（通常为动态滑点）
          - 若未传入，则回退到固定的 self.ENTRY_SLIP（向后兼容）
        long → px * (1 - s)
        short→ px * (1 + s)
        """
        try:
            s = abs(float(self.ENTRY_SLIP if slip is None else slip))
        except Exception:
            s = 0.0
        if s <= 0:
            return float(base_px)
        return float(base_px) * (1 - s) if str(pos_side).lower() == "long" else float(base_px) * (1 + s)

    # ============================================================
    #  我的方向均价读取 + 跟单滑点缩减计算（仅在浮亏且对方均价更优时启用）
    # ============================================================
    def _avg_entry_px(self, inst_id: str, pos_side: str) -> float:
        """读取我方该 inst+side 的开仓均价（OKX 字段一般为 avgPx）"""
        inst_u, side_l = inst_id.upper(), (pos_side or "").lower()
        for p in (self.get_positions() or []):
            if (p.get("instId") or "").upper() != inst_u: 
                continue
            if (p.get("posSide") or "").lower() != side_l:
                continue
            try:
                ap = float(p.get("avgPx") or 0.0)
                if ap > 0: 
                    return ap
            except Exception:
                pass
        return 0.0

    def _follow_shrink_entry_slip(self, inst_id: str, pos_side: str, peer_avg_px: float, last_px: float) -> tuple[float | None, bool, float]:
        """
        返回 (new_slip | None, force_ioc, shrink_ratio)。
        仅当：①浮亏 ②对方合并均价更优 时，给出“缩减后的滑点”和是否用 IOC；否则 (None, False, 0)。
        """
        try:
            my_avg = float(self._avg_entry_px(inst_id, pos_side) or 0.0)
            peer   = float(peer_avg_px or 0.0)
            last   = float(last_px or 0.0)
        except Exception:
            return (None, False, 0.0)
        if my_avg <= 0 or peer <= 0 or last <= 0:
            return (None, False, 0.0)

        side_l = (pos_side or "").lower()
        floating_loss = (last < my_avg) if side_l == "long" else (last > my_avg)
        peer_better   = (peer < my_avg) if side_l == "long" else (peer > my_avg)
        if not (floating_loss and peer_better):
            return (None, False, 0.0)

        # 基础滑点（沿用你现有 ATR/价差逻辑）
        base_slip = float(self._dynamic_entry_slip(inst_id, last) or 0.0)
        if base_slip <= 0:
            return (None, False, 0.0)

        # 距离按“相对我的均价”的分数计算；与目标安全距离（bps）对比
        dist_frac = abs(my_avg - peer) / my_avg
        target_frac = max(1e-6, float(self.FOLLOW_DIST_BPS) / 10000.0)
        # 距离越大 → 缩减越多，上限 FOLLOW_SLIP_SHRINK_MAX
        shrink_ratio = min(1.0, dist_frac / target_frac)
        new_slip = base_slip * (1.0 - float(self.FOLLOW_SLIP_SHRINK_MAX) * shrink_ratio)
        new_slip = max(float(self.FOLLOW_MIN_SLIP_PCT or 0.0), new_slip)

        # 若距离已达到阈值比例，则建议把本笔设为限价 IOC（只扫一跳，避免深挂）
        force_ioc = (shrink_ratio >= float(self.FOLLOW_IOC_THRESHOLD or 1.0))
        return (float(new_slip), bool(force_ioc), float(shrink_ratio))
    # ============================================================
    #  跟单 IOC 微追价（仅在浮亏且对方均价更优时触发）
    #  用法：收到“对方加仓”MQ事件时调用；能成就成，剩余立刻取消（ordType=ioc）
    # ============================================================
    def follow_ioc_add_once(
        self,
        coin: str,
        side_hint: str,            # "LONG"/"SHORT" 或 "buy"/"sell"
        add_size: float,           # 本次想加的数量（默认 contracts；可用 size_unit 控制）
        peer_avg_px: float,        # 对方合并均价（来自 MQ）
        mq_lev: int | None = None,
        size_unit: str = "contracts",
    ) -> dict:
        """
        仅在“浮亏且对方均价更优”时，做一次 IOC 微追价加仓；能成就成，剩余立刻取消。
        返回 OKX 下单回包；若条件不满足则返回 {"ok": False, "reason": "..."}。
        """
        inst_id = self._get_inst_id(coin)
        if not inst_id:
            return {"ok": False, "reason": f"no instId for {coin}"}

        side_l = str(side_hint or "").lower()
        pos_side = "long" if side_l in ("long", "buy") else "short"

        # 基价拆分：
        # ① logic_px：用于“是否浮亏/距离/缩滑点”判定（mid/last，避免用 ask/bid 误杀浮亏）
        # ② exec_base_px：用于 IOC 执行价基准（ask/bid，贴近可成交侧）
        rec = self._px_cache.get(inst_id) or {}
        fresh = (time.time() - float(rec.get("ts") or 0.0)) <= max(0.0, float(self.WS_PX_TTL or 0.0))
        if fresh and "bid" in rec and "ask" in rec:
            bid = float(rec["bid"]); ask = float(rec["ask"])
            logic_px = (bid + ask) / 2.0
            exec_base_px = float(ask if pos_side == "long" else bid)
        else:
            px = float(self._safe_last_price(inst_id) or 0.0)
            logic_px = px
            exec_base_px = px
        if exec_base_px <= 0 or logic_px <= 0:
            return {"ok": False, "reason": "no fresh price"}

        # 距离驱动的滑点缩减 & 是否强制 IOC
        new_slip, force_ioc, _ratio = self._follow_shrink_entry_slip(
            inst_id=inst_id, pos_side=pos_side, peer_avg_px=float(peer_avg_px), last_px=logic_px
        )
        if new_slip is None:
            return {"ok": False, "reason": "not floating-loss or peer not better"}

        # 追价方向：多头向上、空头向下；对齐 tickSz（多头向上取整，空头向下取整）
        slip = float(new_slip)
        raw_px = exec_base_px * (1.0 + slip) if pos_side == "long" else exec_base_px * (1.0 - slip)
        tick = 0.0
        try:
            # 保证 tickSz 已缓存（_get_inst_spec 会回填）
            _ = self._get_inst_spec(inst_id)
            tick = float((self._spec_cache.get(inst_id) or {}).get("tickSz") or 0.0)
        except Exception:
            tick = 0.0
        if tick > 0:
            k = math.ceil(raw_px / tick) if pos_side == "long" else math.floor(raw_px / tick)
            exec_px = max(tick, k * tick)
        else:
            exec_px = raw_px

        # 尺寸统一到“张”并对齐步长
        unit = str(size_unit or "contracts").lower()
        if unit in ("coin", "coins", "base", "base_ccy"):
            sz_ct = self._contracts_from_coin(float(add_size), inst_id)
        else:
            sz_ct = float(add_size)
        step = self._min_contract_step(inst_id)
        sz_ct = math.floor((sz_ct + 1e-12) / step) * step
        if sz_ct < step:
            return {"ok": False, "reason": f"order size too small after step align: {sz_ct} < step {step}"}

        # 确保杠杆 → 总是发 IOC（能成就成，剩余立刻取消；不留远离市场的挂单）
        self._ensure_leverage(inst_id, pos_side, mq_lev)
        side = "buy" if pos_side == "long" else "sell"
        ord_type = "ioc"

        resp = self.open_position(
            inst_id=inst_id,
            price=float(exec_px),
            size=float(sz_ct),
            side=side,
            mq_lev=mq_lev,
            size_unit="contracts",
            ord_type=ord_type,
        )
        return resp
    # ============================================================
    #  交易对解析 / 负缓存 / 过滤
    # ============================================================
    def _is_filtered_coin(self, coin: str) -> bool:
        return coin.upper() in getattr(self, "FILTER_TOKENS", set())

    def _skip_miss_coin(self, coin: str) -> bool:
        ts = self._pair_miss.get(coin.upper())
        return (ts is not None) and (time.time() - ts < self.MISS_TTL)

    def _note_miss_coin(self, coin: str) -> None:
        coin = coin.upper()
        self._pair_miss[coin] = time.time()
        last = self._miss_log_ts.get(coin, 0.0)
        if time.time() - last > 300:
            # 降级到 info 频率控制下的单点日志，其余路径降到 debug
            logging.info("未找到 %s 对应的 SWAP 交易对（将在 %ds 内不再查询）", coin, int(self.MISS_TTL))
            self._miss_log_ts[coin] = time.time()

    def query_pairs(self, keyword: str, inst_type: str = "SWAP") -> list:
        """
        查询交易对（默认合约 SWAP），按关键字过滤。
        只选 USDT 本位永续，且 base 精确匹配。
        """
        base = keyword.upper()
        try:
            if hasattr(self.public_api, "get_instruments"):
                direct = self._call_api(
                    self.public_api.get_instruments,
                    instType=inst_type, instId=f"{base}-USDT-SWAP"
                )
                if str((direct or {}).get("code", "")) == "0" and (direct or {}).get("data"):
                    # 直查命中：立刻写入 pair cache
                    try:
                        inst_id = ((direct or {}).get("data") or [{}])[0].get("instId") or f"{base}-USDT-SWAP"
                        inst_id = inst_id.upper()
                        if inst_id:
                            self._pair_cache[base] = inst_id
                            self._pair_miss.pop(base, None); self._miss_log_ts.pop(base, None)
                    except Exception:
                        pass
                    logging.debug("[PAIR_LOOKUP] direct hit %s", f"{base}-USDT-SWAP")
                    return (direct or {}).get("data") or []
        except Exception as e:
            logging.debug("[PAIR_LOOKUP] direct instId lookup failed: %s", e)

        # ② 再回落到全量列表/行情列表
        try:
            if hasattr(self.public_api, "get_instruments"):
                resp = self._call_api(self.public_api.get_instruments, instType=inst_type)
                logging.debug("[PAIR_LOOKUP] 使用 get_instruments() instType=%s", inst_type)
            elif hasattr(self.market_api, "get_tickers"):
                resp = self._call_api(self.market_api.get_tickers, instType=inst_type)
                logging.debug("[PAIR_LOOKUP] get_instruments 不存在，降级到 get_tickers() instType=%s", inst_type)
            else:
                logging.error("MarketAPI 既无 get_instruments 也无 get_tickers —— 请检查 python-okx 版本")
                return []
        except Exception as e:
            logging.exception("MarketAPI 查询失败: %s", e)
            return []
        data = (resp or {}).get("data", []) or []
        results = []
        for item in data:
            inst = item.get("instId", "").upper()
            if not inst.endswith("USDT-SWAP"):
                continue
            parts = inst.split("-")
            if len(parts) >= 3 and parts[0] == base:
                results.append(item)
        return results

    def _get_inst_id(self, coin: str) -> str | None:
        coin = coin.upper()
        if self._is_filtered_coin(coin):
            return None
        if self._skip_miss_coin(coin):
            ttl = max(0, int(self.MISS_TTL - (time.time() - self._pair_miss.get(coin, 0))))
            # 降级为 debug，避免刷屏
            logging.debug("[INST_ID][MISS_CACHE] coin=%s still in TTL ~%ss", coin, ttl)
            return None
        inst_id = self._pair_cache.get(coin)
        if inst_id:
            logging.debug("[INST_ID][HIT] coin=%s -> instId=%s (cache)", coin, inst_id)
            return inst_id
        pairs = self.query_pairs(coin, inst_type="SWAP")
        if not pairs:
            # 额外探针 ①：直接查 instruments 指定 instId（判断“接口成功但确实没有”）
            ok_inst = False
            try:
                d = self._call_api(self.public_api.get_instruments, instType="SWAP", instId=f"{coin}-USDT-SWAP")
                if str((d or {}).get("code", "")) == "0":
                    ok_inst = True
                    dat = (d or {}).get("data") or []
                    if dat:
                        inst_id = (dat[0].get("instId") or f"{coin}-USDT-SWAP").upper()
                        self._pair_cache[coin] = inst_id
                        self._pair_miss.pop(coin, None); self._miss_log_ts.pop(coin, None)
                        logging.debug("[INST_ID][DIRECT_HIT] coin=%s -> %s", coin, inst_id)
                        return inst_id
            except Exception as e:
                logging.debug("[INST_ID][DIRECT_PROBE_FAIL] %s", e)

            # 额外探针 ②：若标记价查询能返回，说明合约存在，避免误记 MISS。
            ok_probe = False
            try:
                probe = self._call_api(
                    self.public_api.get_mark_price,
                    instType="SWAP", instId=f"{coin}-USDT-SWAP"
                )
                if str((probe or {}).get("code", "")) == "0":
                    ok_probe = True
                if (probe or {}).get("data"):
                    inst_id = f"{coin}-USDT-SWAP"
                    self._pair_cache[coin] = inst_id
                    logging.debug("[INST_ID][PROBE_HIT] coin=%s -> instId=%s (via mark_price)", coin, inst_id)
                    self._pair_miss.pop(coin, None); self._miss_log_ts.pop(coin, None)
                    return inst_id
            except Exception as e:
                logging.debug("[INST_ID][PROBE_FAIL] %s", e)
            # 热身期：不记负缓存，避免冷启动阶段“误伤”
            try:
                if (time.time() - float(getattr(self, "_start_ts", time.time()))) < float(self.WARMUP_SEC or 0.0):
                    logging.debug("[INST_ID][WARMUP_SKIP] coin=%s (skip negative cache during warmup %.1fs)",
                                  coin, float(self.WARMUP_SEC or 0.0))
                    return None
            except Exception:
                pass

            # 仅当「两次探针都成功返回」但确实没有数据时，才记 miss
            if ok_inst and ok_probe:
                logging.info("[INST_ID][MISS] coin=%s no USDT-SWAP found (record miss)", coin)
                self._note_miss_coin(coin)
            try:
                self._monitored_coin[coin] = False
                self._monitor_last[coin] = {
                    "monitored": False, "reason": "no_swap_on_okx",
                    "ts": time.time(), "coin": coin, "instId": ""
                }
                for k in list(self._vp_levels.keys()):
                    inst = k.split(":")[0]
                    if inst.startswith(f"{coin}-") and k.endswith((":long", ":short")):
                        # 清理该币前缀的 VP/HVN/LVN（即使在 infra 层，清状态是无害的）
                        self._vp_levels.pop(k, None)
            except Exception:
                pass
            return None
        inst_id = pairs[0]["instId"]
        self._pair_cache[coin] = inst_id
        logging.debug("[INST_ID][HIT] coin=%s -> instId=%s", coin, inst_id)
        self._pair_miss.pop(coin, None)
        self._miss_log_ts.pop(coin, None)
        return inst_id

    # ============================================================
    #  价格/波动缓存（来自 WS/MQ 的低延迟价）+ 安全 REST 兜底
    # ============================================================
    def _get_atr_state(self, inst_id: str) -> dict | None:
        """读取 TTL 内的 ATR% 缓存（由 MQ 的 vol 字段写入）。对 _vol_ttl 做容错默认。"""
        now = time.time()
        st = self._vol_cache.get(inst_id)
        # 若 trade.py 尚未设置 _vol_ttl，则使用 20s 的安全默认值
        ttl = float(getattr(self, "_vol_ttl", 20.0))
        if st and (now - st.get("ts", 0.0) < max(0.0, ttl)):
            return st
        return None

    def _budget_scale_by_vol(self, inst_id: str) -> float:
        """Risk-parity 近似：ATR 越大预算越小；返回 [min,max] 区间内的缩放系数。"""
        st = self._get_atr_state(inst_id)
        if not st:
            return 1.0
        atr = float(st.get("atr_pct") or 0.0)
        if atr <= 0:
            return 1.0
        return max(self.BUDGET_VOL_MIN, min(self.BUDGET_VOL_MAX, self.BUDGET_TARGET_ATR / atr))

    def _update_px_cache(self, inst_id: str, last: float | None = None,
                         bid: float | None = None, ask: float | None = None,
                         ts: float | None = None) -> None:
        """把来自 MQ/WS 的价推入缓存。任何一个字段存在即可更新。"""
        if not inst_id:
            return
        rec = self._px_cache.get(inst_id, {})
        if last is not None:
            with suppress(Exception):
                rec["last"] = float(last)
        if bid is not None:
            with suppress(Exception):
                rec["bid"] = float(bid)
        if ask is not None:
            with suppress(Exception):
                rec["ask"] = float(ask)
        tnow = time.time()
        if ts is None:
            ts = tnow
        else:
            with suppress(Exception):
                ts = float(ts)
                if ts > 1e12:
                    ts = ts / 1000.0
        rec["ts"] = float(ts) if ts else tnow
        self._px_cache[inst_id] = rec

    def _maybe_update_px_cache_from_evt(self, evt: dict, inst_id: str | None = None) -> None:
        """从事件里提取 mid/last/bbo 更新缓存（若带有 price & ts）。"""
        try:
            inst = (inst_id or evt.get("instId") or "").upper()
            if not inst:
                return
            ts = evt.get("ts") or evt.get("timestamp") or evt.get("T") or evt.get("t")
            last = evt.get("midPx") or evt.get("last") or evt.get("px")
            bid = evt.get("bid") or (evt.get("bbo") or {}).get("bid")
            ask = evt.get("ask") or (evt.get("bbo") or {}).get("ask")
            if last is None and (bid is None or ask is None):
                return
            self._update_px_cache(inst, last=last, bid=bid, ask=ask, ts=ts)
        except Exception:
            pass

    def _fresh_evt_px(self, e: dict) -> float | None:
        """若事件时间戳足够新（≤ WS_PX_TTL），提取一个可用价。"""
        try:
            ts = e.get("ts") or e.get("timestamp") or e.get("T") or e.get("t")
            if ts is None:
                return None
            ts = float(ts);  ts = ts / 1000.0 if ts > 1e12 else ts
            if (time.time() - ts) > max(0.0, float(self.WS_PX_TTL)):
                return None
            v = e.get("midPx") or e.get("last") or e.get("px")
            b = e.get("bid") or (e.get("bbo") or {}).get("bid")
            a = e.get("ask") or (e.get("bbo") or {}).get("ask")
            if v is not None:
                v = float(v)
                return v if v > 0 else None
            if b is not None and a is not None:
                return (float(b) + float(a)) / 2.0
        except Exception:
            return None
        return None

    def _safe_last_price(self, inst_id: str) -> float | None:
        """
        可靠地取最新价；优先使用 WS/MQ 缓存（低延迟），过期或缺失再回退 REST。
        """
        try:
            rec = self._px_cache.get(inst_id)
            if rec:
                ts = float(rec.get("ts", 0.0) or 0.0)
                if ts > 0 and (time.time() - ts) <= max(0.0, float(self.WS_PX_TTL)):
                    if "last" in rec:
                        px = float(rec["last"])
                        if px > 0:
                            return px
                    if "bid" in rec and "ask" in rec:
                        b = float(rec["bid"]); a = float(rec["ask"])
                        mid = (a + b) / 2.0
                        if mid > 0:
                            return mid
        except Exception:
            pass
        try:
            tick = self._call_api(self.market_api.get_ticker, instId=inst_id)
            data = (tick or {}).get("data") or []
            if data:
                try:
                    px = float((data[0].get("last") or data[0].get("lastPx") or data[0].get("px") or 0) or 0)
                except Exception:
                    px = 0.0
                if px > 0:
                    return px
            # fallthrough to mark-price fallback
        except Exception:
            # swallow and try mark-price fallback next
            pass
        # NEW Fallback: 若 ticker 也未取到，尝试标记价作为“安全价”兜底
        try:
            r = self._call_api(self.public_api.get_mark_price, instType="SWAP", instId=inst_id)
            data = (r or {}).get("data") or []
            if data:
                mp = 0.0
                try:
                    mp = float((data[0].get("markPx") or 0) or 0)
                except Exception:
                    mp = 0.0
                return mp if mp > 0 else None
        except Exception:
            pass
        return None
    # ============================================================
    #  特征快照订阅（q.features.snapshot）→ 缓存 ATR%、多时段 VP、邻近 HVN/LVN、以及价
    # ============================================================
    def _start_feature_feed(self) -> None:
        t = threading.Thread(target=self._feature_feed, name="feat-feed", daemon=True)
        t.start()

    def _feature_feed(self) -> None:
        """
        消费指标服务直投的快照（默认 q.features.snapshot）。
        仅缓存必要字段：vol / profile_multi / profile_nodes / features 等，并尝试更新价缓存。
        日志已降噪：默认仅输出启动与退出异常；细节日志置于 DEBUG，另提供原始字节采样输出。
        环境变量：
        - FEATURE_FEED_DEBUG: 1/true 时开启详细 DEBUG 日志（默认关闭）
        - FEATURE_FEED_RAW_SAMPLE_N: 原始字节日志采样间隔 N（N>0 时每 N 条打印一次）
        """

        DEBUG = os.getenv("FEATURE_FEED_DEBUG", "").lower() in {"1", "true", "yes", "y", "debug"}
        RAW_SAMPLE_N = int(os.getenv("FEATURE_FEED_RAW_SAMPLE_N", "0"))  # 0=不打印原始字节
        msg_count = 0

        try:
            cred = pika.PlainCredentials(
                os.getenv("RABBIT_USER", "monitor"),
                os.getenv("RABBIT_PASSWORD", "P@ssw0rd"),
            )
            params = pika.ConnectionParameters(
                host=os.getenv("RABBIT_HOST", "127.0.0.1"),
                port=int(os.getenv("RABBIT_PORT", "5672")),
                virtual_host=os.getenv("RABBIT_VHOST", "/"),
                heartbeat=30,
                blocked_connection_timeout=30,
                credentials=cred,
            )
            conn = pika.BlockingConnection(params)
            ch = conn.channel()
            ch.basic_qos(prefetch_count=64)
            qname = os.getenv("FEATURE_Q", "q.features.snapshot")

            def _on_msg(ch_, method, props, body):
                nonlocal msg_count
                msg_count += 1
                try:
                    # 原始字节（采样+DEBUG 才打印）
                    if DEBUG and RAW_SAMPLE_N > 0 and (msg_count % RAW_SAMPLE_N == 0):
                        try:
                            _rk = getattr(method, "routing_key", "?")
                            _dtag = getattr(method, "delivery_tag", "?")
                            logging.debug(
                                "[WIRE][RAW] rk=%s dtag=%s size=%d body[:240]=%r",
                                _rk, _dtag, len(body or b""), (body or b"")[:240]
                            )
                        except Exception:
                            pass

                    payload = _json.loads(body.decode("utf-8"))

                    # 顶层 keys（仅 DEBUG）
                    if DEBUG:
                        try:
                            logging.debug("[WIRE][JSON][L1] keys=%s", list(payload.keys())[:12])
                            if isinstance(payload.get("event"), dict):
                                logging.debug("[WIRE][JSON][HAS_EVENT]=True inner_keys=%s",
                                            list(payload["event"].keys())[:12])
                        except Exception:
                            pass

                    # 路径存在性与盘口概览（仅 DEBUG）
                    try:
                        _feat = payload.get("feat") if isinstance(payload.get("feat"), dict) else {}
                        _book = _feat.get("book") if isinstance(_feat.get("book"), dict) else {}
                        if DEBUG:
                            _bb  = _book.get("best_bid")
                            _ba  = _book.get("best_ask")
                            _l5b = len(((_book.get("l5") or {}).get("bids") or [])) if isinstance(_book.get("l5"), dict) else 0
                            _l5a = len(((_book.get("l5") or {}).get("asks") or [])) if isinstance(_book.get("l5"), dict) else 0
                            _l10b = len(((_book.get("l10") or {}).get("bids") or [])) if isinstance(_book.get("l10"), dict) else 0
                            _l10a = len(((_book.get("l10") or {}).get("asks") or [])) if isinstance(_book.get("l10"), dict) else 0
                            logging.debug(
                                "[WIRE][PATH] feat=%s feat.book=%s bbo(bb,ba)=(%s,%s) l5=%d/%d l10=%d/%d seqId=%s ts=%s",
                                isinstance(_feat, dict), isinstance(_book, dict),
                                str(_bb), str(_ba), _l5b, _l5a, _l10b, _l10a,
                                str(_book.get("seqId")), str(payload.get("ts"))
                            )
                    except Exception:
                        pass

                    snap = payload.get("feat") or payload
                    inst = (
                        snap.get("instId") or payload.get("instId") or
                        payload.get("inst_id") or payload.get("symbol") or
                        payload.get("coin") or payload.get("base")
                    )
                    inst = inst.upper() if inst else ""
                    if inst:
                        snap.setdefault("instId", inst)

                    # instId 来源（仅 DEBUG）
                    if DEBUG:
                        try:
                            _src_keys = [k for k in ("instId", "inst_id", "symbol", "coin", "base")
                                        if (snap.get(k) is not None) or (payload.get(k) is not None)]
                            logging.debug("[WIRE][INST] inst=%s src_keys=%s", inst, _src_keys)
                        except Exception:
                            pass

                    if not inst:
                        ch_.basic_ack(delivery_tag=method.delivery_tag)
                        return

                    # 统一 ts 为秒（内存）
                    import time as _time
                    _ts_outer = payload.get("ts")
                    try:
                        _ts_s = float(_ts_outer) if _ts_outer is not None else _time.time()
                    except Exception:
                        _ts_s = _time.time()
                    if _ts_s > 10_000_000_000:  # ms -> s
                        _ts_s = _ts_s / 1000.0

                    # 处理 book，写入 bbo_cache
                    _book = snap.get("book") if isinstance(snap.get("book"), dict) else None
                    _norm_book = None
                    if isinstance(_book, dict) and _book:
                        def _f(x):
                            try:
                                v = float(x)
                                return v if v == v else None
                            except Exception:
                                return None

                        bb = _book.get("best_bid"); ba = _book.get("best_ask")
                        if (bb is None or ba is None) and isinstance(_book.get("l1"), dict):
                            _bid = _book["l1"].get("bid"); _ask = _book["l1"].get("ask")
                            if _bid is not None: bb = (_bid[0] if isinstance(_bid, (list,tuple)) and _bid else _bid) if bb is None else bb
                            if _ask is not None: ba = (_ask[0] if isinstance(_ask, (list,tuple)) and _ask else _ask) if ba is None else ba
                        if bb is None: bb = _book.get("bestBid")
                        if ba is None: ba = _book.get("bestAsk")
                        if bb is None and isinstance(_book.get("bids"), list) and _book["bids"]:
                            _topb = _book["bids"][0]; bb = _topb[0] if isinstance(_topb,(list,tuple)) and _topb else _topb
                        if ba is None and isinstance(_book.get("asks"), list) and _book["asks"]:
                            _topa = _book["asks"][0]; ba = _topa[0] if isinstance(_topa,(list,tuple)) and _topa else _topa

                        bsz = _book.get("best_bid_size") or _book.get("bidSz")
                        asz = _book.get("best_ask_size") or _book.get("askSz")
                        if bsz is None and isinstance(_book.get("bids"), list) and _book["bids"]:
                            _first = _book["bids"][0]
                            if isinstance(_first,(list,tuple)) and len(_first) > 1: bsz = _first[1]
                        if asz is None and isinstance(_book.get("asks"), list) and _book["asks"]:
                            _first = _book["asks"][0]
                            if isinstance(_first,(list,tuple)) and len(_first) > 1: asz = _first[1]

                        _norm_book = {
                            "best_bid": _f(bb),
                            "best_ask": _f(ba),
                            "best_bid_size": _f(bsz),
                            "best_ask_size": _f(asz),
                            "bids": _book.get("bids") or ((_book.get("l10") or {}).get("bids")) or ((_book.get("l5") or {}).get("bids")),
                            "asks": _book.get("asks") or ((_book.get("l10") or {}).get("asks")) or ((_book.get("l5") or {}).get("asks")),
                            "l1": _book.get("l1"),
                            "l5": _book.get("l5"),
                            "l10": _book.get("l10"),
                            "seqId": _book.get("seqId"),
                            "ts": _ts_s,
                        }

                        prev = self._bbo_cache.get(inst)
                        self._bbo_cache[inst] = _norm_book
                        # 仅首次看到该 inst 或 DEBUG 时打印一次盘口
                        if prev is None or DEBUG:
                            try:
                                logging.info(
                                    "[BOOK][CACHED] inst=%s src=feed bb=%.8f ba=%.8f l5=%s/%s l10=%s/%s seqId=%s",
                                    inst,
                                    float(_norm_book["best_bid"]) if _norm_book["best_bid"] is not None else float("nan"),
                                    float(_norm_book["best_ask"]) if _norm_book["best_ask"] is not None else float("nan"),
                                    len(((_norm_book.get("l5") or {}).get("bids") or [])) if isinstance(_norm_book.get("l5"), dict) else 0,
                                    len(((_norm_book.get("l5") or {}).get("asks") or [])) if isinstance(_norm_book.get("l5"), dict) else 0,
                                    len(((_norm_book.get("l10") or {}).get("bids") or [])) if isinstance(_norm_book.get("l10"), dict) else 0,
                                    len(((_norm_book.get("l10") or {}).get("asks") or [])) if isinstance(_norm_book.get("l10"), dict) else 0,
                                    str(_norm_book.get("seqId")),
                                )
                            except Exception:
                                pass

                    # 更新价缓存（仅 DEBUG 打点）
                    try:
                        last_px = snap.get("midPx") or snap.get("last") or snap.get("px")
                        bid = snap.get("bid") or (snap.get("bbo") or {}).get("bid")
                        ask = snap.get("ask") or (snap.get("bbo") or {}).get("ask")
                        ts = payload.get("ts")  # ms
                        if last_px is not None or (bid is not None and ask is not None):
                            self._update_px_cache(inst, last=last_px, bid=bid, ask=ask, ts=ts)
                            if DEBUG:
                                logging.debug("[WIRE][PX] update_px_cache inst=%s last=%s bid=%s ask=%s ts=%s",
                                            inst, str(last_px), str(bid), str(ask), str(ts))
                    except Exception:
                        pass

                    # 写入 feat_cache（仅 DEBUG 打点）
                    _cache_entry = {
                        "vol": snap.get("vol") or {},
                        "profile_multi": snap.get("profile_multi") or {},
                        "profile_nodes": snap.get("profile_nodes") or {},
                        "features": snap.get("features") or {},
                        "cvd": snap.get("cvd"),
                        "vpin": snap.get("vpin"),
                        "vpin_risk": snap.get("vpin_risk"),
                        "ts": payload.get("ts") or int(_ts_s * 1000),  # ms
                    }
                    if _norm_book:
                        _cache_entry["book"] = _norm_book
                    self._feat_cache[inst] = _cache_entry
                    # NEW: 将 ATR% 写入 _vol_cache（供 _dynamic_entry_slip 使用）
                    try:
                        vol_obj = snap.get("vol") if isinstance(snap.get("vol"), dict) else {}
                        # 兼容不同字段名：atr_pct / atrPercent / atr_pc
                        atr_val = (
                            vol_obj.get("atr_pct")
                            or vol_obj.get("atrPercent")
                            or vol_obj.get("atr_pc")
                        )
                        if atr_val is not None:
                            self._vol_cache[inst] = {"atr_pct": float(atr_val), "ts": time.time()}
                    except Exception:
                        pass
                    if DEBUG:
                        try:
                            _has_vol  = isinstance(snap.get("vol"), dict)
                            _has_pm   = isinstance(snap.get("profile_multi"), dict)
                            _has_pn   = isinstance(snap.get("profile_nodes"), (list, dict))
                            _has_book = isinstance(snap.get("book"), dict)
                            logging.debug(
                                "[WIRE][CACHE][PUT] inst=%s vol=%s pm=%s nodes=%s book_in_snap=%s ts=%s",
                                inst, _has_vol, _has_pm, _has_pn, _has_book, str(snap.get("ts"))
                            )
                            # 预览盘口（仅 DEBUG）
                            if _has_book:
                                _bb = snap["book"].get("best_bid"); _ba = snap["book"].get("best_ask")
                                logging.debug("[WIRE][BBO][PREVIEW] inst=%s best_bid=%s best_ask=%s",
                                            inst, str(_bb), str(_ba))
                        except Exception:
                            pass

                    # 避免 peek 日志刷屏：仅 DEBUG 调用
                    if DEBUG:
                        try:
                            peek_features_snapshot(snap=snap, raw=False)
                        except Exception:
                            pass

                except Exception as e:
                    logging.debug("[FEAT][DROP] %s", e, exc_info=DEBUG)
                finally:
                    ch_.basic_ack(delivery_tag=method.delivery_tag)

            ch.basic_consume(queue=qname, on_message_callback=_on_msg, auto_ack=False)
            logging.info("[FEAT] consuming %s ...", qname)
            ch.start_consuming()
        except Exception as e:
            logging.warning("[FEAT] feature feed exited: %s", e)

    #HJJ
    # ============================================================
    #  特征摄取：把事件里带来的 ATR/VP/HVN/LVN 写入本地缓存
    # ============================================================
    def _ingest_features_from_evt(self, inst_id: str, evt: dict, ttl_sec: float = 180.0) -> None:
        """将事件中的 vol/profile/profile_multi/profile_nodes 摄取进 _feat_cache。"""
        if not inst_id or not isinstance(evt, dict):
            return
        snap = self._feat_cache.get(inst_id, {})
        changed = False

        # --- NEW: 允许 feature.snapshot 外层 "feat" 包装 ---
        root = evt.get("feat") if isinstance(evt.get("feat"), dict) else evt

        # 0) BBO/订单簿（优先扁平 best_*，其次 l1/l5/l10，再次 bids/asks 顶档 & 交易所别名）
        try:
            feat = evt.get("feat") or {}
            book = (feat.get("book") or evt.get("book")) or {}
            if isinstance(book, dict) and book:
                def _f(x):
                    try:
                        v = float(x)
                        return v if v == v else None  # NaN 过滤
                    except Exception:
                        return None
                # 归一化 best_bid/ask：若缺失则从 l1 补
                bb = book.get("best_bid")
                ba = book.get("best_ask")
                if (bb is None or ba is None) and isinstance(book.get("l1"), dict):
                    bid = book["l1"].get("bid"); ask = book["l1"].get("ask")
                    # l1 既可能是 [px, sz] 也可能是 float
                    if bid is not None:
                        bb = (bid[0] if isinstance(bid, (list, tuple)) and bid else bid) if bb is None else bb
                    if ask is not None:
                        ba = (ask[0] if isinstance(ask, (list, tuple)) and ask else ask) if ba is None else ba
                # 再兜底：交易所/别名字段
                if bb is None: bb = book.get("bestBid")
                if ba is None: ba = book.get("bestAsk")
                if bb is None: bb = book.get("bidPx")
                if ba is None: ba = book.get("askPx")
                # 再兜底：从 bids/asks 顶档推导
                if bb is None and isinstance(book.get("bids"), list) and book["bids"]:
                    topb = book["bids"][0]
                    bb = topb[0] if isinstance(topb, (list, tuple)) and topb else topb
                if ba is None and isinstance(book.get("asks"), list) and book["asks"]:
                    topa = book["asks"][0]
                    ba = topa[0] if isinstance(topa, (list, tuple)) and topa else topa
                bb = _f(bb); ba = _f(ba)
                if bb is not None and ba is not None:
                    # 亦尝试从别名/顶档补 size
                    bsz = book.get("best_bid_size"); asz = book.get("best_ask_size")
                    if bsz is None: bsz = book.get("bidSz")
                    if asz is None: asz = book.get("askSz")
                    if bsz is None and isinstance(book.get("bids"), list) and book["bids"]:
                        first = book["bids"][0]
                        if isinstance(first, (list, tuple)) and len(first) > 1:
                            bsz = first[1]
                    if asz is None and isinstance(book.get("asks"), list) and book["asks"]:
                        first = book["asks"][0]
                        if isinstance(first, (list, tuple)) and len(first) > 1:
                            asz = first[1]
                    # 统一时间戳为“秒”
                    _ts = evt.get("ts")
                    try:
                        _ts = float(_ts) if _ts is not None else time.time()
                    except Exception:
                        _ts = time.time()
                    if _ts > 10_000_000_000:  # 若是毫秒，转成秒
                        _ts = _ts / 1000.0
                    norm = {
                        "best_bid": bb,
                        "best_ask": ba,
                        "best_bid_size": _f(bsz),
                        "best_ask_size": _f(asz),
                        "bids": book.get("bids") or ((book.get("l10") or {}).get("bids")) or ((book.get("l5") or {}).get("bids")),
                        "asks": book.get("asks") or ((book.get("l10") or {}).get("asks")) or ((book.get("l5") or {}).get("asks")),
                        "l1": book.get("l1"),
                        "l5": book.get("l5"),
                        "l10": book.get("l10"),
                        "seqId": book.get("seqId"),
                        # 事件可能没有 ts；用当前时间兜底，便于 TTL 过滤
                        "ts": _ts,
                    }
                    # 写入两处：①快速通道 bbo_cache；②随 feat_cache 一起过期的 book 快照
                    self._bbo_cache[inst_id] = norm
                    snap["book"] = norm
                    changed = True
        except Exception:
            pass

        # 1) 波动/ATR
        feat = evt.get("feat") or {}
        vol = evt.get("vol") or feat.get("vol")
        if isinstance(vol, dict) and vol:
            snap["vol"] = vol
            changed = True
            # NEW: 同步把 ATR% 写入 _vol_cache（秒级 ts，用于 _get_atr_state）
            try:
                atr_val = (
                    vol.get("atr_pct")
                    or vol.get("atrPercent")
                    or vol.get("atr_pc")
                )
                if atr_val is not None:
                    self._vol_cache[inst_id] = {
                        "atr_pct": float(atr_val),
                        "ts": time.time(),
                    }
            except Exception:
                pass
        # 2) 多时段 VP（常见字段名；做几种兼容）
        pm = (evt.get("profile_multi")
              or (evt.get("profile") or {}).get("multi")
              or (evt.get("profile") or {}).get("volume_multi")
              or feat.get("profile_multi")
              or (feat.get("profile") or {}).get("multi")
              or (feat.get("profile") or {}).get("volume_multi")
              or evt.get("profile"))
        if isinstance(pm, dict) and pm:
            snap["profile_multi"] = pm
            changed = True
            # 调试：统计 hvn/lvn 数，确认摄取到 VP
            try:
                hvn_cnt = sum(len(((v.get("volume") or v).get("hvn") or []))
                              for v in pm.values() if isinstance(v, dict))
                lvn_cnt = sum(len(((v.get("volume") or v).get("lvn") or []))
                              for v in pm.values() if isinstance(v, dict))
                logging.info("[FEAT][INGEST] inst=%s got profile_multi hvn=%d lvn=%d keys=%s",
                              inst_id, hvn_cnt, lvn_cnt, list(pm.keys())[:5])
            except Exception:
                pass
        # 3) 已计算好的节点（若上游直接下发）
        pn = evt.get("profile_nodes") or feat.get("profile_nodes")
        if pn:
            snap["profile_nodes"] = pn
            changed = True

        if changed:
            snap["ts"] = time.time()
            self._feat_cache[inst_id] = snap

        # 轻量过期清理（可选）
        try:
            now = time.time()
            stale = [
                k for k, v in self._feat_cache.items()
                if isinstance(v, dict) and (now - float(v.get("ts", 0))) > float(ttl_sec)
            ]
            for k in stale:
                self._feat_cache.pop(k, None)
            # 顺带清理过期的 BBO 缓存
            stale_bbo = [
                k for k, v in self._bbo_cache.items()
                if not isinstance(v, dict) or (now - float(v.get("ts", 0))) > float(self._bbo_cache_ttl)
            ]
            for k in stale_bbo:
                self._bbo_cache.pop(k, None)
        except Exception:
            pass
    def _merge_feat_into_event(self, inst_id: str, evt: dict) -> None:
        """
        将缓存的最新特征并入当前事件（仅当事件缺失该字段时才写入）。
        若缓存缺失：先尝试本地快照窥视，再回退一次 RPC 拉取（避免热路径长期拿不到 HVN/LVN）。
        """

        # --- NEW: 若当前事件自身带有 feat 包装，先就地扁平化一次 ---
        if isinstance(evt.get("feat"), dict):
            feat = evt["feat"]
            evt.setdefault("vol", feat.get("vol"))
            pm = (feat.get("profile_multi")
                  or (feat.get("profile") or {}).get("multi")
                  or (feat.get("profile") or {}).get("volume_multi")
                  or feat.get("profile"))
            if isinstance(pm, dict):
                evt.setdefault("profile_multi", pm)
            if feat.get("profile_nodes"):
                evt.setdefault("profile_nodes", feat["profile_nodes"])
            # 扁平化 book，不覆盖顶层已有字段
            if isinstance(feat.get("book"), dict):
                src = feat["book"]
                evt.setdefault("book", src)
        snap = self._feat_cache.get(inst_id) or {}
        source = "cache" if snap else None
        # 先定义工具函数（上移，避免先用后定义）
        def _flatten_feat_snapshot(s: dict) -> dict:
            out = dict(s) if isinstance(s, dict) else {}
            f = out.get("feat") if isinstance(out.get("feat"), dict) else None
            if f:
                out.setdefault("vol", f.get("vol"))
                pm = (f.get("profile_multi")
                      or (f.get("profile") or {}).get("multi")
                      or (f.get("profile") or {}).get("volume_multi")
                      or f.get("profile"))
                if isinstance(pm, dict):
                    out.setdefault("profile_multi", pm)
                if isinstance(f.get("profile_nodes"), (list, dict)):
                    out.setdefault("profile_nodes", f.get("profile_nodes"))
                if isinstance(f.get("book"), dict):
                    out.setdefault("book", f.get("book"))
            return out

        # 若缓存里还是“feat.*”结构，先扁平化并回写缓存；顺便把 book 同步进 bbo_cache
        def _norm_ts_s(x):
            try:
                t = float(x)
            except Exception:
                return time.time()
            return (t / 1000.0) if t > 10_000_000_000 else t
        # 若缓存里还是“feat.*”结构，先扁平化并回写缓存；顺便把 book 同步进 bbo_cache
        if isinstance(snap.get("feat"), dict):
            snap_flat = _flatten_feat_snapshot(snap)
            if isinstance(snap_flat.get("book"), dict):
                self._bbo_cache[inst_id] = {**snap_flat["book"], "ts": _norm_ts_s(snap_flat.get("ts") or time.time())}
            snap = snap_flat
            self._feat_cache[inst_id] = {**snap, "ts": time.time()}
        # ⭐️ 关键修复：缓存存在但缺 book 时，也尝试补齐（peek → rpc）
        if not isinstance(snap.get("book"), dict):
            fetched, how = None, None
            try:
                s = peek_features_snapshot(inst_id)
                if isinstance(s, dict) and s:
                    fetched = _flatten_feat_snapshot(s)
                    how = "peek"
            except Exception:
                pass
            if not fetched:
                try:
                    s = rpc_indicator_snapshot(inst_id)
                    if isinstance(s, dict) and s:
                        fetched = _flatten_feat_snapshot(s)
                        how = "rpc"
                except Exception:
                    pass
            if isinstance((fetched or {}).get("book"), dict):
                snap.update({"book": fetched["book"]})
                self._feat_cache[inst_id] = {**snap, "ts": time.time()}
                self._bbo_cache[inst_id] = {**fetched["book"], "ts": _norm_ts_s(fetched.get("ts") or time.time())}
                source = how or source
                logging.info("[BOOK][RECOVER] inst=%s via=%s (cache had no book → filled)", inst_id, how)

        # 回退 1：本地快照窥视（无 I/O）
        if not snap:
            try:
                s = peek_features_snapshot(inst_id)
                if isinstance(s, dict) and s:
                    snap = _flatten_feat_snapshot(s)
                    source = "peek"
                    self._feat_cache[inst_id] = {**snap, "ts": time.time()}
                    if isinstance(snap.get("book"), dict):
                        self._bbo_cache[inst_id] = {**snap["book"], "ts": time.time()}
            except Exception:
                pass

        # 回退 2：必要时做一次 RPC（仅在首次缺失/过期时触发，避免阻塞）
        if not snap:
            try:
                s = rpc_indicator_snapshot(inst_id)
                if isinstance(s, dict) and s:
                    snap = _flatten_feat_snapshot(s)
                    source = "rpc"
                    self._feat_cache[inst_id] = {**snap, "ts": time.time()}
                    if isinstance(snap.get("book"), dict):
                        self._bbo_cache[inst_id] = {**snap["book"], "ts": time.time()}
            except Exception:
                pass

        if not snap:
            return

        evt.setdefault("vol", snap.get("vol"))
        evt.setdefault("profile_multi", snap.get("profile_multi"))
        if snap.get("profile_nodes"):
            evt.setdefault("profile_nodes", snap["profile_nodes"])

        # === 合并 BBO/订单簿到事件顶层（仅在缺失时） ===
        try:
            feat = evt.get("feat") or {}
            # 从本地缓存读取时做 TTL 过滤
            book_cached = None
            bc = self._bbo_cache.get(inst_id)
            if isinstance(bc, dict):
                ts = _norm_ts_s(bc.get("ts", 0))
                if (time.time() - ts) <= float(self._bbo_cache_ttl):
                    book_cached = bc
            # 优先级：事件自带 feat.book/evt.book → bbo_cache(有效期内) → feat_cache.book
            src_book, src_from = None, None
            if isinstance(feat.get("book"), dict):
                src_book, src_from = feat["book"], "feat"
            elif isinstance(evt.get("book"), dict):
                src_book, src_from = evt["book"], "evt"
            elif isinstance(book_cached, dict):
                src_book, src_from = book_cached, "bbo_cache"
            elif isinstance(snap, dict) and isinstance(snap.get("book"), dict):
                src_book, src_from = snap["book"], "feat_cache"
            # 兜底：历史缓存里没有顶层 book，但有 feat.book
            elif isinstance((snap.get("feat") or {}).get("book"), dict):
                src_book, src_from = snap["feat"]["book"], "feat_cache(feat.book)"
            if isinstance(src_book, dict):
                def _put(k, v):
                    if v is None:
                        return
                    if evt.get(k) is None:
                        evt[k] = v
                # 归一化 best_bid/ask（含多重兜底）
                bb = src_book.get("best_bid"); ba = src_book.get("best_ask")
                if (bb is None or ba is None) and isinstance(src_book.get("l1"), dict):
                    bid = src_book["l1"].get("bid"); ask = src_book["l1"].get("ask")
                    if bid is not None:
                        bb = (bid[0] if isinstance(bid, (list, tuple)) and bid else bid) if bb is None else bb
                    if ask is not None:
                        ba = (ask[0] if isinstance(ask, (list, tuple)) and ask else ask) if ba is None else ba
                if bb is None: bb = src_book.get("bestBid")
                if ba is None: ba = src_book.get("bestAsk")
                if bb is None: bb = src_book.get("bidPx")
                if ba is None: ba = src_book.get("askPx")
                if bb is None and isinstance(src_book.get("bids"), list) and src_book["bids"]:
                    topb = src_book["bids"][0]
                    bb = topb[0] if isinstance(topb, (list, tuple)) and topb else topb
                if ba is None and isinstance(src_book.get("asks"), list) and src_book["asks"]:
                    topa = src_book["asks"][0]
                    ba = topa[0] if isinstance(topa, (list, tuple)) and topa else topa
                _put("best_bid", bb); _put("best_ask", ba)
                for k in ("best_bid_size", "best_ask_size", "bids", "asks", "l1", "l5", "l10", "seqId"):
                    v = src_book.get(k)
                    # 亦兼容别名尺寸
                    if v is None and k == "best_bid_size":
                        v = src_book.get("bidSz")
                    if v is None and k == "best_ask_size":
                        v = src_book.get("askSz")
                    # 从顶档补尺寸
                    if v is None and k in ("best_bid_size","best_ask_size"):
                        arr = src_book.get("bids" if k=="best_bid_size" else "asks")
                        if isinstance(arr, list) and arr:
                            first = arr[0]
                            if isinstance(first, (list, tuple)) and len(first) > 1:
                                v = first[1]
                    if v is not None and evt.get(k) is None:
                        evt[k] = v
                # 反写回 feat.book，便于下游统一读取
                evt.setdefault("feat", {})
                if "book" not in evt["feat"]:
                    evt["feat"]["book"] = {
                        k: evt.get(k) for k in
                        ("best_bid","best_ask","best_bid_size","best_ask_size","bids","asks","l1","l5","l10","seqId")
                        if evt.get(k) is not None
                    }
            else:
                logging.info("[BOOK][MISS] inst=%s no src_book (feat/evt/bbo_cache/feat_cache) — cache_hit=%s snap_book=%s",
                             inst_id, isinstance(book_cached, dict), isinstance((snap or {}).get("book") if isinstance(snap, dict) else None, dict))

        except Exception:
            pass
        # —— 可选：打印一次 BBO 合并摘要（每个 instId 至多 3s 打一条，避免刷屏） —— #
        try:
            now = time.time()
            if not hasattr(self, "_book_log_ts"):
                self._book_log_ts = {}
            tkey = f"book_log:{inst_id}"
            last = float(self._book_log_ts.get(tkey, 0))
            if evt.get("best_bid") is not None and evt.get("best_ask") is not None and (now - last) > 3:
                l5 = evt.get("l5") or {}
                l10 = evt.get("l10") or {}
                l5b = len((l5.get("bids") or [])) if isinstance(l5, dict) else 0
                l5a = len((l5.get("asks") or [])) if isinstance(l5, dict) else 0
                l10b = len((l10.get("bids") or [])) if isinstance(l10, dict) else 0
                l10a = len((l10.get("asks") or [])) if isinstance(l10, dict) else 0
                # logging.info("[BOOK][MERGED] inst=%s bb=%.8f ba=%.8f l5=%d/%d l10=%d/%d seqId=%s",
                #              inst_id, float(evt["best_bid"]), float(evt["best_ask"]),
                #              l5b, l5a, l10b, l10a, evt.get("seqId"))
                self._book_log_ts[tkey] = now
            # 未命中时给出来源诊断（升为 INFO）
            elif (now - last) > 3:
                has_feat_book = isinstance((evt.get("feat") or {}).get("book"), dict)
                has_evt_book  = isinstance(evt.get("book"), dict)
                keys = list(((evt.get("feat") or {}).get("book") or evt.get("book") or {}) .keys())[:6]
                logging.info("[BOOK][MISS] inst=%s feat.book=%s evt.book=%s cache_hit=%s keys=%s",
                             inst_id, has_feat_book, has_evt_book,
                             ('book_cached' in locals() and isinstance(book_cached, dict)),
                             keys)
                self._book_log_ts[tkey] = now
        except Exception:
            pass
        # 调试：合并后事件内 VP 简要统计
        try:
            pm = evt.get("profile_multi")
            hvn_cnt = lvn_cnt = 0
            if isinstance(pm, dict):
                hvn_cnt = sum(len(((v.get("volume") or v).get("hvn") or []))
                              for v in pm.values() if isinstance(v, dict))
                lvn_cnt = sum(len(((v.get("volume") or v).get("lvn") or []))
                              for v in pm.values() if isinstance(v, dict))
            # logging.info("[FEAT][MERGE] inst=%s src=%s has_pm=%s hvn=%d lvn=%d",
            #               inst_id, (source or "none"), isinstance(pm, dict), hvn_cnt, lvn_cnt)
        except Exception:
            pass    

    # ============================================================
    #  其它：保证金比率（imr）TTL 缓存与快速读取
    # ============================================================
    def _get_imr_fast(self, inst_id: str, lev: float) -> float:
        """
        以 TTL 缓存方式读取 imr；失败或无数据回退到 1/lev，并与 1/lev 取更严格者。
        返回值已是“有效 imr”（= max(1/lev, imr_from_api)）。
        """
        try:
            now = time.time()
            st = self._tier_cache.get(inst_id)
            if st and (now - float(st.get("ts", 0.0))) < max(0.0, float(self.TIER_TTL)):
                imr_api = float(st.get("imr_api") or st.get("imr") or 0.0)
                return max(1.0 / float(lev or 1.0), imr_api if imr_api > 0 else 0.0)
        except Exception:
            pass
        imr = 0.0
        try:
            _uly = "-".join(inst_id.split("-")[:2])
            r = self._call_api(self.public_api.get_position_tiers, instType="SWAP", uly=_uly, tdMode=self.margin_mode)
            if r.get("code") == "0" and (r.get("data") or []):
                imr = float((r["data"][0].get("imr") or 0) or 0)
        except Exception:
            imr = 0.0
        eff_imr = max(1.0 / float(lev or 1.0), float(imr or 0.0))
        self._tier_cache[inst_id] = {"imr_api": float(imr or 0.0), "ts": time.time()}
        return eff_imr

    # core_infra.py 内
    def _handle_monitor_status(self, evt: dict) -> None:

        # 1) 提取字段（容错）
        inst = (evt.get("instId") or "").upper()
        coin = (evt.get("coin") or evt.get("asset") or "").upper()
        # OK: 既兼容 monitored 又兼容 status
        status = bool(evt.get("monitored") if evt.get("monitored") is not None else evt.get("status"))
        reason = str(evt.get("reason") or "")

        # 若只有 coin 没有 inst，尝试补齐 instId（可选）
        if not inst and coin:
            try:
                inst = (self._get_inst_id(coin) or "").upper()
            except Exception:
                pass

        # 2) 读取旧状态，用于对比
        prev_inst = self._monitored_inst.get(inst) if inst else None
        prev_coin = self._monitored_coin.get(coin) if coin else None

        # 3) 写入最新状态
        if inst:
            self._monitored_inst[inst] = status
        if coin:
            self._monitored_coin[coin] = status

        key = inst or coin or "UNKNOWN"
        rec = {
            "monitored": status,
            "reason": reason,
            "ts": time.time(),
            "coin": coin,
            "instId": inst,
        }
        self._monitor_last[key] = rec

        # 4) 仅在“状态变化”时打 INFO，否则 DEBUG（防止刷屏）
        changed = (
            (inst and prev_inst is not None and prev_inst != status) or
            (coin and prev_coin is not None and prev_coin != status) or
            (prev_inst is None and prev_coin is None)  # 首次也算变化
        )
        logging.info(
            "[MONITOR_STATUS] inst=%s coin=%s -> %s (prev_inst=%s prev_coin=%s) reason=%s evt_keys=%s",
            inst or "-", coin or "-", status, prev_inst, prev_coin, reason, list(evt.keys())
        )

        # 5) 关闭监控时清理 VP/HVN/LVN 状态并记日志
        if not status and inst:
            try:
                self._clear_vp_state(inst)  # 你类里已实现：清理 _vp_levels/_node_* 等
                logging.info("[MONITOR_STATUS][CLEAN] inst=%s cleared VP/HVN/LVN state", inst)
            except Exception as e:
                logging.debug("[MONITOR_STATUS][CLEAN][SKIP] inst=%s reason=%s", inst, e)


    def _bar_sec(self, bar: str) -> int:
        m = {"1s":1,"5s":5,"15s":15,"30s":30,"1m":60,"3m":180,"5m":300,"15m":900,"30m":1800,"1H":3600,"2H":7200}
        return m.get(bar, 60)

    def _recent_high_low(self, inst_id: str, window_sec: int, bar: str):
        """
        最好-effort：尝试从 OKX K 线接口取最近窗口的极值；失败则返回 (None, None)
        注意：不同 SDK 版本的 K线方法名/返回格式可能不同；这里尽量容错。
        """
        try:
            limit = max(1, int(math.ceil(window_sec / self._bar_sec(bar))))
            # 常见方法名：get_candlesticks / get_history_candlesticks（不同版本可能不同）
            getter = getattr(self.market_api, "get_candlesticks", None) or getattr(self.market_api, "get_history_candlesticks", None)
            if not getter:
                return (None, None)
            resp = self._call_api(getter, instId=inst_id, bar=bar, limit=str(limit))
            rows = resp.get("data", []) or []
            highs, lows = [], []
            for r in rows:
                # OKX 一般返回 [ts, o, h, l, c, vol, ...]（字符串）
                if isinstance(r, (list, tuple)) and len(r) >= 4:
                    h = float(r[2]); l = float(r[3])
                elif isinstance(r, dict):
                    h = float(r.get("high") or 0); l = float(r.get("low") or 0)
                else:
                    continue
                if h > 0: highs.append(h)
                if l > 0: lows.append(l)
            return (max(highs) if highs else None, min(lows) if lows else None)
        except Exception:
            return (None, None)

    def _px_tol_pct(self, inst_id: str) -> float:
        """
        价格去重容差（百分比）。优先使用 ATR 推导：tol_pct = k_atr * ATR%。
        k_atr 由 trade.OKXTradingClient.__init__ 读取配置 self._k_atr。
        """
        try:
            st = self._get_atr_state(inst_id) or {}
            atr_pct = float(st.get("atr_pct") or 0.0)
            k = float(getattr(self, "_k_atr", 0.0) or 0.0)
            return max(0.0, k * atr_pct)
        except Exception:
            return 0.0

    # ======== 币数量 <-> 合约张数 换算 ======== #
    def _coin_to_contracts(self, coin_amt: float, ct_val: float, ct_ccy: str, px: float) -> float:
        """
        把 coin 数量换算成合约张数（返回 *浮点值*，允许 <1）
        由调用方决定 round / floor / ceil 方式。
        """
        notional = ct_val if ct_ccy.upper() == "USDT" else ct_val * px
        return coin_amt * px / notional   # 特别注意：如果价格反复在同一位置上涨或回撤，这里注意应该只触发一次


    # ============================================================
    #  同比例减仓（剃单优先 amend，必要时才撤）
    # ============================================================
    def _min_contract_step(self, inst_id: str) -> float:
        """
        取得该合约的“最小下单步长”。优先 lotSz（OKX 下单数量精度），兜底 minSz。
        """
        try:
            _, _, _, min_sz = self._get_inst_spec(inst_id)
            lot_sz = 0.0
            try:
                lot_sz = float((self._spec_cache.get(inst_id) or {}).get("lotSz") or 0.0)
            except Exception:
                lot_sz = 0.0
            step = lot_sz if lot_sz > 0 else float(min_sz or 0.0)
        except Exception:
            step = 0.0
        return max(1e-8, float(step or 0.0))
    # ─────────────────────────────────────────────────────────────
    # NEW: 规格/面值与价格获取 + 币↔张换算小工具
    # ─────────────────────────────────────────────────────────────
    def _inst_notional_per_contract(self, inst_id: str) -> tuple[float, float, str]:
        """
        返回 (notional_per_ct_in_usdt, last_px, base_ccy)
        notional_per_ct = ctVal            (ctValCcy==USDT)
                        = ctVal * last_px (ctValCcy==BASE)
        """
        base_ccy = inst_id.split("-")[0] if "-" in inst_id else ""
        _max_lev, ct_val, ct_ccy, _min_sz = self._get_inst_spec(inst_id)
        last_px = float(self._safe_last_price(inst_id) or 0.0)
        # Fallback: 若没有最新价，尝试使用标记价，避免 notional=0
        if last_px <= 0:
            try:
                r = self._call_api(self.public_api.get_mark_price, instType="SWAP", instId=inst_id)
                last_px = float(((r or {}).get("data") or [{}])[0].get("markPx") or 0) or last_px
            except Exception:
                pass
        if str(ct_ccy).upper() == "USDT":
            notional = float(ct_val)
        else:
            notional = float(ct_val) * (last_px if last_px > 0 else 1.0)
        return float(notional or 0.0), last_px, base_ccy
    def _contracts_from_coin(self, coin_amt: float, inst_id: str) -> float:
        notional, last_px, _ = self._inst_notional_per_contract(inst_id)
        if notional <= 0 or last_px <= 0:
            return 0.0
        return float(coin_amt) * last_px / notional

    def _coins_from_contracts(self, contracts: float, inst_id: str) -> float:
        notional, last_px, _ = self._inst_notional_per_contract(inst_id)
        if notional <= 0 or last_px <= 0:
            return 0.0
        return float(contracts) * notional / last_px

    def _pending_orders_by_side(self, inst_id: str, pos_side: str | None):
        """
        【保持原有语义】但注意：真正的“按量剃单”会走下面的
        _get_open_orders_map/_rebalance_pending_to_target，以满足“改单前快照 + 校验”的要求。
        """
        pendings: list[dict] = []
        unfilled_total = 0.0
        want_side = None
        if pos_side:
            want_side = "buy" if pos_side.lower() == "long" else "sell"

        for od in self.get_pending_orders():
            if (od.get("instId") or "").upper() != inst_id.upper():
                continue
            # 只处理“开仓/加仓”类挂单；保护性 reduceOnly 不动
            if str(od.get("reduceOnly") or "").lower() == "true":
                continue
            # 在双向持仓模式下，仅同向挂单才计入
            if self.pos_mode == "long_short" and pos_side:
                if (od.get("posSide") or "").lower() != pos_side.lower():
                    continue
                if (od.get("side") or "").lower() != want_side:
                    continue
            filled = float(od.get("accFillSz") or 0.0)
            sz = float(od.get("sz") or 0.0)
            unfilled = max(sz - filled, 0.0)
            if unfilled > 0:
                pendings.append(od)
                unfilled_total += unfilled

        # 小未成交在前，更容易“剃”
        pendings.sort(
            key=lambda o: max(float(o.get("sz") or 0.0) - float(o.get("accFillSz") or 0.0), 0.0)
        )
        return pendings, unfilled_total

    def _pending_closes_by_side(self, inst_id: str, pos_side: str) -> tuple[list[dict], float]:
        """
        仅统计“平仓挂单”（reduceOnly=True）的未成交量。
        - 仅匹配同一 instId
        - 仅匹配同一 posSide（'long'/'short'）
        - 方向需为该持仓的平仓方向：long→sell，short→buy
        返回：(挂单列表，小到大按未成交量排序), 未成交总张数
        """
        pendings: list[dict] = []
        total_unfilled = 0.0
        want_side = "sell" if (pos_side or "").lower() == "long" else "buy"

        for od in self.get_pending_orders():
            try:
                if (od.get("instId") or "").upper() != (inst_id or "").upper():
                    continue
                # 只要平仓单
                if str(od.get("reduceOnly") or "").lower() != "true":
                    continue
                # 双向持仓：仅统计同向持仓的平仓单，且方向为平仓方向
                if (self.pos_mode or "").lower() == "long_short":
                    if (od.get("posSide") or "").lower() != (pos_side or "").lower():
                        continue
                    if (od.get("side") or "").lower() != want_side:
                        continue
                filled = float(od.get("accFillSz") or 0.0)
                sz = float(od.get("sz") or 0.0)
                unfilled = max(sz - filled, 0.0)
                if unfilled > 0:
                    pendings.append(od)
                    total_unfilled += unfilled
            except Exception:
                # 单条异常不影响整体统计
                continue

        # 小未成交在前，更容易“剃”
        pendings.sort(
            key=lambda o: max(float(o.get("sz") or 0.0) - float(o.get("accFillSz") or 0.0), 0.0)
        )
        return pendings, total_unfilled

    def _held_contracts_by_side(self, inst_id: str, pos_side: str) -> float:
        """
        OKX 0.4.0 /v5/account/positions 读取持仓张数（仅双向持仓）：
        仅使用官方返回的 `pos`（张数，字符串）与 `posSide`（'long'/'short'）。
        不做任何旧版字段兼容或币数量换算。
        """
        held_ct = 0.0
        inst_id_u = (inst_id or "").upper()
        side_l = (pos_side or "").lower()
        for p in (self.get_positions() or []):
            if (p.get("instId") or "").upper() != inst_id_u:
                continue
            if (p.get("posSide") or "").lower() != side_l:
                continue
            try:
                qty = abs(float(p.get("pos") or 0.0))
            except Exception:
                qty = 0.0
            if qty > 0:
                held_ct += qty
        return float(held_ct)
    # ─────────────────────────────────────────────────────────────
    # 新增：side-key 节流与串行化、快照与重新配比工具
    # ─────────────────────────────────────────────────────────────
    def _side_key(self, inst_id: str, pos_side: str | None) -> str:
        return f"{(inst_id or '').upper()}:{(pos_side or '-').lower()}"

    def _throttle_side_ops(self, inst_id: str, pos_side: str | None) -> None:
        """对同一 inst/posSide 的改单/撤单做 150–300ms 节流（可配），避免热抖。"""
        key = self._side_key(inst_id, pos_side)
        now = time.time()
        next_ok = self._side_next_ts.get(key, 0.0)
        if now < next_ok:
            time.sleep(max(0.0, next_ok - now))
        span = (random.randint(self.DEC_THROTTLE_MIN_MS, self.DEC_THROTTLE_MAX_MS)) / 1000.0
        self._side_next_ts[key] = time.time() + span

    def _get_open_orders_map(self, inst_id: str, pos_side: str | None) -> dict[str, dict]:
        """
        拉未完全成交挂单快照并构建 {ordId: {sz, filled, unfilled, px, side, posSide}}。
        只保留同向开/加仓（非 reduceOnly）。
        """
        mp: dict[str, dict] = {}
        try:
            if hasattr(self.trade_api, "get_orders_pending"):
                r = self._call_api(self.trade_api.get_orders_pending)
            else:
                r = self._call_api(self.trade_api.get_order_list)
            for od in (r or {}).get("data", []) or []:
                if (od.get("instId") or "").upper() != (inst_id or "").upper():
                    continue
                if str(od.get("reduceOnly") or "").lower() == "true":
                    continue
                if self.pos_mode == "long_short" and pos_side:
                    if (od.get("posSide") or "").lower() != (pos_side or "").lower():
                        continue
                    # side 也需要是同向开/加仓
                    want_side = "buy" if (pos_side or "").lower() == "long" else "sell"
                    if (od.get("side") or "").lower() != want_side:
                        continue
                sz = float(od.get("sz") or 0.0)
                filled = float(od.get("accFillSz") or 0.0)
                unfilled = max(0.0, sz - filled)
                if unfilled <= 0:
                    continue
                mp[od["ordId"]] = {
                    "ordId": od["ordId"],
                    "sz": sz, "filled": filled, "unfilled": unfilled,
                    "px": float(od.get("px") or 0.0) if od.get("px") else 0.0,
                    "side": (od.get("side") or "").lower(),
                    "posSide": (od.get("posSide") or "").lower(),
                }
        except Exception:
            pass
        return mp

    def _rebalance_pending_to_target(self, inst_id: str, pos_side: str, target_unfilled: float) -> float:
        """
        按“当前挂单快照”把同向未成交总量重配为 target_unfilled。
        - 对每个 ordId 计算 new_unfilled（按比例、落步长），若 <step 则直接撤单；
          否则 amend 到 new_total = filled + new_unfilled。
        返回：实际“剃掉”的张数（>=0）。
        """
        shaved_total = 0.0
        step = self._min_contract_step(inst_id)
        # 最多跑若干轮（避免极端重试占用过久）
        for _pass in range(self.DEC_REALLOC_MAX_PASSES):
            snap = self._get_open_orders_map(inst_id, pos_side)
            if not snap:
                return shaved_total
            ords = list(snap.values())
            curr_total = sum(o["unfilled"] for o in ords)
            curr_total = math.floor((curr_total + 1e-12) / step) * step
            target_unfilled = max(0.0, math.floor((target_unfilled + 1e-12) / step) * step)
            if curr_total <= target_unfilled + self.DEC_REALLOC_EPS:
                return shaved_total  # 已达标
            # 目标分配：按比例向下取整，剩余误差允许 <step
            ratio = 0.0 if curr_total <= 0 else (target_unfilled / curr_total)
            plan: list[tuple[dict, float]] = []
            for o in ords:
                want_unf = math.floor((o["unfilled"] * ratio + 1e-12) / step) * step
                plan.append((o, want_unf))
            # 执行
            for o, want_unf in sorted(plan, key=lambda x: x[0]["unfilled"]):  # 小单优先
                cur_unf = o["unfilled"]
                if cur_unf <= want_unf + self.DEC_REALLOC_EPS:
                    continue
                shave_here = cur_unf - want_unf
                shave_here = math.floor((shave_here + 1e-12) / step) * step
                if shave_here <= 0:
                    continue
                new_unf = cur_unf - shave_here
                self._throttle_side_ops(inst_id, pos_side)
                with self._side_op_locks[self._side_key(inst_id, pos_side)]:
                    if new_unf < step - 1e-12:
                        # 直接撤单
                        try:
                            self._call_api(self.trade_api.cancel_order, instId=inst_id, ordId=o["ordId"])
                            shaved_total += cur_unf
                            continue
                        except Exception as e:
                            logging.warning("[DEC][REALLOC][CANCEL][FAIL] inst=%s ordId=%s err=%s",
                                            inst_id, o["ordId"], e)
                            # 撤单失败，下一轮再尝试
                            continue
                    else:
                        # 先校验“仍存在且数量未变”
                        curr = self._get_open_orders_map(inst_id, pos_side).get(o["ordId"])
                        if (not curr) or abs(curr["unfilled"] - o["unfilled"]) > 1e-12 or abs(curr["sz"] - o["sz"]) > 1e-12:
                            # 快照已变 → 下一轮再重配
                            continue
                        # amend 到 new_total = filled + new_unf
                        new_total = o["filled"] + new_unf
                        ok = False
                        for attempt in range(1, self.DEC_AMEND_MAX_RETRY + 1):
                            self._throttle_side_ops(inst_id, pos_side)
                            resp = self._amend_order_size(inst_id, o["ordId"], new_total, pos_side=pos_side)
                            code = str((resp or {}).get("code", ""))
                            if code == "0":
                                shaved_total += shave_here
                                ok = True
                                break
                            # code!=0 → 立刻重拉快照判断变化/消失
                            after = self._get_open_orders_map(inst_id, pos_side).get(o["ordId"])
                            if (after is None) or abs(after.get("unfilled", -1) - o["unfilled"]) > 1e-12 or abs(after.get("sz", -1) - o["sz"]) > 1e-12:
                                # 状态已变：交给下一轮重配
                                break
                            # 仍存在但失败 → 指数退避 + 抖动
                            backoff = (self.DEC_BACKOFF_BASE_MS * (1.6 ** (attempt - 1)) +
                                       random.randint(0, self.DEC_BACKOFF_JITTER_MS)) / 1000.0
                            time.sleep(backoff)
                        if not ok:
                            # 超出重试仍失败 → cancel +（如需）重下 new_total
                            try:
                                self._throttle_side_ops(inst_id, pos_side)
                                self._call_api(self.trade_api.cancel_order, instId=inst_id, ordId=o["ordId"])
                                # 注意：这里是“减量型”重配，通常不需要“重下”；若你希望保留分布，可在此重下 want_unf
                                shaved_total += cur_unf
                            except Exception as e:
                                logging.warning("[DEC][REALLOC][CANCEL][FAIL-FINAL] inst=%s ordId=%s err=%s",
                                                inst_id, o["ordId"], e)
                                # 放弃本单，下一轮再处理
                                continue
            # 达标则退出；否则做下一轮（快照刷新后再配）
        return shaved_total
    def _amend_order_size(self, inst_id: str, ord_id: str, new_sz: float, pos_side: str | None = None) -> dict:
        """
        使用 OKX amend 修改订单尺寸（newSz 是“新的总数量”，非 delta）。
        - 先按合约步长对齐
        - 注：python-okx 0.4.0 的 amend_order 不支持 posSide，这里统一不传
        - 若步长>=1，newSz 取整；否则保留小数
        """
        amend_fn = getattr(self.trade_api, "amend_order", None)
        if not amend_fn:
            raise RuntimeError("OKX SDK 缺少 amend_order 方法（普通委托）")
        step = self._min_contract_step(inst_id)
        new_sz = math.floor((float(new_sz) + 1e-12) / step) * step
        new_sz_str = str(int(new_sz)) if step >= 1.0 else str(new_sz)
        # 0.4.0 不支持 posSide；方向由原订单 ordId 决定，这里统一不传
        kwargs = {"instId": inst_id, "ordId": ord_id, "newSz": new_sz_str}
        resp = self._call_api(amend_fn, **kwargs)
        code = str((resp or {}).get("code", ""))
        if code != "0":
            logging.warning("[AMEND][FAIL] inst=%s ordId=%s newSz=%s code=%s msg=%s",
                            inst_id, ord_id, new_sz_str, code, (resp or {}).get("msg"))
        else:
            # 刷新订单缓存
            try:
                od = self._call_api(self.trade_api.get_order, instId=inst_id, ordId=ord_id)
                self._record_order(od)
            except Exception:
                pass
        return resp

    def _scaled_target_contracts(self, current_total: float, ratio: float, inst_id: str) -> float:
        """
        将“当前总张数 * 比例”落地到合约步长（通常向下取整保守一些）。
        """
        step = self._min_contract_step(inst_id)
        tgt = max(0.0, float(current_total) * float(ratio))
        # floor 到步长
        tgt = math.floor((tgt + 1e-9) / step) * step
        return tgt
    def _shave_pending_by_amount(self, inst_id: str, pos_side: str, shave_need: float) -> float:
        """
        改进版“按量剃单”：
        - 单次先拉快照；只对“仍存在且数量未变”的 ord 做 amend；
        - amend 失败 → 立即重拉快照校验；数量变/ord 消失 → 放弃本单，交给“重配”；
        - 连续失败超阈值 → cancel（必要时重下）；最后通过“重配”把总量对齐到目标。
        返回：实际剃掉的张数。
        """
        step = self._min_contract_step(inst_id)
        shave_need = math.floor((shave_need + 1e-12) / step) * step
        if shave_need < step - 1e-12:
            return 0.0

        shaved_total = 0.0
        # 第一阶段：基于快照的“直接剃减”
        base = self._get_open_orders_map(inst_id, pos_side)
        # 小单优先
        for ordId, o in sorted(base.items(), key=lambda kv: kv[1]["unfilled"]):
            if shave_need < step - 1e-12:
                break
            cur_unf = o["unfilled"]; filled = o["filled"]; sz = o["sz"]
            permissible = math.floor((cur_unf + 1e-12) / step) * step
            shave_here = min(shave_need, permissible)
            shave_here = math.floor((shave_here + 1e-12) / step) * step
            if shave_here < step:
                continue
            new_unf = cur_unf - shave_here
            # 改前再校验“仍存在且数量未变”
            curr = self._get_open_orders_map(inst_id, pos_side).get(ordId)
            if (not curr) or abs(curr["unfilled"] - cur_unf) > 1e-12 or abs(curr["sz"] - sz) > 1e-12:
                continue  # 快照已变，交给重配

            # 串行化 + 节流
            self._throttle_side_ops(inst_id, pos_side)
            with self._side_op_locks[self._side_key(inst_id, pos_side)]:
                if new_unf < step - 1e-12:
                    # 直接撤单
                    try:
                        self._call_api(self.trade_api.cancel_order, instId=inst_id, ordId=ordId)
                        shaved_total += cur_unf
                        shave_need -= cur_unf
                        continue
                    except Exception as e:
                        logging.warning("[DEC][SHAVE][CANCEL][FAIL] inst=%s ordId=%s err=%s",
                                        inst_id, ordId, e)
                        # 留给“重配”阶段兜底
                        continue
                # amend → new_total = filled + new_unf
                new_total = filled + new_unf
                ok = False
                for attempt in range(1, self.DEC_AMEND_MAX_RETRY + 1):
                    self._throttle_side_ops(inst_id, pos_side)
                    resp = self._amend_order_size(inst_id, ordId, new_total, pos_side=pos_side)
                    code = str((resp or {}).get("code", ""))
                    if code == "0":
                        shaved_total += shave_here
                        shave_need -= shave_here
                        ok = True
                        break
                    # code!=0 → 立刻重拉快照判断变化/消失
                    after = self._get_open_orders_map(inst_id, pos_side).get(ordId)
                    if (after is None) or abs(after.get("unfilled", -1) - cur_unf) > 1e-12 or abs(after.get("sz", -1) - sz) > 1e-12:
                        # 状态已变：交给重配
                        break
                    # 仍存在但失败 → 指数退避 + 抖动
                    backoff = (self.DEC_BACKOFF_BASE_MS * (1.6 ** (attempt - 1)) +
                               random.randint(0, self.DEC_BACKOFF_JITTER_MS)) / 1000.0
                    time.sleep(backoff)
                if not ok:
                    # 超重试仍失败 → cancel，剩余交给“重配”
                    try:
                        self._throttle_side_ops(inst_id, pos_side)
                        self._call_api(self.trade_api.cancel_order, instId=inst_id, ordId=ordId)
                        shaved_total += cur_unf
                        shave_need -= cur_unf
                    except Exception as e:
                        logging.warning("[DEC][SHAVE][CANCEL][FAIL-FINAL] inst=%s ordId=%s err=%s",
                                        inst_id, ordId, e)
                        # 留给重配
                        pass

        # 第二阶段：如仍有缺口 → 基于“当前快照”做重新配比（一次或少量多次）
        if shave_need >= step - 1e-12:
            # 计算“重配”的目标未成交 = 当前未成交总量 - shave_need
            snap_now = self._get_open_orders_map(inst_id, pos_side)
            total_now = math.floor((sum(o["unfilled"] for o in snap_now.values()) + 1e-12) / step) * step
            target_unfilled = max(0.0, total_now - shave_need)
            shaved_total += self._rebalance_pending_to_target(inst_id, pos_side, target_unfilled)
        return shaved_total

    def _reduce_filled_if_needed(self, inst_id: str, pos_side: str, remaining_need: float, held_available: float | None = None) -> float:
        """
        若“剃挂单后”仍需缩量，则对已持仓下 reduceOnly 限价（或你也可以改成市价）。
        返回：真正提交去减的张数（按步长对齐）。
        """
        step = self._min_contract_step(inst_id)
        if remaining_need < step - 1e-12:
            return 0.0
        # 对齐步长（保守起见，向下取整）
        cut = math.floor((remaining_need + 1e-12) / step) * step
        if cut < step:
            return 0.0
        # 价格：用安全价 + 方向滑点
        px0 = self._safe_last_price(inst_id) or 0.0
        if px0 <= 0:
            logging.warning("[DEC] no safe last price, skip reduceOnly for now")
            return 0.0
        # 不要超过当前已持仓（避免 reduceOnly 被拒单）
        try:
            held_now = float(held_available if held_available is not None else self._held_contracts_by_side(inst_id, pos_side))
            cut = min(cut, math.floor((held_now + 1e-12) / step) * step)
        except Exception:
            pass
        # 上限裁剪后若不足一步，直接返回，避免下 0 张
        if cut < step - 1e-12:
            return 0.0
        px = self._with_slip_for_side(pos_side, px0)
        try:
            # reduce_position 的 side 传“原方向”（buy=long / sell=short）
            side = "buy" if pos_side.lower() == "long" else "sell"
            self.reduce_position(inst_id, price=float(px), size=float(cut), side=side)
            logging.info("[DEC] reduce position: side=%s size=%.8f px=%s", pos_side, cut, px)
            return float(cut)
        except Exception as e:
            logging.warning("[DEC] reduceOnly place_order failed: %s", e)
            return 0.0

    def dec_proportional_shave(self, coin: str, side_hint: str, ratio: float) -> dict:
        """
        等比例缩小本地同向暴露（持仓 + 未成交挂单）。
        **优化：改为“持仓优先”**：
        - ① 先对已持仓 reduceOnly（IOC/限价，按步长对齐）；
        - ② 再对剩余缺口剃挂单（带“改单前快照/校验/指数退避/重配”逻辑）。
        参数：
        coin:  币种（如 "BTC"）
        side_hint: "LONG" / "SHORT"（必传，来自上游 SIZE-DEC 事件）
        ratio: 目标比例（0~1），例如 0.7 表示把当前“持仓+挂单”的总暴露缩小到 70%
        返回：{ok: bool, shaved_pending: float, reduced_pos: float, detail: ...}
        """
        try:
            ratio = max(0.0, min(1.0, float(ratio)))
        except Exception:
            ratio = 1.0

        inst_id = self._get_inst_id(coin)
        if not inst_id:
            return {"ok": False, "reason": f"no instId for {coin}"}

        pos_side = "long" if (side_hint or "").upper() == "LONG" else "short"
        want_order_side = "buy" if pos_side == "long" else "sell"   # 用于筛同向挂单
        strict_flat = float(ratio) <= 1e-12
        # 统计当前本地暴露：持仓张数 + 同向未成交挂单张数（统一“张数”口径）
        held_now = self._held_contracts_by_side(inst_id, pos_side)

        # 若目标是“清零”，先撤掉该 inst 的所有挂单（双边），避免后续又被成交产生新暴露
        if strict_flat:
            try:
                self._cancel_all_pending(inst_id)   # pos_side=None → 双边全撤
            except Exception:
                pass
        _, pending_unfilled = self._pending_orders_by_side(inst_id, pos_side)
        # 诊断日志：把张数同时换算成币数量，方便和“我现在 10.8 HYPE”对齐核对
        notional, last_px, base_ccy = self._inst_notional_per_contract(inst_id)
        held_coin_est    = self._coins_from_contracts(held_now, inst_id) if held_now > 0 else 0.0
        pending_coin_est = self._coins_from_contracts(pending_unfilled, inst_id) if pending_unfilled > 0 else 0.0
        logging.info("[EXPO] inst=%s side=%s held=%.6f ct (~%.6f %s)  pending=%.6f ct (~%.6f %s)",
                     inst_id, pos_side.upper(), held_now, held_coin_est, base_ccy,
                     pending_unfilled, pending_coin_est, base_ccy)

        curr_total = held_now + pending_unfilled
        if curr_total <= 0:
            return {"ok": True, "shaved_pending": 0.0, "reduced_pos": 0.0, "detail": "no local exposure"}

        target_total = curr_total * ratio
        need_cut = max(0.0, curr_total - target_total)

        shaved_pending = 0.0
        reduced_pos = 0.0
        # === 使用 lotSz + minSz 做量化（不要强转 int）===
        # OKX 要求：下单数量必须是 lotSz 的整数倍，且 >= minSz
        # 其中 minSz 取接口规格与交易步长中的较大值，避免“能报单但不可成交”的边界
        max_lev, ct_val, ct_ccy, min_sz_spec = self._get_inst_spec(inst_id)
        lot_sz, min_sz2 = self._get_trade_steps(inst_id)
        min_sz = max(float(min_sz_spec), float(min_sz2))

        # Step 1) **持仓优先**：先 reduceOnly
        if need_cut > 0 and held_now > 0:
            # 目标减仓（不超过已持仓），按 lotSz 向下量化；若不足 minSz 则放弃本步减仓
            cap = min(need_cut, held_now)
            cut_ct = self._quantize_size(cap, lot_sz, "down")
            if cut_ct + 1e-12 >= min_sz:
                last_px = self._safe_last_price(inst_id) or 0.0
                if last_px > 0:
                    side_param = "buy" if pos_side == "long" else "sell"
                    px_exec = self._with_slip_for_side(pos_side, last_px)
                    if mode == "real":
                        self._throttle_side_ops(inst_id, pos_side)
                        with self._side_op_locks[self._side_key(inst_id, pos_side)]:
                            # 以浮点/字符串均可；此处保持浮点，并已按步长对齐
                            self.reduce_position(inst_id, float(px_exec), float(cut_ct), side_param)
                    logging.info("[DEC][POSITION] reduce inst=%s side=%s size=%.8f px=%.8f",
                                 inst_id, pos_side, float(cut_ct), float(px_exec))
                    reduced_pos = float(cut_ct)
                    need_cut = max(0.0, need_cut - cut_ct)

        # Step 2) 对剩余缺口剃挂单（带“快照/校验/重配/退避”）
        if need_cut > 0:
            shaved_pending = self._shave_pending_by_amount(inst_id, pos_side, need_cut)
            need_cut = max(0.0, need_cut - shaved_pending)
        # ── 若目标为 0，做一次强制校验并打印“已清零”日志（便于观察）
        try:
            if target_total <= 1e-12:
                # 立即强制刷新，绕过 positions TTL
                _ = self.get_positions(force=True)
                held_after = self._held_contracts_by_side(inst_id, pos_side)
                if (self._pending_orders_by_side(inst_id, pos_side)[1]) <= 1e-12 and held_after <= 1e-12:
                    logging.info("[DEC][FLAT] inst=%s side=%s exposure -> 0 (shaved=%.8f, reduced=%.8f)",
                                 inst_id, pos_side, shaved_pending, reduced_pos)
        except Exception:
            pass
        return {"ok": True, "shaved_pending": shaved_pending, "reduced_pos": reduced_pos,
                "detail": f"curr={curr_total:.4f} -> target={target_total:.4f}"}

 
    # ============================================================
    #  量化工具：基于 lotSz & minSz 的尺寸对齐（不再用 int()/ceil）
    # ============================================================
    def _quantize_size(self, sz: float, step: float, mode: str = "down") -> float:
        """按步长 step 对齐：'down' 向下取整，'up' 向上取整。"""
        try:
            dsz, dstep = Decimal(str(sz)), Decimal(str(step))
            if dstep <= 0:
                return float(sz)
            units = (dsz / dstep).to_integral_value(rounding=(ROUND_UP if mode == "up" else ROUND_DOWN))
            return float(units * dstep)
        except (InvalidOperation, ValueError, TypeError):
            return float(sz)

    def _ensure_min_step(self, sz: float, lot_sz: float, min_sz: float, mode: str = "up") -> float:
        """先按步长量化，再保证 ≥ minSz。"""
        q = self._quantize_size(sz, lot_sz, mode)
        if q < float(min_sz):
            q = self._quantize_size(min_sz, lot_sz, "up")
        return float(q)

    def _get_trade_steps(self, inst_id: str) -> tuple[float, float]:
        """
        返回 (lotSz, minSz)。优先 10 分钟内缓存；失败时优先回退到本地规格缓存/_get_inst_spec。
        """
        # 确保缓存字典存在
        if not hasattr(self, "_inst_steps"):
            self._inst_steps = {}
        # 命中 10 分钟内缓存
        rec = self._inst_steps.get(inst_id)
        if rec and (time.time() - float(rec.get("ts", 0.0))) < 600:
            return float(rec["lotSz"]), float(rec["minSz"])

        lot_sz, min_sz = 0.0, 0.0
        try:
            resp = self._call_api(self.public_api.get_instruments, instType="SWAP", instId=inst_id)
            if str(resp.get("code", "")) == "0" and (resp.get("data") or []):
                rows = resp["data"]
                # 精确匹配 instId；缺省取第 0 条
                row = next((r for r in rows if (r.get("instId") or "").upper() == inst_id.upper()), rows[0])
                # 解析 lotSz/minSz（允许字符串）
                try:
                    lot_sz = float(row.get("lotSz") or 0.0)
                except Exception:
                    lot_sz = 0.0
                try:
                    min_sz = float(row.get("minSz") or 0.0)
                except Exception:
                    min_sz = 0.0
        except Exception:
            # fall through to fallback below
            pass

        # Fallback：优先使用 _spec_cache / _get_inst_spec 的 minSz；lotSz 没有时用 minSz 兜底
        if lot_sz <= 0.0 or min_sz <= 0.0:
            cached = (self._spec_cache.get(inst_id) or {})
            if lot_sz <= 0.0:
                try:
                    lot_sz = float(cached.get("lotSz") or 0.0)
                except Exception:
                    lot_sz = 0.0
            if min_sz <= 0.0:
                try:
                    min_sz = float(cached.get("minSz") or 0.0)
                except Exception:
                    min_sz = 0.0
        if min_sz <= 0.0:
            # 兜底拿一遍规格；至少要拿到 minSz
            try:
                _max_lev, _ct, _ccy, min_sz_spec = self._get_inst_spec(inst_id)
                if float(min_sz_spec) > 0:
                    min_sz = float(min_sz_spec)
            except Exception:
                pass
        if lot_sz <= 0.0:
            # 再兜底：没有 lotSz 时，按 minSz 作为步长（常见于很多合约 lotSz==minSz）
            lot_sz = float(min_sz) if float(min_sz) > 0 else 1.0
        if min_sz <= 0.0:
            # 最后兜底，避免返回 0
            min_sz = lot_sz if lot_sz > 0 else 1.0

        self._inst_steps[inst_id] = {"lotSz": float(lot_sz), "minSz": float(min_sz), "ts": time.time()}
        logging.info("[SPEC] %s lotSz=%s minSz=%s", inst_id, lot_sz, min_sz)
        return float(lot_sz), float(min_sz)
    # =====================================================================
    #  极简对账：① 拉权威快照（一次或两次）② 覆盖本地缓存+计算偏差 ③ 最小化自愈（降风险）
    # =====================================================================
    def _reconcile_loop(self) -> None:
        """
        周期性对账，单线程 singleflight：
        - 每 RECON_INTERVAL_SEC ±JITTER 跑一次；
        - 任一异常只记日志，下一轮继续。
        """
        base = max(5.0, float(self.RECON_INTERVAL_SEC or 60.0))
        jitter = max(0.0, min(0.95, float(self.RECON_JITTER or 0.0)))
        while True:
            # 抖动后的睡眠时长
            factor = 1.0 + (random.random() * 2.0 - 1.0) * jitter
            sleep_sec = base * factor
            try:
                self._reconcile_once()
                self._recon_fail = 0
            except Exception as e:
                self._recon_fail += 1
                logging.warning("[RECON] reconcile_once failed (cnt=%s): %s", self._recon_fail, e, exc_info=True)
            finally:
                time.sleep(sleep_sec)

    def _reconcile_once(self) -> None:
        """
        三步极简对账，不做补仓，只做“剃挂单→减仓”的降风险修复。
        - NET 模式：仅剃挂单（不下 reduceOnly，避免方向不明带来的风险）
        - LONG_SHORT 模式：允许每个 (inst, side) 做一次剃挂单 + 最多一次 reduceOnly
        """
        # API 尚未就绪时直接返回，避免 AttributeError
        if not getattr(self, "account_api", None) or not getattr(self, "trade_api", None):
            return
        if not self._recon_lock.acquire(blocking=False):
            return  # singleflight：已有一轮在跑
        self._reconciling = True
        try:
            # ─────────────────────────────────────────────────────────
            # ① 拉权威快照（可选双读）
            # ─────────────────────────────────────────────────────────
            pos_data_1 = []
            try:
                r1 = self._call_api(self.account_api.get_positions)
                pos_data_1 = (r1 or {}).get("data", []) or []
            except Exception as e:
                logging.warning("[RECON] get_positions #1 failed: %s", e)

            pos_data = pos_data_1
            if self.RECON_DOUBLE_READ:
                try:
                    r2 = self._call_api(self.account_api.get_positions)
                    pos_data_2 = (r2 or {}).get("data", []) or []
                    # 简单一致性判断：长度或（|pos|总和）变动则采用第二次（期间可能有成交）
                    def _sig(rows):
                        s = 0.0
                        for p in rows:
                            v = p.get("pos") or p.get("posQty") or 0
                            try:
                                s += abs(float(v or 0.0))
                            except Exception:
                                pass
                        return (len(rows), round(s, 8))
                    if _sig(pos_data_1) != _sig(pos_data_2):
                        pos_data = pos_data_2
                except Exception as e:
                    logging.debug("[RECON] get_positions #2 failed (ignore): %s", e)

            # 未成交挂单（直接查当前）
            pending_all = []
            try:
                pending_all = list(self.get_pending_orders())
            except Exception as e:
                logging.debug("[RECON] get_pending_orders failed (ignore): %s", e)

            # ─────────────────────────────────────────────────────────
            # ② 覆盖本地缓存 + 计算偏差（以交易所为权威）
            # ─────────────────────────────────────────────────────────
            now_mono = time.monotonic()
            self._last_good_positions = pos_data[:]  # 覆盖
            self._pos_cache = {"ts": now_mono, "data": pos_data[:]}  # 覆盖（singleflight 内安全）

            # 要处理的 instId 集合：从 positions 与 pendings 联合得到
            inst_ids = set()
            for p in pos_data:
                inst = (p.get("instId") or "").upper()
                if inst:
                    inst_ids.add(inst)
            for od in pending_all:
                inst = (od.get("instId") or "").upper()
                if inst:
                    inst_ids.add(inst)
            if not inst_ids:
                return  # 无暴露，结束本轮

            # ─────────────────────────────────────────────────────────
            # ③ 最小化自愈（仅降风险）
            #    - LONG_SHORT：每边最多“剃挂单一次 + reduceOnly 一次”
            #    - NET：仅剃挂单
            # ─────────────────────────────────────────────────────────
            ls_mode = (str(getattr(self, "pos_mode", "")).lower() == "long_short")
            side_set = ("long", "short") if ls_mode else (None,)

            for inst in sorted(inst_ids):
                for side in side_set:
                    # 当前同边暴露：持仓张数 + 同向未成交张数（统一“张数”口径）
                    try:
                        held_ct = self._held_contracts_by_side(inst, side)  # 已持仓
                    except Exception:
                        held_ct = 0.0
                    try:
                        _pendings, unfilled_ct = self._pending_orders_by_side(inst, side)  # 同向未成交
                    except Exception:
                        unfilled_ct = 0.0
                    actual = float(held_ct) + float(unfilled_ct)

                    # 目标张数：如未配置 _target_contracts，则以“当前实际”为目标（不做补仓）
                    key = (inst, side or "")
                    tgt = float(self._target_contracts.get(key, actual) or actual)
                    # 容差：tol_factor × 合约步长（缺省 = 1 × 步长）
                    step = self._min_contract_step(inst)
                    tol = max(0.0, float(self.RECON_TOL_FACTOR or 1.0) * float(step))

                    # 仅处理“超配”场景（actual > target + tol），不处理“欠配”（不补仓）
                    over = max(0.0, actual - tgt)
                    if over < (tol - 1e-12):
                        continue

                    shaved = 0.0
                    reduced = 0.0

                    # ③-1 剃同向挂单（优先，尽量不触碰已持仓）
                    try:
                        shaved = float(self._shave_pending_by_amount(inst, side, over))
                    except Exception as e:
                        logging.debug("[RECON] shave pendings failed inst=%s side=%s: %s", inst, side, e)

                    remaining = max(0.0, over - shaved)
                    if remaining < (step - 1e-12):
                        logging.info("[RECON] inst=%s side=%s shaved=%.8f over=%.8f → OK",
                                    inst, side or "-", shaved, over)
                        continue

                    # ③-2 NET：仅剃挂单，不做 reduceOnly（方向不明，避免额外风险）
                    if not ls_mode:
                        logging.info("[RECON] inst=%s NET-mode shaved=%.8f remain=%.8f → skip reduceOnly",
                                    inst, shaved, remaining)
                        continue

                    # ③-2 LONG_SHORT：若仍超配，做一次 reduceOnly（限价，带方向滑点）
                    try:
                        reduced = float(self._reduce_filled_if_needed(
                            inst, side, remaining, held_available=held_ct
                        ))
                    except Exception as e:
                        logging.debug("[RECON] reduceOnly failed inst=%s side=%s: %s", inst, side, e)

                    logging.info("[RECON] inst=%s side=%s actual=%.8f target=%.8f tol=%.8f → shaved=%.8f, reduced=%.8f",
                                inst, side or "-", actual, tgt, tol, shaved, reduced)
        finally:
            self._reconciling = False
            try:
                self._recon_lock.release()
            except Exception:
                pass

    # ────────────────────────────────────────────────────────────
    #  ❶ 新增：风控/价格去重等“开仓等效”重置（供权威对齐后出现新仓位时使用）
    # ────────────────────────────────────────────────────────────
    def _reset_guards_for_side(self, inst_id: str, pos_side: str) -> None:
        # 确保各状态字典存在，避免 AttributeError（与其它 mixin 解耦）
        for _name in ("_pp_hits","_lp_hits","_pp_last_px","_lp_last_px",
                      "_mfe_peak","_mfe_hits","_hys_state_pp","_hys_state_lp",
                      "_cooldown_ts","_mfe_last_roe","_mfe_gated","_mfe_last_peak_trig"):
            if not hasattr(self, _name):
                setattr(self, _name, {})
        # PP/LP 分档 & 触发价去重
        self._pp_hits[self._pp_key(inst_id, pos_side)] = 0
        # 使用 LP 的 key；若无 _lp_key 则回退到 _pp_key 以避免 KeyError
        _lp_key_fn = getattr(self, "_lp_key", None) or getattr(self, "_pp_key")
        self._lp_hits[_lp_key_fn(inst_id, pos_side)] = 0
        self._save_profit_guard_state(); self._save_loss_guard_state()
        self._pp_last_px.pop(self._pp_key(inst_id, pos_side), None)
        self._lp_last_px.pop(_lp_key_fn(inst_id, pos_side), None)
        self._save_profit_guard_px_state(); self._save_loss_guard_px_state()
        # MFE/滞回/冷却
        k = f"{inst_id}:{pos_side}"
        for d in (self._mfe_peak, self._mfe_hits, self._hys_state_pp, self._hys_state_lp,
                 self._cooldown_ts, self._mfe_last_roe, self._mfe_gated, self._mfe_last_peak_trig):
            with suppress(Exception):
                d.pop(k, None)

    def _reset_guards_if_new_pos_from_authority(self, snap_or_diff: dict) -> None:
        """
        权威快照或 diff 里若出现“本地原本无暴露”的方向 → 视同一次开仓，重置风控/去重/MFE。
        - 兼容两种输入：
            • snap: {positions: [...]}
            • diff: {authoritative_new_sides: [{"instId":..., "posSide":"long|short"}]}
        """
        try:
            # 1) diff 路径
            new_from_diff = (snap_or_diff or {}).get("authoritative_new_sides") or []
            if new_from_diff:
                for it in new_from_diff:
                    inst_id = it.get("instId"); side = (it.get("posSide") or "").lower()
                    if inst_id and side in ("long","short"):
                        self._reset_guards_for_side(inst_id, side)
                        logging.info("[AUTH][RESET-GUARDS] inst=%s side=%s (from diff)", inst_id, side)
                return
            # 2) snap 路径（向后兼容）
            pos_list = (snap_or_diff or {}).get("positions") or []
            if not pos_list:
                return
            local = self.get_positions()
            for it in pos_list:
                inst_id = it.get("instId")
                try:
                    qty = float(it.get("pos") or 0)
                except Exception:
                    qty = 0.0
                if not inst_id or qty == 0:
                    continue
                side = "long" if qty > 0 else "short"
                found = next((p for p in local
                              if p.get("instId")==inst_id and
                                 (self.pos_mode!="long_short" or (p.get("posSide","").lower()==side))), None)
                cur = abs(float(found.get("pos") or 0)) if found else 0.0
                if cur == 0.0:
                    self._reset_guards_for_side(inst_id, side)
                    logging.info("[AUTH][RESET-GUARDS] inst=%s side=%s (from snap)", inst_id, side)
        except Exception as e:
            logging.debug("[AUTH][RESET-GUARDS][SKIP] %s", e)

    # ────────────────────────────────────────────────────────────
    #  ❷ 新增：最小自愈调度器（优先调用 core_infra 的实现；否则降级为本地简版）
    # ────────────────────────────────────────────────────────────
    def _do_minimal_heal(self, diff: dict) -> None:
        """只做“降风险”的修复：撤孤儿挂单、按建议 reduceOnly。"""
        try:
            # 若 core_infra 已提供完整实现，优先调用
            if hasattr(self, "infra_apply_minimal_heal") and callable(getattr(self, "infra_apply_minimal_heal")):
                return self.infra_apply_minimal_heal(diff)
        except Exception as e:
            logging.debug("[HEAL][DELEGATE][SKIP] %s", e)
        # —— 退化：轻量内置（兼容示例字段；没有就忽略）——
        try:
            for od in (diff or {}).get("ghost_open_orders_to_cancel", []) or []:
                inst = od.get("instId"); oid = od.get("ordId") or od.get("clOrdId")
                if inst and oid:
                    self._send_with_authority_fallback(self.cancel_order, inst, oid)
            for dec in (diff or {}).get("reduce_only_suggestions", []) or []:
                inst = dec.get("instId"); px = dec.get("px"); sz = dec.get("sz"); sd = dec.get("side")
                if inst and sz and sd:
                    self._send_with_authority_fallback(self.reduce_position, inst, px, sz, sd)
        except Exception as e:
            logging.debug("[HEAL][LOCAL][SKIP] %s", e)
    def cancel_order(self, inst_id: str, ord_id: str) -> dict:
        """撤单包装（供最小自愈/对账等路径统一调用）"""
        return self._call_api(self.trade_api.cancel_order, instId=inst_id, ordId=ord_id)
    # ────────────────────────────────────────────────────────────
    #  ❸ 新增：权威对齐/偏差计算/最小自愈 —— 提供给 trade.py 启动序列直接调用
    #      • infra_fetch_authoritative_snapshot(tries=2)
    #      • infra_override_local_caches(snap)
    #      • infra_compute_diff(snap)  → diff
    #      • infra_apply_minimal_heal(diff)
    # ────────────────────────────────────────────────────────────
    def infra_fetch_authoritative_snapshot(self, tries: int = 1) -> dict:
        """
        从交易所拉“权威状态”，包含：positions + 未完全成交的挂单。
        返回：
          {
            "positions": [...],
            "pending_orders": [...],
            "ts": time.time()
          }
        """
        tries = max(1, int(tries or 1))
        last_exc = None
        pos_data = []
        for _ in range(tries):
            try:
                r = self._call_api(self.account_api.get_positions)
                pos_data = (r or {}).get("data", []) or []
                break
            except Exception as e:
                last_exc = e
        if last_exc and not pos_data:
            logging.warning("[AUTH] get_positions failed (tries=%s): %s", tries, last_exc)

        # 未成交挂单（live/partially_filled）
        pending = []
        try:
            if hasattr(self.trade_api, "get_orders_pending"):
                r = self._call_api(self.trade_api.get_orders_pending)
            else:
                r = self._call_api(self.trade_api.get_order_list)
            for od in (r or {}).get("data", []) or []:
                st = str(od.get("state") or "").lower()
                if st in ("live","partially_filled"):
                    pending.append(od)
        except Exception as e:
            logging.debug("[AUTH] fetch pendings failed: %s", e)

        return {"positions": pos_data, "pending_orders": pending, "ts": time.time()}

    def infra_override_local_caches(self, snap: dict) -> None:
        """
        用权威快照覆盖本地缓存（positions + 未成交订单）。
        注意：只刷新“仍在进行”的订单；不会把已撤/已完结的历史订单写回。
        """
        now_mono = time.monotonic()
        pos = (snap or {}).get("positions") or []
        self._last_good_positions = pos[:]
        self._pos_cache = {"ts": now_mono, "data": pos[:]}
        # 刷新订单缓存中的 pending（其余保持不动）
        try:
            for od in (snap or {}).get("pending_orders", []) or []:
                self.orders[od["ordId"]] = od
            # 同时把“存在性仓位”也记入 orders，便于 get_active_trades()
            for p in pos:
                key = f"pos_{p.get('instId')}_{p.get('posSide')}"
                self.orders[key] = p
        except Exception:
            pass

    def infra_compute_diff(self, snap: dict) -> dict:
        """
        计算“仅用于降风险修复”的偏差 diff。
        产出：
          {
            "ts": ...,
            "authoritative_new_sides": [{"instId":..., "posSide":"long|short"}],  # 权威新增方向（用于重置风控）
            "per_side": [
              {"instId":..., "posSide":"long|short"|null, "actual":float, "target":float,
               "over":float, "tol":float, "step":float}
            ]
          }
        约定：target 默认等于“权威持仓张数”（不做补仓），只在 _target_contracts 有目标时才会要求回落到目标。
        """
        ls_mode = (str(getattr(self, "pos_mode", "")).lower() == "long_short")
        step_cache = {}
        def _step(inst): 
            if inst not in step_cache:
                step_cache[inst] = self._min_contract_step(inst)
            return step_cache[inst]

        # 1) 解析权威 sides
        auth_positions = (snap or {}).get("positions") or []
        auth_sides = set()
        auth_held: dict[tuple[str, str|None], float] = {}
        for p in auth_positions:
            inst = (p.get("instId") or "").upper()
            if not inst:
                continue
            # 仅使用 0.4.0 的 `pos` 字段（张数）
            try:
                qty_ct = abs(float(p.get("pos") or 0.0))
            except Exception:
                qty_ct = 0.0
            if qty_ct <= 0.0:
                continue
            side = (p.get("posSide") or "").lower()
            if side not in ("long", "short"):
                continue
            key = (inst, side)
            auth_sides.add(key)
            auth_held[key] = float(auth_held.get(key, 0.0) + qty_ct)

        # 2) 本地已有 sides（对比“权威新增方向”用于风控重置）
        local = self.get_positions() or []
        local_sides = set()
        for p in local:
            inst = (p.get("instId") or "").upper()
            if not inst: 
                continue
            side = (p.get("posSide") or "").lower()
            if side not in ("long","short"):
                continue
            local_sides.add((inst, side))
        new_sides = [{"instId": i, "posSide": (s or "")} for (i,s) in (auth_sides - local_sides) if s]

        # 3) 统计同向未成交（只统计非 reduceOnly 的开仓/加仓挂单）
        pendings = []
        try:
            pendings = list(self.get_pending_orders())
        except Exception:
            pass
        unfilled: dict[tuple[str, str|None], float] = {}
        for od in pendings:
            inst = (od.get("instId") or "").upper()
            if not inst:
                continue
            if str(od.get("reduceOnly") or "").lower() == "true":
                continue
            side = (od.get("posSide") or "").lower()
            if side not in ("long","short"):
                continue
            filled = float(od.get("accFillSz") or 0.0)
            sz = float(od.get("sz") or 0.0)
            un = max(sz - filled, 0.0)
            if un <= 0:
                continue
            unfilled[(inst, side)] = unfilled.get((inst, side), 0.0) + un

        # 4) 生成 per_side 差异（只关注超配 → 需要“降风险”）
        per_side = []
        keys = set(auth_sides) | set(unfilled.keys())
        for (inst, side) in sorted(keys):
            actual = float(auth_held.get((inst, side), 0.0) + unfilled.get((inst, side), 0.0))
            # 目标：若外部有 target 则用之，否则默认取“权威持仓”→ 只削挂单，不补也不额外减仓
            tgt = float(self._target_contracts.get((inst, side or ""), auth_held.get((inst, side), 0.0)) or 0.0)
            step = _step(inst)
            tol = max(0.0, float(self.RECON_TOL_FACTOR or 1.0) * float(step))
            over = max(0.0, actual - tgt - tol)
            per_side.append({
                "instId": inst, "posSide": side, "actual": actual,
                "target": tgt, "over": over, "tol": tol, "step": step
            })
        return {"ts": time.time(), "authoritative_new_sides": new_sides, "per_side": per_side}

    def infra_apply_minimal_heal(self, diff: dict) -> None:
        """
        根据 diff 仅执行“降风险”动作：
          • 先按量剃同向未成交（优先 amend，不足步长则撤）
          • LONG_SHORT 模式下，若仍超配再下 reduceOnly
        """
        if not diff:
            return
        # 固定双向持仓
        ls_mode = True
        for row in (diff.get("per_side") or []):
            inst = row.get("instId"); side = row.get("posSide")
            over = float(row.get("over") or 0.0); step = float(row.get("step") or 1.0)
            if not inst or over < (step - 1e-12):
                continue
            shaved = 0.0
            try:
                shaved = float(self._shave_pending_by_amount(inst, side, over))
            except Exception as e:
                logging.debug("[HEAL] shave pendings failed inst=%s side=%s: %s", inst, side, e)
            remain = max(0.0, over - shaved)
            if remain < (step - 1e-12):
                continue
            if not ls_mode:
                logging.info("[HEAL] inst=%s NET-mode shaved=%.8f remain=%.8f → skip reduceOnly", inst, shaved, remain)
                continue
            try:
                self._reduce_filled_if_needed(inst, side, remain)
            except Exception as e:
                logging.debug("[HEAL] reduceOnly failed inst=%s side=%s: %s", inst, side, e)
    # ────────────────────────────────────────────────────────────
    #  ❸ 新增：下单/撤单失败 → 立即做一次“权威对齐”回退
    #     （注意：不做任何“发单前预检”，以免延迟发单）
    # ────────────────────────────────────────────────────────────
    def _send_with_authority_fallback(self, fn: Callable, *a, **kw):
        try:
            resp = fn(*a, **kw)
        except Exception as e:
            logging.warning("[ORDER][EXC] %s — triggering authority resync", e)
            try:
                if hasattr(self, "infra_fetch_authoritative_snapshot") and hasattr(self, "infra_override_local_caches"):
                    snap = self.infra_fetch_authoritative_snapshot(tries=2)
                    self.infra_override_local_caches(snap)
            except Exception:
                pass
            raise
        # OKX 成功通常 code=="0"
        try:
            ok = str((resp or {}).get("code", "0")) == "0"
        except Exception:
            ok = True
        if not ok:
            logging.warning("[ORDER][ERR] resp=%s — triggering authority resync", resp)
            try:
                if hasattr(self, "infra_fetch_authoritative_snapshot") and hasattr(self, "infra_override_local_caches"):
                    snap = self.infra_fetch_authoritative_snapshot(tries=2)
                    self.infra_override_local_caches(snap)
            except Exception:
                pass
        return resp



    def _dynamic_entry_slip(self, inst_id: str, last_px: float) -> float:
        """
        返回用于乘以价格的滑点比例（分数，如 0.003 = 0.3%）。
        优先 ATR%，再与价差/最小跳动/绝对地板做 max 约束。
        """
        # 读取配置（先在 __init__ 里保存这些属性，见下）
        mode = getattr(self, "SLIP_MODE", "atr")
        slip = abs(float(getattr(self, "ENTRY_SLIP", 0.0) or 0.0))  # 兜底：旧的固定比例

        if mode == "atr":
            st = self._get_atr_state(inst_id) or {}
            atr_pct = float(st.get("atr_pct") or 0.0)  # 单位：百分比数值，如 0.7 表示 0.7%
            if atr_pct > 0:
                # atr_k × ATR%（把百分比转成分数）
                slip = (float(self.SLIP_ATR_K) * atr_pct) / 100.0
                # 钳制上下限（分数）
                slip = max(float(self.SLIP_MIN_PCT), min(float(self.SLIP_MAX_PCT), slip))

            # 与价差/最小跳动/绝对地板做下限合并（都转为“分数”比较）
            try:
                if getattr(self, "SLIP_SPREAD_MULT", 0.0) > 0:
                    ob = self._call_api(self.market_api.get_books, instId=inst_id, sz=1)["data"][0]
                    best_ask = float(ob["asks"][0][0]); best_bid = float(ob["bids"][0][0])
                    spread_frac = max(0.0, (best_ask - best_bid) / max(1e-12, float(last_px)))
                    slip = max(slip, float(self.SLIP_SPREAD_MULT) * spread_frac)
            except Exception:
                pass
            try:
                # 使用已缓存的 tickSz
                _ = self._get_inst_spec(inst_id)
                tick = float((self._spec_cache.get(inst_id) or {}).get("tickSz") or 0.0)
                if getattr(self, "SLIP_TICK_MULT", 0.0) > 0 and tick > 0 and last_px > 0:
                    tick_frac = tick / float(last_px)
                    slip = max(slip, float(self.SLIP_TICK_MULT) * tick_frac)
            except Exception:
                pass
            try:
                if getattr(self, "SLIP_ABS_FLOOR", 0.0) > 0 and last_px > 0:
                    abs_floor_frac = float(self.SLIP_ABS_FLOOR) / float(last_px)
                    slip = max(slip, abs_floor_frac)
            except Exception:
                pass

        return max(0.0, float(slip))

    # ============================================================
    #  订单错误自愈：51121（尺寸/精度不合法）按步进下取整并重试一次
    # ============================================================
    def _resp_has_code(self, resp: dict | None, code: str | int) -> bool:
        """检测顶层 code 或 data[].sCode 是否包含指定错误码（字符串/整数均可）"""
        try:
            if resp is None:
                return False
            if str(resp.get("code", "")).strip() == str(code):
                return True
            for x in (resp.get("data") or []):
                if str(x.get("sCode", "")).strip() == str(code):
                    return True
        except Exception:
            pass
        return False

    def _maybe_retry_51121(
        self,
        first_resp: dict,
        *,
        send_again: Callable[[str], dict],
        size_str: str,
        lot_sz: float,
        min_sz: float,
        inst_id: str,
        side_desc: str,
        px: float,
    ) -> dict:
        """若首发返回 51121，则把 size 向下对齐一步（不低于 minSz）并重试一次。"""
        if not self._resp_has_code(first_resp, "51121"):
            return first_resp
        try:
            cur = max(0.0, float(size_str))
            # 向下对齐一步；若依然 < minSz 则不重试，直接返回原响应
            new_sz = self._quantize_size(max(0.0, cur - float(lot_sz)), float(lot_sz), "down")
            if new_sz < float(min_sz):
                logging.info("[ORDER][RETRY][51121][SKIP] inst=%s size after floor %.8f < minSz %.8f",
                             inst_id, new_sz, float(min_sz))
                return first_resp
            new_sz_str = f"{new_sz:.8f}".rstrip('0').rstrip('.')
            logging.info("[ORDER][RETRY][51121] inst=%s side=%s px=%.8f sz:%s→%s (lotSz=%s minSz=%s)",
                         inst_id, side_desc, float(px), size_str, new_sz_str, lot_sz, min_sz)
            second = send_again(new_sz_str)
            return second or first_resp
        except Exception as _e:
            logging.warning("[ORDER][RETRY][51121][FAIL] inst=%s err=%s", inst_id, _e)
            return first_resp

    # ---- 工具：是否已有同向挂单（用于全程去重） ----
    def _has_same_side_pending(self, inst_id: str, *, side: str | None = None, pos_side: str | None = None) -> bool:
        """
        返回是否已存在该 instId 下、匹配方向(side/posSide)的“未完全成交”的 live 挂单。
        side    : "buy"/"sell"（与 OKX 下单 side 一致）
        pos_side: "long"/"short"（仅在 long_short 模式下校验）
        """
        try:
            for od in self.get_pending_orders():
                if od.get("instId") != inst_id:
                    continue
                if side and od.get("side", "").lower() != str(side).lower():
                    continue
                if self.pos_mode == "long_short" and pos_side:
                    if od.get("posSide", "").lower() != str(pos_side).lower():
                        continue
                unfilled = float(od.get("sz", 0) or 0) - float(od.get("accFillSz", 0) or 0)
                if unfilled > 0 and od.get("state") in {"live", "partially_filled"}:
                    return True
        except Exception:
            pass
        return False

    # ============================================================
    #  权威重建（以交易所为真相源）+ 暴露分解/打印
    # ============================================================
    def _authority_resync(self, reason: str | None = None, inst_id: str | None = None) -> None:
        """
        触发一次“权威 → 本地”的完整同步，优先走 CoreInfra 的快照接口；
        若不可用则退回直接拉取 positions / live orders。
        """
        try:
            if reason:
                logging.info("[AUTH][RESYNC][BEGIN] reason=%s inst=%s", reason, inst_id or "-")
            if hasattr(self, "infra_fetch_authoritative_snapshot"):
                # 尽量多尝试一次，降低瞬时抖动
                snap = self.infra_fetch_authoritative_snapshot(tries=2)
                with suppress(Exception):
                    if hasattr(self, "infra_compute_diff"):
                        diff = self.infra_compute_diff(snap)
                        if diff and hasattr(self, "_reset_guards_if_new_pos_from_authority"):
                            self._reset_guards_if_new_pos_from_authority(diff)
                if hasattr(self, "infra_override_local_caches"):
                    self.infra_override_local_caches(snap)
                with suppress(Exception):
                    if diff and hasattr(self, "_do_minimal_heal"):
                        self._do_minimal_heal(diff)
            else:
                # 退回直拉（SDK 命名以你项目为准，以下调用与项目内其他处一致）
                pos = self._call_api(self.account_api.get_positions, instType="SWAP")
                live = self._call_api(self.trade_api.get_orders_pending)
                # 尽量保持与 CoreInfra 的缓存结构一致（只做最小覆盖）
                if isinstance(live, dict) and "data" in live:
                    for od in (live["data"] or []):
                        self.orders[str(od.get("ordId"))] = od
            self._log_exposure(inst_id)  # 重建后给一条暴露分解日志
            logging.info("[AUTH][RESYNC][END] ok")
        except Exception as e:
            logging.warning("[AUTH][RESYNC][FAIL] %s", e)

    def _expo_breakdown(self, inst_id: str, pos_side: str) -> dict:
        """
        返回该 inst/方向的暴露分解：
          - held: 仅已成交持仓
          - pending_open: 未成交的“开仓”方向委托
          - pending_close: 未成交的“平仓/减仓”方向委托
          - expo_net: max(held - pending_close, 0)
        约束：pending_open 绝不并入暴露。
        """
        held = self._held_contracts_by_side(inst_id, pos_side)
        # 你已有的工具：_pending_orders_by_side(inst_id, pos_side)
        # 语义约定：返回 (pending_open, pending_close)
        pend_open, pend_close = 0.0, 0.0
        with suppress(Exception):
            po, pc = self._pending_orders_by_side(inst_id, pos_side)
            pend_open, pend_close = float(po or 0.0), float(pc or 0.0)
        expo_net = max(0.0, float(held) - float(pend_close or 0.0))
        return {
            "held": float(held),
            "pending_open": float(pend_open),
            "pending_close": float(pend_close),
            "expo_net": float(expo_net),
        }

    def _log_exposure(self, inst_id: str | None = None) -> None:
        """
        打印 inst 的暴露拆分（长短两个方向各一条）；inst_id 为空则跳过。
        """
        if not inst_id:
            return
        for side in ("long", "short"):
            b = self._expo_breakdown(inst_id, side)
            logging.info(
                "[EXPO] inst=%s side=%s held=%.6f  pending_open=%.6f  pending_close=%.6f  expo_net=%.6f",
                inst_id, side.upper(), b["held"], b["pending_open"], b["pending_close"], b["expo_net"]
            )

    # ============================================================
    #  OKX 撤单显式分支：已撤/已完成/不存在 → 都从本地 pending 移除
    # ============================================================
    def _cancel_okx(self, ord_id: str, inst_id: str) -> dict:
        """
        包一层撤单并解析 OKX 返回码。
        返回：{"ok":bool, "status":"cancelled|finished|not_found|error", "code":str, "raw":resp}
        """
        try:
            resp = self.cancel_order(ord_id, inst_id)
            data = (resp or {}).get("data") or [{}]
            sCode = str((data[0] or {}).get("sCode") or (resp or {}).get("code") or "")
            if sCode in ("0", "00000"):
                return {"ok": True, "status": "cancelled", "code": sCode, "raw": resp}
            # 已完成/不存在 → 视为市场侧已消失，同样从本地移除
            if sCode in {"51620", "51610"}:
                return {"ok": True, "status": "finished" if sCode == "51620" else "not_found", "code": sCode, "raw": resp}
            # 其他返回码
            return {"ok": False, "status": "error", "code": sCode, "raw": resp}
        except Exception as e:
            logging.warning("[CANCEL][EXC] inst=%s ordId=%s err=%s", inst_id, ord_id, e)
            return {"ok": False, "status": "error", "code": "EXC", "raw": {"err": str(e)}}


    # === NEW: 按本地实际持仓方向收集 long/short（仅 long_short 模式） ===
    def _pos_sides_with_exposure(self, inst_id: str, positions: list[dict]) -> list[str]:
        """
        返回当前 instId 下“有已成交持仓且 avgPx>0”的方向列表（['long', 'short'] 的子集，保持出现顺序）。
        仅用于 long_short 模式。
        """
        sides: list[str] = []
        if self.pos_mode != "long_short":
            return sides
        for p in positions:
            try:
                if p.get("instId") != inst_id:
                    continue
                ps  = (p.get("posSide") or "").lower()
                qty = abs(float(p.get("pos", 0) or 0))
                apx = float(p.get("avgPx", 0) or 0)
                if ps in ("long", "short") and qty > 0 and apx > 0:
                    sides.append(ps)
            except Exception:
                continue
        # 去重但保持顺序
        out, seen = [], set()
        for s in sides:
            if s not in seen:
                out.append(s); seen.add(s)
        return out

  