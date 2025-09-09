# /root/copyTrade/indicator/ws/okx_client.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import asyncio
import logging
import time
import math
import statistics
import hmac
import hashlib
import base64
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Tuple

import aiohttp
import ujson
from pathlib import Path
from sqlite_store import SqliteStore
# ----------------------- 回调类型 -----------------------
OnOrderbook = Callable[[str, dict], asyncio.Future | None]
OnTrade = Callable[[str, dict], asyncio.Future | None]
OnStatus = Callable[[dict], asyncio.Future | None]
OnTpoSample = Callable[[str, float, int], asyncio.Future | None]  # (instId, mid_px, ts_ms)

# ----------------------- 本地订单簿状态 -----------------------
@dataclass
class _BookState:
    bids: Dict[float, float] = field(default_factory=dict)  # 价→量（买，降序遍历）
    asks: Dict[float, float] = field(default_factory=dict)  # 价→量（卖，升序遍历）
    last_seq: Optional[int] = None
    last_ts: Optional[int] = None
    first_prev_seq: Optional[int] = None   # 首帧 prevSeqId（用于快照对齐）
    buffer: List[dict] = field(default_factory=list)  # 对齐前的增量缓存
    bbo_ready: bool = False  # 首次出现有效 BBO（bid/ask 同时非空）后置 True

def _to_int(x) -> Optional[int]:
    try:
        return int(x) if x is not None else None
    except Exception:
        return None


class OKXClient:
    """
    OKX V5 公共 WS + REST 数据源管理器（生产版）
      - WebSocket: books-l2-tbt / books / trades / tickers
      - 订单簿：快照对齐 + seqId/prevSeqId 回放 + 断线重连 + 自动降级
      - REST：candlesticks / mark-price-candles / mark-price / funding-rate-history
      - 波动刻度：ATR%(Wilder) 与 收盘绝对收益中位数%
      - 缓存与重试：TTL 缓存、miss 负缓存、指数退避
    """

    def __init__(
        self,
        cfg: dict,
        on_orderbook: OnOrderbook,
        on_trade: OnTrade,
        on_status: Optional[OnStatus] = None,
        on_tpo_sample: Optional[OnTpoSample] = None,
    ):
        self.cfg = cfg
        self.on_orderbook = on_orderbook
        self.on_trade = on_trade
        self.on_status = on_status or (lambda _s: None)
        self.on_tpo_sample = on_tpo_sample  # 可为 None（则仅做成交回灌）

        okx_cfg = cfg.get("okx", {})
        self.ws_url: str = okx_cfg.get("ws_url", "wss://ws.okx.com:8443/ws/v5/public")
        # 可在 config.yaml 自定义（US 区域用 https://www.okcoin.com）
        self.rest_base_url: str = okx_cfg.get("rest_base_url", "https://www.okx.com")

        chs = okx_cfg.get("channels", {})
        self.orderbook_channel: str = chs.get("orderbook", "books-l2-tbt")
        self.trades_channel: str = chs.get("trades", "trades")
        self.tickers_channel: str = chs.get("tickers", "tickers")
        self.inst_ids: List[str] = list(okx_cfg.get("instIds", []))
        # ---- WS auth（public 的 l2-tbt 也可能要求登录）----
        auth = (okx_cfg.get("auth") or {})
        self._api_key: Optional[str] = auth.get("api_key")
        self._api_secret: Optional[str] = auth.get("secret_key")
        self._passphrase: Optional[str] = auth.get("passphrase")
        # 是否具备密钥（随时可登录）
        self._has_creds: bool = bool(self._api_key and self._api_secret and self._passphrase)
        # 开机是否先登录（business 或订阅 l2-tbt 都建议预登录，避免 60011）
        self._need_login: bool = bool(self._has_creds and (("business" in self.ws_url) or ("l2-tbt" in self.orderbook_channel)))
 
        self._login_ok: bool = False
        self._login_waiter: Optional[asyncio.Future] = None
        # 会话
        self._session: Optional[aiohttp.ClientSession] = None
        self._ws: Optional[aiohttp.ClientWebSocketResponse] = None

        # 本地订单簿
        self._books: Dict[str, _BookState] = {inst: _BookState() for inst in self.inst_ids}

        # 重连退避
        self._reconnect_backoff = 1.0
        self._last_status_ts = 0
        # --------- 本地聚合缓存 (SQLite) ---------
        storage_cfg = (cfg.get("storage", {}) or {})
        db_path = Path(storage_cfg.get("sqlite_path", "./okx_cache.db"))
        # 若是相对路径，则锚定到项目根目录（ws/ 的上一级，即 /root/copyTrade/indicator）
        if not db_path.is_absolute():
            proj_root = Path(__file__).resolve().parent.parent  # ws/ -> 项目根
            db_path = (proj_root / db_path).resolve()
        self._store = SqliteStore(db_path)
        logging.info("[OKX][DB] using sqlite at %s", db_path)
        self._vp_steps = self._build_vp_steps()
        # 是否保存原始成交（默认 True），只要你只用 VP 最大价位，就可以关掉减少写入量
        self._store_raw_trades: bool = bool(storage_cfg.get("store_raw_trades", True))
        # --------- 波动/ATR 相关配置（替代旧 MarketDataFeed） ---------
        vcfg = (cfg.get("features", {}) or {}).get("volatility", {}) or {}
        # --------- 回灌（bootstrap）策略 ---------
        pboot = (cfg.get("features", {}) or {}).get("profile", {}) or {}
        self._bootstrap_mode: str = str(pboot.get("bootstrap_mode", "gap")).lower()  # "full" | "gap"
        # 每攒多少分钟落库/打点（默认 4 小时 = 240 分钟）
        self._bootstrap_flush_minutes: int = int(pboot.get("bootstrap_flush_minutes", 240))
        # TPO 采样周期（用于让回补与实时 on_tpo_sample 频率一致；实时通常是 30_000ms）
        # 仅影响回补时发送给 on_tpo_sample 的频率；数据库仍按 1 分钟精度存储
        self._tpo_sample_ms: int = int(pboot.get("tpo_sample_ms", 30_000))
        self._atr_window: int = int(vcfg.get("atr_window", 14))
        self._bar: str = str(vcfg.get("bar", "3m"))
        self._kl_max: int = int(vcfg.get("max_candles", 500))
        self._vol_ttl: float = float(vcfg.get("cache_ttl_sec", 20.0))
        self._rest_retries: int = int(vcfg.get("retries", 5))
        self._rest_backoff: float = float(vcfg.get("backoff_base", 0.5))

        self._bootstrap_minutes: int = int(pboot.get("bootstrap_minutes", 4320))
        self._bootstrap_trade_pages: int = int(pboot.get("bootstrap_trade_pages", 0))
        self._fast_start_if_covered: bool = bool(pboot.get("fast_start_if_covered", False))
        self._fast_subscribe_per_inst: bool = bool(pboot.get("fast_subscribe_per_inst", False))
        # 记录已在 WS 上订阅的 instId 集合（用于降级重订阅等）
        self._subscribed_insts: set[str] = set()
        self._bootstrapped: bool = False
        # 若未显式配置 bootstrap_minutes，则从 MultiProfile 的最长窗口推断（例如 72h=4320m）
        if self._bootstrap_minutes <= 0:
            try:
                _pf = (cfg.get("features", {}) or {}).get("profile", {}) or {}
                _wins = _pf.get("multi_windows_minutes") or []
                if isinstance(_wins, (list, tuple)) and _wins:
                    self._bootstrap_minutes = int(max(int(x) for x in _wins))
            except Exception:
                pass
        # 兜底：仍无效则回到 480 分钟
        if self._bootstrap_minutes <= 0:
            self._bootstrap_minutes = 480
        # 运行时缓存 & miss
        self._vol_cache: Dict[str, Dict[str, float]] = {}           # instId -> {..., "ts": epoch}
        self._miss_until: Dict[Tuple[str, str], float] = {}          # (instId, kind) -> until_ts
        self._init_rate_limiters()  # 初始化端点级限流器
        # —— REST 并发与分页节流（防 429） ——
        okx_top = (cfg.get("okx", {}) or {})
        self._rest_concurrency: int = int(okx_top.get("rest_concurrency", 2))  # 全局 REST 并发上限
        self._rest_sem = asyncio.Semaphore(self._rest_concurrency)
        # ⭐ 强制串行回补所有交易对
        self._backfill_parallel_insts: int = 1
        self._page_pause_s: float = float(pboot.get("backfill_page_pause_s", 0.10))  # 100ms
        # —— 新增：记录启动时刻 & 回灌启动开关 ——
        self._startup_t0: Optional[int] = None   # 进程启动时刻（ms）
        self._backfill_started: bool = False     # 避免多次并发启动后台回灌
        # —— 新增：OKX SWAP 清单与未命中负缓存 ——
        self._okx_swaps: set[str] = set()
        self._okx_swaps_loaded_at: float = 0.0
        self._inst_not_found_ttl: float = 1800.0  # 与日志一致：未找到交易对，静默 1800s
        # —— 新增：有效/无效 inst 及“未监控”提示节流 ——
        self._valid_insts: list[str] = []
        self._invalid_insts: list[str] = []
        self._unmonitored_emit_ts: dict[str, float] = {}
        self._unmonitored_emit_ttl: float = 600.0  # 同一 inst 10 分钟内只提示一次
        # —— 新增：已发送“监控就绪( true )”去重集 ——
        self._monitored_sent: set[str] = set()

    async def _bootstrap_all(self, minutes_by_inst: Optional[Dict[str, int]] = None):
        # 仅回灌一次（真正完成才置位）
        if self._bootstrapped:
            return
        logging.info("[OKX][BOOTSTRAP] begin (minutes=%d, insts=%s)",
                     self._bootstrap_minutes, ",".join(self.inst_ids))
        try:
            for inst in self.inst_ids:
                if self._bootstrap_mode == "full":
                    # 多轮回补：直到覆盖到 target_earliest 才算完成
                    rounds = 0
                    while True:
                        rem = await self._decide_full_remaining_minutes(inst)
                        if rem <= 0:
                            break
                        try:
                            upper_exclusive = await asyncio.to_thread(self._store.get_backfill_earliest_ms, inst)
                        except Exception:
                            upper_exclusive = None
                        rounds += 1
                        logging.info("[OKX][BOOTSTRAP] %s full-mode round #%d, remaining=%d min, upper<%s",
                                     inst, rounds, rem, upper_exclusive)
                        await self._bootstrap_trades(inst, minutes=rem, upper_ts_exclusive=upper_exclusive)
                    # 交易侧覆盖完成后，再按目标窗口回灌 TPO（用 1m K 线）
                    # 交易侧扫到目标最早点后，修复目标窗口内的缺口（按 4h 块阈值）
                    try:
                        await self._fix_gaps_full_window(inst)
                    except Exception as e:
                        logging.warning("[OKX][BOOTSTRAP] gap-fix error %s: %s", inst, e)
                    # 缺口处理后，再按目标窗口回灌 TPO（用 1m K 线）
                    await self._bootstrap_tpo(inst, minutes=self._bootstrap_minutes)
                else:
                    # gap 模式：按缺口分钟数回一次
                    m = (minutes_by_inst or {}).get(inst, self._bootstrap_minutes)
                    logging.info("[OKX][BOOTSTRAP] -> %s trades (gap-mode, %d min) ...", inst, m)
                    await self._bootstrap_trades(inst, minutes=m, upper_ts_exclusive=None,
                                                 replay_to_handlers=True, aggregate_replay=True)
                    logging.info("[OKX][BOOTSTRAP] -> %s tpo(1m, %d min) ...", inst, m)
                    await self._bootstrap_tpo(inst, minutes=m)
            logging.info("[OKX][BOOTSTRAP] all instruments fully backfilled — ready for WS")
            self._bootstrapped = True
        except Exception as e:
            logging.warning("[OKX][BOOTSTRAP] error: %s", e, exc_info=True)
            # 不设置 self._bootstrapped，交由外层策略决定是否继续或降级

    async def _bootstrap_trades(
        self,
        inst_id: str,
        minutes: int = 480,
        upper_ts_exclusive: Optional[int] = None,
        lower_ts_inclusive: Optional[int] = None,
        replay_to_handlers: Optional[bool] = None,
        *, aggregate_replay: bool = False,
    ):
        """
        回溯逐笔成交并分块(默认4h)落库。若给出上下界，则仅覆盖 [lower_ts_inclusive, upper_ts_exclusive)。
        当 replay_to_handlers 为 None 时，若设置了 upper_ts_exclusive（例如回补到 t0），默认**不回放**到上游；
        否则默认回放。可通过显式传入 True/False 覆盖该行为。
        当 aggregate_replay=True 时，采用“价阶×时间槽”聚合回放（方案二），避免逐笔重放。
        """
        # ✅ 如果 minutes<=0，说明我们已经覆盖到了需要的进度，直接跳过
        if int(minutes) <= 0:
            logging.info("[OKX][BOOTSTRAP] %s skip trades (minutes<=0)", inst_id)
            return
        # 锚定到 t0（upper_ts_exclusive）来决定窗口左边界，避免少补启动后到回补开始这几分钟
        ref_now_ms = int(upper_ts_exclusive) if upper_ts_exclusive is not None else int(time.time() * 1000)
        since_ms = ref_now_ms - max(1, int(minutes)) * 60_000
        # 决定是否回放给上游（默认：有 upper_ts_exclusive 时不回放）
        if replay_to_handlers is None:
            do_replay = (upper_ts_exclusive is None)
        else:
            do_replay = bool(replay_to_handlers)
        if lower_ts_inclusive is not None:
            # 允许更早翻页以拿到边界页，后续会对 <lower 的记录硬裁剪
            since_ms = min(since_ms, int(lower_ts_inclusive))
        # 先 history-trades（翻旧）, 再 trades-history 作兜底
        urls = [
            f"{self.rest_base_url}/api/v5/market/history-trades",
            f"{self.rest_base_url}/api/v5/market/trades-history",
        ]
        after_id: Optional[str] = None    # 分页游标：tradeId/billId（向更早翻页）
        # —— 分块缓冲（按 flush_minutes 聚合），避免整窗全部驻留内存 ——
        buf: List[dict] = []
        buf_min_ts: Optional[int] = None
        buf_max_ts: Optional[int] = None
        flush_span_ms = max(1, int(self._bootstrap_flush_minutes)) * 60_000
        first_chunk_committed = False  # 只在第一块（最新一段）更新 last_trade_state
        seen_ids: set[str] = set()
        seen_fallback: set[tuple] = set()
        dup_id = 0
        dup_fk = 0
        stop_reason = "unknown"
        pages = 0
        reached_window_end = False
        # 如果用户设置 pages<=0，表示不设上限；循环仅以“触达时间窗口下沿”或“游标未推进(卡页)”退出
        unlimited = (self._bootstrap_trade_pages <= 0)
        max_pages = self._bootstrap_trade_pages if self._bootstrap_trade_pages > 0 else 0

        # 统计：整体最早覆盖到的时间戳（用于写进度表）
        global_earliest_ts: Optional[int] = None
        page_added = 0  # 防止 while 未进入时后续引用未定义（UnboundLocalError）
        while (not reached_window_end) and (unlimited or pages < max_pages):
            rows = []
            # 明确声明以 tradeId 翻页：type=1（见 OKX 文档）
            params = {"instId": inst_id, "limit": "100", "type": "1"}
            if after_id:
                params["after"] = str(after_id)

            # 两个端点二选一（先试 history-trades，失败/无数据则试 trades-history）
            for u in urls:
                try:
                    data = await self._rest_get(u, params)
                    rows = (data or {}).get("data") or []
                    if rows:
                        break
                except Exception as e:
                    logging.warning("[OKX][BOOTSTRAP] %s fetch err on %s: %s", inst_id, u, e)
            logging.info("[OKX][BOOTSTRAP] %s page=%d len=%d after_id=%s url_used=%s",
                         inst_id, pages+1, len(rows), after_id, u if rows else None)
            if not rows:
                stop_reason = "no_rows"
                break

            # 确定分页 ID 字段
            id_key = "tradeId" if ("tradeId" in rows[0]) else ("billId" if ("billId" in rows[0]) else None)
            if id_key is None:
                stop_reason = "no_id_key"
                break

            # rows 通常从新到旧；筛选时间线内并暂存（附带去重 & 毫秒兜底）
            page_min_ts = None
            page_max_ts = None
            page_added = 0
            for r in rows:
                try:
                    ts = int(r.get("ts"))
                    if 0 < ts < 10_000_000_000:  # 若偶遇秒级时间戳，统一升为毫秒
                        ts *= 1000
                    px = float(r.get("px"))
                    sz = float(r.get("sz"))
                    side = str(r.get("side") or "").lower()
                    # 若给了上界（不含），跳过边界右侧（更近）的记录
                    if upper_ts_exclusive is not None and ts >= int(upper_ts_exclusive):
                        continue
                    # 若给了下界（含），一旦越界到更旧处则停止（不再翻页）
                    if lower_ts_inclusive is not None and ts < int(lower_ts_inclusive):
                        reached_window_end = True
                        stop_reason = "reached_lower_bound"
                        break
                    # 超出下界：达到了目标窗口的更早处
                    if ts < since_ms:
                        reached_window_end = True
                        stop_reason = "reached_window_end"
                        break

                    _tid = str(r.get(id_key, "")) if id_key else ""
                    if _tid:
                        if _tid in seen_ids:
                            dup_id += 1
                            continue
                        seen_ids.add(_tid)
                    else:
                        fk = (ts, px, sz, side)
                        if fk in seen_fallback:
                            dup_fk += 1
                            continue
                        seen_fallback.add(fk)
                    buf.append({
                        "ts": ts,
                        "px": px,
                        "sz": sz,
                        "side": side,
                        "tradeId": _tid,
                    })
                    # 维护本块范围
                    buf_min_ts = ts if (buf_min_ts is None or ts < buf_min_ts) else buf_min_ts
                    buf_max_ts = ts if (buf_max_ts is None or ts > buf_max_ts) else buf_max_ts
                    global_earliest_ts = ts if (global_earliest_ts is None or ts < global_earliest_ts) else global_earliest_ts

                    page_added += 1
                    if (page_min_ts is None) or (ts < page_min_ts): page_min_ts = ts
                    if (page_max_ts is None) or (ts > page_max_ts): page_max_ts = ts
                except Exception:
                    continue
            if page_min_ts is not None and page_max_ts is not None:
                dt_sec = (page_max_ts - page_min_ts) / 1000.0
                logging.info("[OKX][BOOTSTRAP] %s page=%d stats: range=%d→%d (Δ%.3fs) added=%d dup(id)=%d dup(fk)=%d",
                             inst_id, pages+1, page_min_ts, page_max_ts, dt_sec, page_added, dup_id, dup_fk)
            # 若已触达窗口下沿/下界，不再翻页
            if reached_window_end:
                break
            # 达到 4 小时块：落库 + 回放 + 写 checkpoint
            if buf and buf_min_ts is not None and buf_max_ts is not None:
                if (buf_max_ts - buf_min_ts) >= flush_span_ms:
                    # 按时间升序稳定排序
                    buf.sort(key=lambda x: (x["ts"], x.get("tradeId","")))
                    # 1) 批量落库（原子 upsert + VP 聚合）
                    await asyncio.to_thread(self._persist_trades_and_vp, inst_id, buf)
                    # 记录本次成功写入的覆盖区间
                    try:
                        await asyncio.to_thread(self._store.add_backfill_chunk, inst_id, int(buf_min_ts), int(buf_max_ts), "trades")
                    except Exception:
                        pass
                    # 2) 回放到上游（按需：逐笔 或 聚合）
                    if do_replay:
                        if aggregate_replay:
                            await self._replay_aggregated_profile(inst_id, buf)
                        else:
                            for t in buf:
                                trade = {
                                    "ts": t["ts"], "px": t["px"], "sz": t["sz"],
                                    "side": t.get("side",""), "tradeId": t.get("tradeId","")
                                }
                                coro = self.on_trade(inst_id, trade)
                                if asyncio.iscoroutine(coro):
                                    await coro
                    # 3) 仅在第一块（最新的一段）更新 last_trade_state（单调递增）
                    if not first_chunk_committed:
                        last_ts = buf[-1]["ts"]; last_tid = buf[-1].get("tradeId","")
                        await asyncio.to_thread(self._store.set_last_trade_state, inst_id, last_ts, last_tid)
                        first_chunk_committed = True
                    # 4) 写“回补进度”：最早覆盖到的时间戳（持续向更旧推进）
                    if global_earliest_ts is not None:
                        await asyncio.to_thread(self._store.set_backfill_earliest_ms, inst_id, int(global_earliest_ts))
                    # 5) 释放本块内存
                    buf.clear(); buf_min_ts = None; buf_max_ts = None

            # 往更旧翻页：使用该页最后一条记录的 tradeId/billId 作为 after 游标
            new_after = str(rows[-1].get(id_key)) if rows and rows[-1].get(id_key) else None
            # 防“卡页”：after_id 未推进则跳出，避免重复抓同一页
            if new_after == after_id:
                logging.warning("[OKX][BOOTSTRAP] %s stuck page (after_id=%s), break",
                                inst_id, after_id)
                stop_reason = "stuck_after_id"
                break
            after_id = new_after
            pages += 1
            # 轻微节流，平滑请求速率（防 429）
            if self._page_pause_s > 0:
                await asyncio.sleep(self._page_pause_s)

        # 若因为“页数上限”跳出（且未触达时间窗口），给出明确原因
        if (not reached_window_end) and (not unlimited) and (pages >= max_pages):
            stop_reason = "page_cap"

        if buf:
            buf.sort(key=lambda x: (x["ts"], x.get("tradeId","")))
            await asyncio.to_thread(self._persist_trades_and_vp, inst_id, buf)
            # 记录尾块覆盖区间
            try:
                await asyncio.to_thread(self._store.add_backfill_chunk, inst_id, int(buf[0]["ts"]), int(buf[-1]["ts"]), "trades")
            except Exception:
                pass
            if do_replay:
                if aggregate_replay:
                    await self._replay_aggregated_profile(inst_id, buf)
                else:
                    for t in buf:
                        trade = {"ts": t["ts"], "px": t["px"], "sz": t["sz"], "side": t.get("side",""), "tradeId": t.get("tradeId","")}
                        coro = self.on_trade(inst_id, trade)
                        if asyncio.iscoroutine(coro):
                            await coro
            if not first_chunk_committed:
                last_ts = buf[-1]["ts"]; last_tid = buf[-1].get("tradeId","")
                await asyncio.to_thread(self._store.set_last_trade_state, inst_id, last_ts, last_tid)
            if global_earliest_ts is not None:
                await asyncio.to_thread(self._store.set_backfill_earliest_ms, inst_id, int(global_earliest_ts))
            buf.clear()

        if not (buf or page_added or (global_earliest_ts is not None)):
            logging.info("[OKX][BOOTSTRAP] %s no trades within %d min (upper<%s)", inst_id, minutes, upper_ts_exclusive)
  
            return
        # 统计日志
        cov_first = global_earliest_ts if global_earliest_ts is not None else None
        cov_last  = None
        try:
            last_ts_state = await asyncio.to_thread(self._store.get_last_trade_ts, inst_id)
            cov_last = int(last_ts_state) if last_ts_state else None
        except Exception:
            pass
        if cov_first and cov_last and cov_last >= cov_first:
            span_min = (cov_last - cov_first) / 60000.0
            logging.info("[OKX][BOOTSTRAP] %s backfill window now covered: %d → %d (~%.1f min, pages=%d, stop=%s, dup_id=%d, dup_fk=%d, target=%dmin)",
                         inst_id, cov_first, cov_last, span_min, pages, stop_reason, dup_id, dup_fk, minutes)
        else:
            logging.info("[OKX][BOOTSTRAP] %s backfill done; pages=%d stop=%s dup_id=%d dup_fk=%d target=%dmin",
                         inst_id, pages, stop_reason, dup_id, dup_fk, minutes)
    
    async def _bootstrap_tpo(self, inst_id: str, minutes: int = 480):
        """
        通过 1m K 线回溯最近 N 分钟，按 mid≈(O+H+L+C)/4 回放到 on_tpo_sample(inst, mid, ts)。
        为了与实时一致，若 tpo_sample_ms < 60_000 且能整除 60_000，则对每根 1m K 线
        以步长 tpo_sample_ms 在这一分钟内补发多次采样（仅首个 1m 样本写 DB）。
        若未传入 on_tpo_sample，则跳过。
        """
        if not callable(self.on_tpo_sample):
            logging.info("[OKX][BOOTSTRAP] %s skip TPO backfill (no on_tpo_sample)", inst_id)
            return
        # 需要 N 根 1m K 线
        need = max(1, int(minutes))
        try:
            kl, source = await self._fetch_candles_for_tpo(inst_id, need)
        except Exception as e:
            logging.warning("[OKX][BOOTSTRAP] %s tpo candles error: %s", inst_id, e, exc_info=True)
            return
        if not kl:
            logging.info("[OKX][BOOTSTRAP] %s no candles for TPO", inst_id)
            return
        if len(kl) < minutes:
            logging.warning("[OKX][BOOTSTRAP] %s TPO candles insufficient: need=%d got=%d source=%s — coverage=~%.1fmin",
                            inst_id, minutes, len(kl), source, len(kl))
        # 从旧到新逐根喂给 TPO 采样；DB 仍按 1 分钟落库，但对引擎按 tpo_sample_ms 补发
        tpo_ms = int(getattr(self, "_tpo_sample_ms", 60_000) or 60_000)
        factor = 1
        if tpo_ms > 0 and (60_000 % tpo_ms == 0):
            factor = max(1, 60_000 // tpo_ms)
        sent_bars = 0
        sent_samples = 0
        tpo_rows = []
        for ts, o, h, l, c in kl:
            mid = (o + h + l + c) / 4.0
            ts_min = int(ts)  # OKX 1m K 线时间戳为分钟起始 ms
            # 仅把“分钟起始”写 DB（保持 schema 不变）
            tpo_rows.append((ts_min, float(mid)))
            # 向引擎补发 factor 次（0s、+tpo_ms、…），与实时密度一致
            for k in range(factor):
                emit_ts = ts_min + k * tpo_ms
                coro = self.on_tpo_sample(inst_id, float(mid), int(emit_ts))
                if asyncio.iscoroutine(coro):
                    await coro
                sent_samples += 1
            sent_bars += 1
        # 批量写入 TPO 分钟
        try:
            await asyncio.to_thread(self._store.bulk_tpo_upsert, inst_id, tpo_rows)
            if tpo_rows:
                await asyncio.to_thread(self._store.set_last_tpo_min, inst_id, tpo_rows[-1][0])
        except Exception as e:
            logging.warning("[OKX][TPO][DB] %s bulk upsert error: %s", inst_id, e)
  
        logging.info(
            "[OKX][BOOTSTRAP] %s backfilled %d 1m-bars; emitted %d TPO samples (factor=%dx) via %s klines(1m)",
            inst_id, sent_bars, sent_samples, factor, source
        )
    async def _fetch_candles_for_tpo(self, inst_id: str, need_bars: int) -> Tuple[List[Tuple[int, float, float, float, float]], str]:
        """
        拉 1m 标记价K线（优先）/交易价K线，用于 TPO 回灌。
        返回升序 [(ts_ms, o, h, l, c)] 与 source 标记。
        """
        # 优先标记价（更合适做 TPO/风控）
        try:
            kl = await self._get_mark_price_candles(inst_id, "1m", max_n=need_bars)
            if kl:
                return kl, "mark"
        except Exception:
            pass
        # 退回交易价
        kl2 = await self._get_trade_price_candles(inst_id, "1m", max_n=need_bars)
        return kl2, "last"
    # ======================= 生命周期 =======================
    async def start(self):
        if not self._session:
            # 全局可宽松，但每个请求单独再设一个合理超时
            self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60))
        # 记录启动时刻（毫秒），作为历史回灌的上界
        if self._startup_t0 is None:
            self._startup_t0 = int(time.time() * 1000)
            logging.info("[OKX][BOOTSTRAP] startup t0=%d", self._startup_t0)
        # ⭐ 不再修改 self.inst_ids，而是拆分“有效/无效”并对无效的发出回退提示
        self._valid_insts = []
        self._invalid_insts = []
        if self.inst_ids:
            try:
                swaps = await self._load_okx_swaps()
            except Exception as e:
                logging.warning("[OKX] load swaps failed: %s (fallback use all instIds)", e)
                swaps = set()
            if swaps:
                self._valid_insts = [i for i in self.inst_ids if i in swaps]
                self._invalid_insts = [i for i in self.inst_ids if i not in swaps]
            else:
                # 拿不到清单：全部先当作有效（保持原行为）
                self._valid_insts = list(self.inst_ids)
            # 对未监控（无效）的交易对，明确通知消费端回退 ATR（不订阅它们）
            for _inst in self._invalid_insts:
                asyncio.create_task(self._notify_unmonitored(_inst, "no_swap_on_okx"))

        # 本地订单簿对象仅需为“可能会订阅”的标的准备
        self._books.update({inst: self._books.get(inst, _BookState()) for inst in self._valid_insts})
 
        # —— 新策略：先判断每个标的剩余回补分钟数 ——
        minutes_map: Dict[str, int] = {}
        if self._bootstrap_minutes > 0:
            try:
                if self._bootstrap_mode == "full":
                    base = self._valid_insts or self.inst_ids
                    for inst in base:
                        minutes_map[inst] = await self._decide_full_remaining_minutes(inst)
                else:
                    base = self._valid_insts or self.inst_ids
                    for inst in base:
                        minutes_map[inst] = await self._decide_backfill_minutes(inst)
            except Exception:
                logging.warning("[OKX][BOOTSTRAP] prepare minutes_map error", exc_info=True)
        # 仅用于日志展示
        covered = [i for i, m in minutes_map.items() if m <= 0]
        pending  = [i for i, m in minutes_map.items() if m > 0]
        if minutes_map:
            logging.info(
                "[OKX][BOOTSTRAP] minutes_map=%s covered=%s pending=%s",
                {k: int(v) for k, v in minutes_map.items()},
                covered, pending
            )

        while True:
            try:
                async with self._session.ws_connect(
                    self.ws_url,
                    heartbeat=20.0,
                    compress=15,
                    autoping=True
                ) as ws:
                    self._ws = ws
                    self._reconnect_backoff = 1.0
                    # 先发起（可选）登录
                    subscribed = False
                    self._login_ok = False
                    if self._need_login:
                        # 等读循环里收 event=login 成功后再订阅
                        await self._ws_login(wait=False)
                        # 登录 ACK 兜底：若 2 秒仍未订阅，则只做订阅（不做任何回灌）
                        async def _login_sub_fallback():
                            await asyncio.sleep(2.0)
                            nonlocal subscribed
                            if not subscribed:
                                await self._subscribe_all()
                                subscribed = True
                                asyncio.create_task(self._warmup_books())
                                if self._bootstrap_minutes > 0 and not self._backfill_started:
                                    asyncio.create_task(self._run_backfills_to_t0(minutes_map or None))
                                    self._backfill_started = True
                        asyncio.create_task(_login_sub_fallback())
                    else:
                        # 不需要登录：直接全量订阅
                        await self._subscribe_all()
                        subscribed = True
                        asyncio.create_task(self._warmup_books())
                        # 订阅完立刻在后台并行回灌到 t0（WS 已开始吃实时）
                        if self._bootstrap_minutes > 0 and not self._backfill_started:
                            asyncio.create_task(self._run_backfills_to_t0(minutes_map or None))
                            self._backfill_started = True
 
                
                    # 主读循环：登录成功后再订阅
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            data = ujson.loads(msg.data)
                            await self._route_message(data)
                            # 登录成功 → 延迟订阅
                            if (not subscribed) and (not self._need_login or self._login_ok):

                                await self._subscribe_all()
                                subscribed = True
                                asyncio.create_task(self._warmup_books())
                                if self._bootstrap_minutes > 0 and not self._backfill_started:
                                    asyncio.create_task(self._run_backfills_to_t0(minutes_map or None))
                                    self._backfill_started = True

                        elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            raise ConnectionError(f"WS closed: {msg.type}")
                        # BINARY 由 aiohttp 解压，这里无需特别处理

            except asyncio.CancelledError:
                raise
            except Exception as e:
                logging.warning("[OKX][WS] loop error: %s", e, exc_info=True)
                await asyncio.sleep(self._reconnect_backoff)
                self._reconnect_backoff = min(self._reconnect_backoff * 1.7, 30.0)

    async def close(self):
        try:
            if self._ws:
                await self._ws.close()
        finally:
            if self._session:
                await self._session.close()

    # ======================= 订阅 & 路由 =======================
    async def _subscribe_all(self):
        subs = []
        base = self._valid_insts or self.inst_ids
        for inst in base:
            subs.append({"channel": self.orderbook_channel, "instId": inst})
            subs.append({"channel": self.trades_channel, "instId": inst})
            subs.append({"channel": self.tickers_channel, "instId": inst})

        await self._ws.send_json({"op": "subscribe", "args": subs})
        logging.info("[OKX] subscribe %d instruments: %s / %s / %s",
                     len(base), self.orderbook_channel, self.trades_channel, self.tickers_channel)
       # 维护已订阅集合，便于后续降级只针对已订阅对象重订阅
        try:
            self._subscribed_insts.update(base)
        except Exception:
            pass
        # 对未订阅（未监控）的 inst 再次提示一次（防止消费端先于 start() 建立）
        for _inst in self._invalid_insts:
            asyncio.create_task(self._notify_unmonitored(_inst, "no_swap_on_okx"))
        # ⭐ 新增：对于“有效但尚未BBO就绪”的标的，显式告知【暂不监控】（防默认当成已监控）
        now_ms = int(time.time() * 1000)
        for _inst in base:
            try:
                await self._emit_status({
                    "event": "MONITOR_STATUS",
                    "instId": _inst,
                    "coin": self._inst_coin(_inst),
                    "monitored": False,
                    "reason": "pending_book_ready",
                    "ts": now_ms
                })
            except Exception:
                pass
    async def _resubscribe_orderbook_only(self):
        """
        当 books-l2-tbt 订阅失败时，仅对「已订阅集合」执行订单簿降级重订阅。
        """
        try:
            # 尝试退订可能的 l2-tbt 失败/半订阅（幂等，不会出错）
            base = sorted(self._subscribed_insts) or self._valid_insts or self.inst_ids
            args_unsub = [{"channel": "books-l2-tbt", "instId": inst} for inst in base]
            await self._ws.send_json({"op": "unsubscribe", "args": args_unsub})
        except Exception:
            pass
        # 以当前 self.orderbook_channel 重新订阅订单簿
        base = sorted(self._subscribed_insts) or self._valid_insts or self.inst_ids
        args_sub = [{"channel": self.orderbook_channel, "instId": inst} for inst in base]
        await self._ws.send_json({"op": "subscribe", "args": args_sub})
        logging.info("[OKX] re-subscribe orderbook only: %s", self.orderbook_channel)
        # 重新订阅后后台预热（避免降级/重订阅后的空簿窗口）
        asyncio.create_task(self._warmup_books())

    async def _warmup_books(self):
        """
        订阅后用 REST /api/v5/market/books 预热本地订单簿，避免冷启动时上游拿到空簿。
        不阻塞 WS 读循环：请用 asyncio.create_task(self._warmup_books()) 调用本方法。
        """
        # 只针对“已在 WS 上订阅”的标的进行预热；若集合为空则退回全部 instIds
        base = sorted(self._subscribed_insts) or self._valid_insts or self.inst_ids
        for inst in base:
            try:
                ok = await self._resync_with_rest_snapshot(inst, emit=True)
                if ok:
                    logging.info("[OKX][WARMUP] snapshot ready for %s", inst)
                else:
                    logging.debug("[OKX][WARMUP] snapshot unavailable for %s", inst)
            except Exception as e:
                logging.warning("[OKX][WARMUP] %s error: %s", inst, e)
                # 预热失败不影响主流程，等待 WS 首帧覆盖
                continue

    async def _route_message(self, data: dict):
        if "event" in data:
            ev = data.get("event")
            if ev == "login":
                code = str(data.get("code", ""))
                if code == "0":
                    self._login_ok = True
                    if self._login_waiter and not self._login_waiter.done():
                        self._login_waiter.set_result(True)
                    logging.info("[OKX] WS login success")
                else:
                    errmsg = data.get("msg") or data
                    if self._login_waiter and not self._login_waiter.done():
                        self._login_waiter.set_exception(RuntimeError(f"login failed: {errmsg}"))
                    logging.warning("[OKX][WS] login failed: %s", errmsg)
                return
            if ev == "subscribe":
                arg = data.get("arg", {})
                logging.info("[OKX] subscribed: %s %s", arg.get("channel"), arg.get("instId"))
                return
            if ev == "error":
                arg = data.get("arg", {}) or {}
                ch = arg.get("channel")
                inst_in_err = arg.get("instId")
                code = str(data.get("code", ""))
                # 不论 error 是否带 arg，只要我们当前想订 l2-tbt 且报了这些码，就处理
                if ("l2-tbt" in self.orderbook_channel) and (code in ("60011", "60029", "60018")):
  
                    # 60011：需要登录。若有密钥且尚未登录，则先登录后仅重订阅订单簿；否则降级到 'books'
                    if code == "60011" and not self._login_ok:
                        if self._has_creds:
                            logging.warning("[OKX] l2-tbt requires login; trying WS login then re-subscribe ...")
                            try:
                                await self._ws_login()
                                await self._resubscribe_orderbook_only()
                                return
                            except Exception as e:
                                logging.warning("[OKX] WS login failed: %s — fallback to 'books'", e)
                        # 无密钥或登录失败 → 降级
                        logging.warning("[OKX] fallback to 'books' (no creds or login failed)")
                        self.orderbook_channel = "books"
                        await self._resubscribe_orderbook_only()
                        await self._emit_status({"event": "fallback", "channel": "books"})
                    else:
                        # 60029/60018 等其它错误也直接降级，避免卡死在 l2-tbt
                        logging.warning("[OKX] l2-tbt subscribe error(code=%s) → fallback to 'books'", code)
                        self.orderbook_channel = "books"
                        await self._resubscribe_orderbook_only()
                        await self._emit_status({"event": "fallback", "channel": "books"})
                else:
                    logging.warning("[OKX][WS][error] %s", data)
                    # 其它订阅错误：对具体 inst 通知“未监控”，提示消费端回退 ATR
                    if inst_in_err and ch in {"books-l2-tbt", "books", "trades", "tickers"}:
                        try:
                            asyncio.create_task(self._notify_unmonitored(inst_in_err, f"ws_subscribe_error:{code}"))
                        except Exception:
                            pass
                return

        arg = data.get("arg", {})
        ch = arg.get("channel")
        inst = arg.get("instId")
        if not ch or not inst:
            return

        if ch in ("books-l2-tbt", "books50-l2-tbt", "books"):
            for row in data.get("data", []):
                await self._handle_orderbook_row(inst, row, ch)
            return

        if ch == "trades":
            for t in data.get("data", []):
                await self._handle_trade_row(inst, t)
            return

        if ch == "tickers":
            # 当前未用；保留以备将来需要 last/mid 兜底
            return

    # ======================= 订单簿处理 =======================
    async def _handle_orderbook_row(self, inst: str, row: dict, ch: str):
        st = self._books[inst]
        ts = int(row.get("ts", time.time() * 1000))
        seq = _to_int(row.get("seqId"))
        prev = _to_int(row.get("prevSeqId"))
        # === 非 tbt：books 是“快照”，不做 seq 对齐与增量回放 ===
        if ch == "books":
            self._apply_snapshot(st, row)
            st.last_seq = seq  # 可能为 None；books 不依赖它
            st.last_ts = ts
            await self._emit_book_snapshot(inst, st, ts)
            return
        # 首帧：记录 prev，拉 REST 快照，对齐后回放缓存
        if st.last_seq is None and st.first_prev_seq is None:
            st.first_prev_seq = prev
            st.buffer.append(row)
            ok = await self._resync_with_rest_snapshot(inst)
            if ok:
                await self._replay_buffer(inst)
            return

        # 断序：重新快照并回放
        if st.last_seq is not None:
            if not (prev == st.last_seq or prev == seq):
                logging.warning("[OKX][OB] seq mismatch %s: have=%s, prev=%s, seq=%s → resync",
                                inst, st.last_seq, prev, seq)
                st.buffer.clear()
                st.first_prev_seq = prev
                st.buffer.append(row)
                ok = await self._resync_with_rest_snapshot(inst)
                if ok:
                    await self._replay_buffer(inst)
                return

        # 应用增量
        self._apply_deltas(st, row)
        st.last_seq = seq
        st.last_ts = ts

        await self._emit_book_snapshot(inst, st, ts)
    def _apply_snapshot(self, st: _BookState, row: dict) -> None:
        """books 频道：整帧快照替换"""
        st.bids.clear(); st.asks.clear()
        for px_str, sz_str, *_ in row.get("bids", []):
            try:
                px = float(px_str); sz = float(sz_str)
                if sz > 0: st.bids[px] = sz
            except Exception:
                continue
        for px_str, sz_str, *_ in row.get("asks", []):
            try:
                px = float(px_str); sz = float(sz_str)
                if sz > 0: st.asks[px] = sz
            except Exception:
                continue
    async def _resync_with_rest_snapshot(self, inst: str, *, emit: bool = False) -> bool:
        """用 JSON 快照 /api/v5/market/books 对齐（官方建议流程）。"""
        url = f"{self.rest_base_url}/api/v5/market/books"
        params = {"instId": inst, "sz": "50"}  # 显式深度，避免默认太浅
        try:
            data = await self._rest_get(url, params)
        except Exception:
            logging.exception("[OKX][REST] snapshot error %s", inst)
            return False

        if data.get("code") != "0" or not data.get("data"):
            logging.warning("[OKX][REST] snapshot empty %s: %s", inst, data)
            return False

        snap = data["data"][0]
        bids = snap.get("bids", [])
        asks = snap.get("asks", [])
        seq = _to_int(snap.get("seqId"))
        ts = _to_int(snap.get("ts"))

        st = self._books[inst]
        st.bids.clear(); st.asks.clear()
        for px_str, sz_str, *_ in bids:
            px = float(px_str); sz = float(sz_str)
            if sz > 0: st.bids[px] = sz
        for px_str, sz_str, *_ in asks:
            px = float(px_str); sz = float(sz_str)
            if sz > 0: st.asks[px] = sz
        # REST 有时不给 seqId（尤其非 tbt 使用该接口）；回退用首帧 prevSeqId 以完成一次性对齐
        st.last_seq = seq if seq is not None else st.first_prev_seq
        st.last_ts = ts
        logging.info("[OKX][REST] snapshot loaded %s seq=%s", inst, st.last_seq)
        await self._emit_status({"event": "resync", "instId": inst, "seq": st.last_seq})
        # 仅在主动预热场景下，立即把快照发给上游，快速触发 book_ready（tbt 对齐路径仍由 _replay_buffer() 负责）。
        if emit:
            try:
                await self._emit_book_snapshot(inst, st, ts or int(time.time() * 1000))
            except Exception:
                pass
        return True

    async def _replay_buffer(self, inst: str):
        st = self._books[inst]
        for row in st.buffer:
            prev = _to_int(row.get("prevSeqId"))
            seq = _to_int(row.get("seqId"))
            if not (prev == st.last_seq or prev == seq):
                logging.warning("[OKX][OB] buffer mismatch %s: have=%s, prev=%s, seq=%s",
                                inst, st.last_seq, prev, seq)
                st.buffer.clear()
                return
            self._apply_deltas(st, row)
            st.last_seq = seq
            st.last_ts = int(row.get("ts", st.last_ts or 0))
        st.buffer.clear()
        await self._emit_book_snapshot(inst, st, st.last_ts or int(time.time() * 1000))

    def _apply_deltas(self, st: _BookState, row: dict):
        for px_str, sz_str, *_ in row.get("bids", []):
            px = float(px_str); sz = float(sz_str)
            if sz == 0: st.bids.pop(px, None)
            else: st.bids[px] = sz
        for px_str, sz_str, *_ in row.get("asks", []):
            px = float(px_str); sz = float(sz_str)
            if sz == 0: st.asks.pop(px, None)
            else: st.asks[px] = sz

    async def _emit_book_snapshot(self, inst: str, st: _BookState, ts_ms: int):
        b1 = max(st.bids.items(), key=lambda kv: kv[0]) if st.bids else None
        a1 = min(st.asks.items(), key=lambda kv: kv[0]) if st.asks else None
        ready = bool(b1 and a1)
        # 🚫 没有完整 BBO 就不发给交易侧
        if not ready:
            try:
                logging.debug("[OKX][OB][SKIP] %s no BBO yet (bids=%d asks=%d) ts=%d",
                              inst, len(st.bids), len(st.asks), ts_ms)
            except Exception:
                pass
            return
        def _top(levels: Dict[float, float], n: int, rev: bool) -> List[Tuple[float, float]]:
            if not levels: return []
            return sorted(levels.items(), key=lambda kv: kv[0], reverse=rev)[:n]

        top5  = _top(st.bids, 5, True),  _top(st.asks, 5, False)
        top10 = _top(st.bids, 10, True), _top(st.asks, 10, False)
        payload = {
            "ts": ts_ms,
            "seqId": st.last_seq,
            # 兼容原有结构
            "l1": {
                "bid": [b1[0], b1[1]] if b1 else None,
                "ask": [a1[0], a1[1]] if a1 else None,
            },
            "l5":  {"bids": top5[0],  "asks": top5[1]},
            "l10": {"bids": top10[0], "asks": top10[1]},

            # ===== 扁平 BBO 字段：缺失时用 None，而不是 0.0（避免被消费端当作“缺失/False”） =====
            "best_bid": (b1[0] if b1 else None),
            "best_bid_size": (b1[1] if b1 else None),
            "best_ask": (a1[0] if a1 else None),
            "best_ask_size": (a1[1] if a1 else None),
            "bids": top10[0],  # 提供至少 max(depth_levels)=10 档
            "asks": top10[1],
        }
        # 首次就绪时发一条状态 + 日志，便于排查“谁先到”
        if ready and not st.bbo_ready:
            st.bbo_ready = True
            try:
                await self._emit_status({"event": "book_ready", "instId": inst, "ts": ts_ms})
            except Exception:
                pass
            try:
                import logging
                logging.info("[OKX][BBO][READY] %s bid=%.8f ask=%.8f ts=%d", inst, b1[0], a1[0], ts_ms)
            except Exception:
                pass
            # —— 新增：首次就绪即广播“已监控”状态（供消费端切回 VP/HVN/LVN）
            try:
                if inst not in self._monitored_sent:
                    await self._emit_status({
                        "event": "MONITOR_STATUS",
                        "instId": inst,
                        "coin": self._inst_coin(inst),
                        "monitored": True,
                        "reason": "book_ready",
                        "ts": ts_ms
                    })
                    self._monitored_sent.add(inst)
            except Exception:
                pass
        coro = self.on_orderbook(inst, payload)
        if asyncio.iscoroutine(coro):
            await coro

    # ----------------------- 健康快照（供 HealthBeat） -----------------------
    def health_snapshot(self) -> dict:
        now = int(time.time() * 1000)
        by_inst = {}
        for inst, st in self._books.items():
            last_ts = st.last_ts or 0
            by_inst[inst] = {
                "lastSeq": st.last_seq,
                "lastMsgAgoMs": (now - last_ts) if last_ts else None,
                "bookReady": bool(getattr(st, "bbo_ready", False)),
            }
        return {
            "wsConnected": bool(self._ws and not getattr(self._ws, "closed", False)),
            "inst": by_inst
        }
    # ======================= 成交处理 =======================
    async def _handle_trade_row(self, inst: str, t: dict):
        try:
            trade = {
                "ts": int(t.get("ts")),
                "px": float(t.get("px")),
                "sz": float(t.get("sz")),
                "side": str(t.get("side") or "").lower(),
                "tradeId": t.get("tradeId") or t.get("billId") or "",
            }
        except Exception:
            return
        coro = self.on_trade(inst, trade)
        if asyncio.iscoroutine(coro):
            await coro
        # —— 实时增量落库：VP(bin) 聚合；原始成交按开关决定 ——
        try:
            if self._store_raw_trades:
                await asyncio.to_thread(
                    self._store.upsert_trade, inst, trade["tradeId"], trade["ts"], trade["px"], trade["sz"], trade["side"]
                )
            bin_px = self._bin_price(inst, trade["px"])
            buy = trade["sz"] if trade["side"] == "buy" else 0.0
            sell = trade["sz"] if trade["side"] == "sell" else 0.0
            await asyncio.to_thread(self._store.vp_accumulate, inst, bin_px, buy, sell)
            await asyncio.to_thread(self._store.set_last_trade_state, inst, trade["ts"], trade.get("tradeId",""))
        except Exception as e:
            logging.debug("[OKX][DB] trade persist error %s: %s", inst, e)

    async def _emit_status(self, status: dict):
        """
        统一向上游发送状态事件。
        - 对 MONITOR_STATUS 自动补全你的消息结构字段：
            etype="INFO"
            ts: 若缺失则填当前毫秒
            msg_id: f"mon:{instId}:{ts//1000}"
            coin: 若缺失则由 instId 推断
        - 其它事件保持原样透传。
        """
        now = int(time.time() * 1000)
        evt = str(status.get("event", ""))
        if evt == "MONITOR_STATUS":
            status.setdefault("etype", "INFO")
            ts_ms = status.get("ts")
            if not isinstance(ts_ms, int):
                ts_ms = now
                status["ts"] = ts_ms
            inst = str(status.get("instId") or "")
            if not status.get("coin"):
                status["coin"] = self._inst_coin(inst)
            status.setdefault("msg_id", f"mon:{inst}:{ts_ms // 1000}")
        else:
            # 对关键事件放开节流，避免多 inst 同时就绪时丢事件
            if evt not in {"book_ready", "resync", "fallback"}:
                if now - self._last_status_ts < 500:  # 节流
                    return
        self._last_status_ts = now
        coro = self.on_status(status)
        if asyncio.iscoroutine(coro):
            await coro
 
    # ======================= —— 这里开始是新增的 REST 数据源 —— =======================

    # ---------- 外部可用：波动刻度状态（ATR% / median%） ----------
    async def get_vol_state(self, inst_id: str) -> Dict[str, float]:
        """
        返回 {'atr_pct': float, 'median_pct': float, 'samples': int, 'source': 'mark'|'last', 'ts': epoch_ms}
        带 TTL 缓存。优先用标记价K线（更适合风控），失败退回交易价K线。
        """
        st = self._vol_cache.get(inst_id)
        now = time.time()
        if st and (now - float(st.get("ts", 0)) < self._vol_ttl):
            return st

        candles, source = await self._fetch_candles_with_fallback(inst_id)
        atr_pct = self._compute_atr_pct(candles, self._atr_window)
        med_pct = self._compute_median_abs_return_pct(candles)

        res = {
            "atr_pct": atr_pct,
            "median_pct": med_pct,
            "samples": len(candles),
            "source": source,
            "ts": now,
        }
        self._vol_cache[inst_id] = res
        return res

    # ---------- 外部可用：即时标记价 ----------
    async def get_mark_price(self, inst_id: str) -> Optional[float]:
        """
        GET /api/v5/public/mark-price
        """
        url = f"{self.rest_base_url}/api/v5/public/mark-price"
        params = {"instId": inst_id}
        data = await self._rest_get(url, params)
        rows = (data or {}).get("data") or []
        if not rows:

            return None
        row = rows[0]
        for k in ("markPx", "markPrice", "mark_price", "px"):
            if k in row:
                try:
                    v = float(row[k])
                    return v if v > 0 else None
                except Exception:
                    continue
        return None

    # ---------- 外部可用：资金费率历史 ----------
    async def get_funding_rate(self, inst_id: str, limit: int = 10) -> List[Dict[str, float]]:
        """
        GET /api/v5/public/funding-rate-history
        返回 [{'fundingRate': float, 'fundingTime': int(ms)} ...]
        """
        limit = max(1, min(int(limit), 100))
        url = f"{self.rest_base_url}/api/v5/public/funding-rate-history"
        params = {"instId": inst_id, "limit": str(limit)}
        data = await self._rest_get(url, params)
        rows = (data or {}).get("data") or []
        out: List[Dict[str, float]] = []
        for r in rows:
            try:
                fr = float(r.get("fundingRate"))
                ts = int(r.get("fundingTime"))
                out.append({"fundingRate": fr, "fundingTime": ts})
            except Exception:
                continue
        return out

    # ---------- 内部：取 K 线（优先标记价） ----------
    async def _fetch_candles_with_fallback(self, inst_id: str) -> Tuple[List[Tuple[int, float, float, float, float]], str]:
        if not self._is_missed(inst_id, "mark_klines"):
            candles = await self._get_mark_price_candles(inst_id, self._bar, self._kl_max)
            if candles:
                return candles, "mark"
            self._mark_miss(inst_id, "mark_klines", ttl=60.0)  # 1 分钟内不再尝试

        candles = await self._get_trade_price_candles(inst_id, self._bar, self._kl_max)
        return candles, "last"

    async def _get_trade_price_candles(self, inst_id: str, bar: str, max_n: int) -> List[Tuple[int, float, float, float, float]]:
        """
        GET /api/v5/market/candlesticks + /api/v5/market/history-candlesticks（翻页）
        返回升序 [(ts_ms, o, h, l, c)]
        """
        url_curr = f"{self.rest_base_url}/api/v5/market/candlesticks"
        url_hist = f"{self.rest_base_url}/api/v5/market/history-candlesticks"
        got: List[Tuple[int, float, float, float, float]] = []

        params = {"instId": inst_id, "bar": bar, "limit": str(min(300, max_n))}
        data = await self._rest_get(url_curr, params)
        rows = (data or {}).get("data") or []
        got.extend(self._parse_kl_rows(rows))

        pages = 0
        while len(got) < max_n and rows:
            last_ts = min(x[0] for x in got) if got else None
            if last_ts is None:
                break
            need = max_n - len(got)
            params = {
                "instId": inst_id,
                "bar": bar,
                "before": str(last_ts - 1),              # 避免包含边界导致重复页
                "limit": str(min(300, need))
            }
            data = await self._rest_get(url_hist, params)
            rows = (data or {}).get("data") or []
            parsed = self._parse_kl_rows(rows)
            if not parsed:
                break
            exists = {ts for ts, *_ in got}
            added = 0
            for item in parsed:
                if item[0] not in exists:
                    got.append(item)
                    added += 1
            # 没有新增说明命中重复页或无更旧数据，退出
            if added == 0:
                break
            pages += 1
            # 保险丝，避免异常接口导致长循环
            if pages > 50:
                break

        got.sort(key=lambda x: x[0])
        return got[-max_n:]

    async def _get_mark_price_candles(self, inst_id: str, bar: str, max_n: int) -> List[Tuple[int, float, float, float, float]]:
        """
        GET /api/v5/market/mark-price-candles + /api/v5/market/history-mark-price-candles
        返回升序 [(ts_ms, o, h, l, c)]
        """
        url_curr = f"{self.rest_base_url}/api/v5/market/mark-price-candles"
        url_hist = f"{self.rest_base_url}/api/v5/market/history-mark-price-candles"
        got: List[Tuple[int, float, float, float, float]] = []

        params = {"instId": inst_id, "bar": bar, "limit": str(min(300, max_n))}
        data = await self._rest_get(url_curr, params)
        rows = (data or {}).get("data") or []
        got.extend(self._parse_kl_rows(rows))

        pages = 0
        while len(got) < max_n and rows:
            last_ts = min(x[0] for x in got) if got else None
            if last_ts is None:
                break
            need = max_n - len(got)
            params = {
                "instId": inst_id,
                "bar": bar,
                "before": str(last_ts - 1),              # 避免包含边界导致重复页
                "limit": str(min(300, need))
            }
            data = await self._rest_get(url_hist, params)
            rows = (data or {}).get("data") or []
            parsed = self._parse_kl_rows(rows)
            if not parsed:
                break
            exists = {ts for ts, *_ in got}
            added = 0
            for item in parsed:
                if item[0] not in exists:
                    got.append(item)
                    added += 1
            # 没有新增说明命中重复页或无更旧数据，退出
            if added == 0:
                break
            pages += 1
            # 保险丝，避免异常接口导致长循环
            if pages > 50:
                break

        got.sort(key=lambda x: x[0])
        return got[-max_n:]

    @staticmethod
    def _parse_kl_rows(rows: List[List[str]]) -> List[Tuple[int, float, float, float, float]]:
        out: List[Tuple[int, float, float, float, float]] = []
        for r in rows:
            try:
                ts = int(r[0]); o = float(r[1]); h = float(r[2]); l = float(r[3]); c = float(r[4])
                if ts > 0 and min(o, h, l, c) > 0:
                    out.append((ts, o, h, l, c))
            except Exception:
                continue
        return out

    @staticmethod
    def _compute_atr_pct(candles: List[Tuple[int, float, float, float, float]], window: int = 14) -> float:
        """
        Wilder ATR（RMA/TR）→ 以末根收盘为基数的百分比
        """
        n = max(2, int(window))
        if len(candles) <= n:
            return 0.0
        prev_close = candles[0][4]
        trs: List[float] = []
        for _, _o, h, l, c in candles[1:]:
            tr = max(h - l, abs(h - prev_close), abs(l - prev_close))
            trs.append(tr)
            prev_close = c
        if len(trs) < n:
            return 0.0
        atr = sum(trs[:n]) / n
        for x in trs[n:]:
            atr = (atr * (n - 1) + x) / n
        denom = candles[-1][4]
        return (atr / max(1e-12, abs(denom))) * 100.0

    @staticmethod
    def _compute_median_abs_return_pct(candles: List[Tuple[int, float, float, float, float]]) -> float:
        if len(candles) < 3:
            return 0.0
        rets = []
        for i in range(1, len(candles)):
            p0 = candles[i - 1][4]; p1 = candles[i][4]
            if p0 > 0 and p1 > 0:
                rets.append(abs(p1 - p0) / p0 * 100.0)
        return float(statistics.median(rets)) if rets else 0.0

    # ---------- REST 基础：重试 + 退避 ----------
    async def _rest_get(self, url: str, params: dict) -> dict:
        """
        带指数退避的 GET：
          - 429/5xx：尊重 Retry-After（若有），指数退避 + 抖动后重试
          - 404：立即返回空对象（不重试；由上层按需做 miss 记忆）
          - HTTP 200 且 code=50011/50061：视为限流，退避重试（见 OKX 文档）
        """
        limiter = self._pick_rate_limiter(url)
        delay = float(self._rest_backoff)
        last_err: Optional[Exception] = None
        RATE_LIMIT_CODES = {"50011", "50061"}
        for attempt in range(1, self._rest_retries + 1):
            try:
                assert self._session is not None, "ClientSession not ready"
                async with self._rest_sem:  # 👉 额外全局并发闸门
                    await limiter.acquire()  # 端点级令牌桶限流（全局生效，替代局部 sleep）
                    per_req_to = aiohttp.ClientTimeout(connect=5, sock_read=8, total=10)
                    async with self._session.get(url, params=params, timeout=per_req_to) as resp:
                        text = await resp.text()
                        status = resp.status
                    # ---- 404：不可重试，直接空返回 ----
                    if status == 404:
                        logging.info("[OKX][REST] 404 Not Found: %s %s", url, params)
                        return {}
                    # ---- 429/5xx：可重试（尊重 Retry-After）----
                    if status == 429 or 500 <= status < 600:
                        ra = resp.headers.get("Retry-After")
                        try:
                            ra_s = float(ra) if ra is not None else 0.0
                        except Exception:
                            ra_s = 0.0
                        wait_s = max(delay, ra_s)
                        if attempt == self._rest_retries:
                            raise RuntimeError(f"HTTP {status} {text[:200]}")
                        jitter = 0.3 * (attempt % 3)
                        logging.info("[OKX][REST][retry %d/%d] HTTP %d — wait %.2fs (Retry-After=%s)",
                                     attempt, self._rest_retries, status, wait_s + jitter, ra)
                        await asyncio.sleep(wait_s + jitter)
                        delay *= 1.6
                        continue
                    # ---- 正常 2xx：解析业务 code ----
                    if 200 <= status < 300:
                        data = ujson.loads(text)
                        code = str(data.get("code", "0"))
                        if code in RATE_LIMIT_CODES:
                            # 被限流：50011/50061（子账号），按退避重试
                            if attempt == self._rest_retries:
                                raise RuntimeError(f"OKX rate limited: code={code} {data.get('msg')}")
                            jitter = 0.3 * (attempt % 3)
                            logging.info("[OKX][REST][retry %d/%d] business rate limit(code=%s) — wait %.2fs",
                                         attempt, self._rest_retries, code, delay + jitter)
                            await asyncio.sleep(delay + jitter)
                            delay *= 1.6
                            continue
                        return data
                    # 其它非 2xx：抛出进入通用重试
                    raise RuntimeError(f"HTTP {status} {text[:200]}")
            except Exception as e:
                last_err = e
                if attempt == self._rest_retries:
                    raise
                jitter = 0.3 * (attempt % 3)
                logging.info("[OKX][REST][retry %d/%d] %s — next in %.2fs",
                              attempt, self._rest_retries, e, delay + jitter)
                await asyncio.sleep(delay + jitter)
                delay *= 1.6
        if last_err:
            raise last_err
        return {}

    # ---------- miss 记忆 ----------
    def _is_missed(self, inst_id: str, kind: str) -> bool:
        return time.time() < self._miss_until.get((inst_id, kind), 0.0)

    def _mark_miss(self, inst_id: str, kind: str, ttl: float = 60.0) -> None:
        self._miss_until[(inst_id, kind)] = time.time() + max(1.0, float(ttl))

    # ---------- WS 登录：可选等待 ----------
    async def _ws_login(self, *, wait: bool = True, timeout: float = 5.0) -> None:
        """
        发送 WS 登录（op=login）。若 wait=True，则等待服务端返回 event=login, code=0。
        注：必须在读循环运行时才能收到并完成 waiter；否则不要阻塞等待。
        """
        # 只要配置里有密钥，就允许在 public/business 线路登录；已登录则返回
        if not self._has_creds or self._login_ok:
            return
        if self._login_waiter and not self._login_waiter.done():
            # 已在等待中
            if wait:
                await asyncio.wait_for(self._login_waiter, timeout=timeout)
            return
        # 构建登录参数并发送
        self._login_waiter = asyncio.get_running_loop().create_future()
        args = self._make_ws_login_args()
        await self._ws.send_json({"op": "login", "args": [args]})
        logging.info("[OKX] WS login sent")
        if wait:
            await asyncio.wait_for(self._login_waiter, timeout=timeout)

    def _make_ws_login_args(self) -> dict:
        """
        sign = Base64(HMAC_SHA256(secret, timestamp + 'GET' + '/users/self/verify'))
        见 OKX 文档 WebSocket Login。timestamp 用秒级字符串。"""
        ts = str(int(time.time()))
        msg = ts + "GET" + "/users/self/verify"
        digest = hmac.new(self._api_secret.encode("utf-8"), msg.encode("utf-8"), hashlib.sha256).digest()
        sign = base64.b64encode(digest).decode()
        return {
            "apiKey": self._api_key,
            "passphrase": self._passphrase,
            "timestamp": ts,
            "sign": sign,
        }
    # ======================= 新增：聚合/落库工具 =======================
    def _build_vp_steps(self) -> Dict[str, float]:
        """
        从 cfg.features.profile 读取 bin 步长，支持 steps_by_inst 覆盖。
        """
        pcfg = (self.cfg.get("features", {}) or {}).get("profile", {}) or {}
        default_step = float(pcfg.get("step", 1.0))
        steps_by_inst = dict(pcfg.get("steps_by_inst", {}) or {})
        out: Dict[str, float] = {}
        for inst in self.inst_ids:
            out[inst] = float(steps_by_inst.get(inst, default_step))
        return out

    def _bin_price(self, inst: str, px: float) -> float:
        step = max(1e-12, float(self._vp_steps.get(inst, 1.0)))
        # 采用 floor 到 bin 左边界，避免浮点误差，保留 8 位小数
        b = math.floor((px + 1e-12) / step) * step
        return float(round(b, 8))

    def _persist_trades_and_vp(self, inst_id: str, got: List[dict]) -> None:
        """
        后台线程执行：批量 upsert trades，并累计 VP(bin)。
        """
        try:
            if not got:
                return

            # 1) 原始成交
            if self._store_raw_trades:
                self._store.bulk_upsert_trades(inst_id, got)
            # 2) VP 聚合（买/卖）
            bins: Dict[float, List[float]] = {}
            for r in got:
                px = float(r["px"]); sz = float(r["sz"]); side = (r.get("side") or "").lower()
                b = self._bin_price(inst_id, px)
                if b not in bins:
                    bins[b] = [0.0, 0.0]
                if side == "buy":
                    bins[b][0] += sz
                elif side == "sell":
                    bins[b][1] += sz
            bin_map = {k: (v[0], v[1]) for k, v in bins.items()}
            self._store.bulk_vp_accumulate(inst_id, bin_map)
        except Exception as e:
            # 安全降级：DB 出错不影响主流程
            import logging
            logging.warning("[OKX][DB] persist_trades_and_vp error %s: %s", inst_id, e)

    async def _replay_aggregated_profile(self, inst_id: str, rows: List[dict]) -> None:
        """
        ⭐ 方案二：把历史成交按 (bin_px, time_slot) 聚合成极少量“合成成交”回放到 ProfileEngine，
        这样开机即可获得完整 72h Volume Profile，且不需要逐笔回放。
        规则：
          - bin_px = floor(px/step)*step（与实时一致）
          - slot = ts // vp_sample_ms（与 ProfileEngine.vp_sample_ms 对齐）
          - 对每个 (bin, slot)：base_sum = Σsz，vwap = Σ(px*sz)/Σsz
          - 生成一条合成成交：ts = 槽末时刻；px = vwap（位于该 bin 内），sz = base_sum
        """
        if not rows:
            return
        # 与 ProfileEngine 的 vp_sample_ms 对齐；若未配置则默认 60s
        vp_ms = int((((self.cfg.get("features", {}) or {}).get("profile", {}) or {}).get("vp_sample_ms", 60_000)))
        if vp_ms <= 0:
            vp_ms = 60_000
        # agg[(bin, slot)] = [buy_base, buy_quote, sell_base, sell_quote]
        agg: Dict[Tuple[float, int], List[float]] = {}
        for t in rows:
            try:
                ts = int(t["ts"]); px = float(t["px"]); sz = float(t["sz"])
                side = (t.get("side") or "").lower()
            except Exception:
                continue
            bin_px = self._bin_price(inst_id, px)
            slot = ts // vp_ms
            key = (bin_px, slot)
            if key not in agg:
                agg[key] = [0.0, 0.0, 0.0, 0.0]
            if side == "sell":
                agg[key][2] += sz
                agg[key][3] += (px * sz)
            else:
                agg[key][0] += sz
                agg[key][1] += (px * sz)
        # 从旧到新回放（先按时间槽，再按价阶）
        for (bin_px, slot) in sorted(agg.keys(), key=lambda k: (k[1], k[0])):
            buy_base, buy_quote, sell_base, sell_quote = agg[(bin_px, slot)]
            slot_end_ts = (slot + 1) * vp_ms - 1
            if buy_base > 0:
                buy_vwap = buy_quote / buy_base
                trade_b = {"ts": int(slot_end_ts), "px": float(buy_vwap), "sz": float(buy_base),
                           "side": "buy", "tradeId": ""}
                coro = self.on_trade(inst_id, trade_b)
                if asyncio.iscoroutine(coro):
                    await coro
            if sell_base > 0:
                sell_vwap = sell_quote / sell_base
                trade_s = {"ts": int(slot_end_ts), "px": float(sell_vwap), "sz": float(sell_base),
                           "side": "sell", "tradeId": ""}
                coro = self.on_trade(inst_id, trade_s)
                if asyncio.iscoroutine(coro):
                    await coro

    async def _decide_backfill_minutes(self, inst_id: str) -> int:
        """
        根据 DB 的 last_trade_ts 计算缺口分钟数；若无记录则退回默认 self._bootstrap_minutes。
        添加 5 分钟 overlap，避免边界漏数。
        """
        # 与回灌上界保持一致：优先使用启动时刻 t0 作为“now”
        ref_now_ms = int(self._startup_t0 or int(time.time() * 1000))
        now_ms = ref_now_ms
        try:
            last_ts = await asyncio.to_thread(self._store.get_last_trade_ts, inst_id)
            
        except Exception:
            last_ts = None
        if not last_ts:
            # ✅ 兜底：用窗口覆盖度估算“最近覆盖到的 end_ms”，避免每次都整窗回补
            now_ms = ref_now_ms
            win_start = now_ms - max(1, int(self._bootstrap_minutes)) * 60_000
            try:
                cov = await asyncio.to_thread(
                    self._store.get_covered_intervals, inst_id, int(win_start), int(now_ms), "trades"
                )
            except Exception:
                cov = []
            if cov:
                last_end = max(e for _s, e in cov)
                last_ts = int(last_end)
            else:
                return int(self._bootstrap_minutes)
        last_ts = min(int(last_ts), now_ms)
        gap_min = max(0, int(math.ceil((now_ms - int(last_ts)) / 60000.0)))
        # ✅ gap=0 时允许 minutes=0，由上面的“优化1”直接跳过
        minutes = min(max(0, gap_min + 5), int(self._bootstrap_minutes))
        # 直接复用 _bootstrap_* 的分钟参数
        return minutes

    async def _decide_full_remaining_minutes(self, inst_id: str) -> int:
        """
        full 模式下：目标是覆盖到 "now - bootstrap_minutes"。
        如果已持久了最早覆盖到 earliest_ms，则只需补 (earliest_ms - target_earliest) 这段。
        """
        ref_now_ms = int(self._startup_t0 or int(time.time() * 1000))
        target_earliest = ref_now_ms - max(1, int(self._bootstrap_minutes)) * 60_000
        try:
            earliest_ms = await asyncio.to_thread(self._store.get_backfill_earliest_ms, inst_id)
        except Exception:
            earliest_ms = None
        if not earliest_ms:
            return int(self._bootstrap_minutes)
        earliest_ms = int(earliest_ms)
        if earliest_ms <= target_earliest:
            return 0
        remaining = math.ceil((earliest_ms - target_earliest) / 60000.0)
        return max(0, min(int(self._bootstrap_minutes), int(remaining)))

    async def _fix_gaps_full_window(self, inst_id: str, upper_ts_exclusive: Optional[int] = None) -> None:
        """
        在 full 模式完成“扫到最早点”后，基于覆盖区间找出 [now-窗口, now] 内的缺口，
        并用 _bootstrap_trades(minutes=..., upper_ts_exclusive=...) 精确补齐。
        """
        now_ms = int(upper_ts_exclusive or (time.time() * 1000))
        target_start = now_ms - int(self._bootstrap_minutes) * 60_000
        # 把“显著缺口”的阈值设为 flush_span 的 80%，可按需调
        for min_gap_ms in (int(max(1, int(self._bootstrap_flush_minutes)) * 60_000 * 0.8), 60_000):
            attempt = 0
            while True:
                gaps = await asyncio.to_thread(
                    self._store.get_window_gaps, inst_id, int(target_start), int(now_ms), int(min_gap_ms), "trades"
                )
                if not gaps:
                    break
                g0, g1 = gaps[0]
                minutes = max(1, int(math.ceil((now_ms - int(g0)) / 60000.0)))  # minutes 只是上限，不影响 [g0,g1) 精确裁剪
                await self._bootstrap_trades(
                    inst_id,
                    minutes=minutes,
                    upper_ts_exclusive=int(g1),
                    lower_ts_inclusive=int(g0),
                )
                attempt += 1
                if attempt >= 50:
                    logging.warning("[OKX][GAPFIX] %s attempts cap reached for min_gap_ms=%d", inst_id, min_gap_ms)
                    break
    async def _subscribe_inst(self, inst: str):
        """只订阅单个 instId 的 books/trades/tickers。"""
        if not self._ws:
            return
        args = [
            {"channel": self.orderbook_channel, "instId": inst},
            {"channel": self.trades_channel, "instId": inst},
            {"channel": self.tickers_channel, "instId": inst},
        ]
        await self._ws.send_json({"op": "subscribe", "args": args})
        self._subscribed_insts.add(inst)
        logging.info("[OKX] subscribed(inst): %s (%s/%s/%s)", inst, self.orderbook_channel, self.trades_channel, self.tickers_channel)

    async def _subscribe_many(self, insts: list[str]):
        for inst in insts:
            await self._subscribe_inst(inst)


    async def _bootstrap_one_and_subscribe(self, inst: str, initial_minutes: int | None):
        """
        回补单个标的；完成后立刻订阅该标的 WS。
        full 模式：循环 _decide_full_remaining_minutes() 直到覆盖到目标最早点；gap 模式：按分钟一次性补齐。
        """
        try:
            if self._bootstrap_mode == "full":
                # 持续拉到覆盖到 [now - bootstrap_minutes, now]
                while True:
                    rem = await self._decide_full_remaining_minutes(inst)
                    if rem <= 0:
                        break
                    logging.info("[OKX][BOOTSTRAP][ONE] %s remaining=%d minutes ...", inst, rem)
                    await self._bootstrap_trades(
                        inst, minutes=rem, upper_ts_exclusive=self._startup_t0,
                        replay_to_handlers=True, aggregate_replay=True
                    )

                # 补缺口（更精细，按 DB 区间）
                try:
                    await self._fix_gaps_full_window(inst, upper_ts_exclusive=self._startup_t0)

                except Exception as e:
                    logging.warning("[OKX][GAPFIX] %s error: %s", inst, e)
                # 用 1m K 线同步 TPO（覆盖 bootstrap_minutes）
                await self._bootstrap_tpo(inst, minutes=self._bootstrap_minutes)
            else:
                m = int(initial_minutes or self._bootstrap_minutes)
                logging.info("[OKX][BOOTSTRAP][ONE] %s gap-mode minutes=%d ...", inst, m)
                await self._bootstrap_trades(
                    inst, minutes=m, upper_ts_exclusive=self._startup_t0,
                    replay_to_handlers=True, aggregate_replay=True
                )
                await self._bootstrap_tpo(inst, minutes=m)
        finally:
            # ⭐ 关键：完成后立刻订阅该标的
            try:
                await self._subscribe_inst(inst)
            except Exception as e:
                logging.warning("[OKX] subscribe after bootstrap failed %s: %s", inst, e)

    def _init_rate_limiters(self) -> None:
        """
        定义各端点的限流桶（按 OKX 官方配额）：
          - history-trades / trades-history: 20次/2秒 ≈ 10 rps（容量 20）
          - trades: 100次/2秒 ≈ 50 rps（容量 100）
          - history-candlesticks / history-mark-price-candles: 20次/2秒 ≈ 10 rps
          - books: 10 rps（默认；可配置）
          - 其它公共端点：默认 10 rps
        """
        self._rl: Dict[str, _RateLimiter] = {}
        # 允许通过 cfg.okx.rest_rate_limits 覆盖默认值
        rl_cfg = (self.cfg.get("okx", {}) or {}).get("rest_rate_limits", {}) or {}
        def add(key: str, default_rps: float, default_burst: int):
            cfg = rl_cfg.get(key, {}) or {}
            rps = float(cfg.get("rps", default_rps))
            burst = int(cfg.get("burst", default_burst))
            self._rl[key] = _RateLimiter(rps, burst)
        # 下调默认 RPS，避免 429（可被 cfg.okx.rest_rate_limits 覆盖）
        add("history-trades", 10, 20)
        add("trades", 30.0, 60)
        add("history-candles", 4.0, 8)
        add("candles", 8, 16)
        add("books", 6.0, 12)  # /market/books
        add("public", 6.0, 12)

    def _pick_rate_limiter(self, url: str) -> _RateLimiter:
        """
        根据 URL 路径挑选对应限流桶。
        """
        try:
            path = url.split("/api/v5/", 1)[-1]
        except Exception:
            path = url
        if "market/history-trades" in path or "market/trades-history" in path:
            return self._rl["history-trades"]
        if "market/trades" in path:
            return self._rl["trades"]
        if "history-candlesticks" in path or "history-mark-price-candles" in path:
            return self._rl["history-candles"]
        if "candlesticks" in path or "mark-price-candles" in path:
            return self._rl["candles"]
        if "market/books" in path:
            return self._rl.get("books", self._rl["public"])
        return self._rl["public"]

    async def _run_backfills_to_t0(self, minutes_by_inst: Optional[Dict[str, int]] = None):
        """
        在后台把各标的“差的分钟数”补到进程启动时刻 t0（upper_ts_exclusive=t0）。
        期间 WS 已经在工作，避免阻塞。
        """
        t0 = self._startup_t0 or int(time.time() * 1000)
        # 如未传入 minutes_map，则现场再估算一次
        if minutes_by_inst is None:
            minutes_by_inst = {}
            try:
                if self._bootstrap_mode == "full":
                    base = self._valid_insts or self.inst_ids
                    for inst in base:
                        minutes_by_inst[inst] = await self._decide_full_remaining_minutes(inst)
                else:
                    base = self._valid_insts or self.inst_ids
                    for inst in base:
                        minutes_by_inst[inst] = await self._decide_backfill_minutes(inst)
            except Exception:
                logging.warning("[OKX][BOOTSTRAP][to_t0] minutes_map error", exc_info=True)

        # 打印每个 inst 当前 DB 覆盖起止，便于核对
        for inst in (self._valid_insts or self.inst_ids):
            try:
                earliest = await asyncio.to_thread(self._store.get_backfill_earliest_ms, inst)
            except Exception:
                earliest = None
            try:
                latest = await asyncio.to_thread(self._store.get_last_trade_ts, inst)
            except Exception:
                latest = None
            logging.info("[OKX][BOOTSTRAP][to_t0] %s db_coverage earliest=%s latest=%s need_minutes=%s t0=%d",
                          inst, earliest, latest, minutes_by_inst.get(inst), t0)
 
        # 按批次限制“同时回补的 inst 数量”，默认=1（顺序回补，最稳妥）
        inst_items = [(inst, int(m)) for inst, m in (minutes_by_inst or {}).items() if int(m) > 0]
        if not inst_items:
            pass
        elif self._backfill_parallel_insts <= 1:
            for inst, m in inst_items:
                # 严格串行；历史成交按聚合方式回放，确保 ProfileEngine 一启动就具备窗口内体量
                await self._bootstrap_trades(
                    inst,
                    minutes=m,
                    upper_ts_exclusive=int(t0),
                    replay_to_handlers=True, aggregate_replay=True
                )
                await self._bootstrap_tpo(inst, minutes=min(m, self._bootstrap_minutes))
                if self._page_pause_s > 0:
                    await asyncio.sleep(self._page_pause_s)
        else:
            for i in range(0, len(inst_items), self._backfill_parallel_insts):
                batch = inst_items[i:i + self._backfill_parallel_insts]
                tasks: List[asyncio.Task] = []
                for inst, m in batch:
                    tasks.append(asyncio.create_task(
                        self._bootstrap_trades(inst, minutes=m, upper_ts_exclusive=int(t0),
                                               replay_to_handlers=True, aggregate_replay=True)
                    ))
                    tasks.append(asyncio.create_task(
                        self._bootstrap_tpo(inst, minutes=min(m, self._bootstrap_minutes))
                    ))
                await asyncio.gather(*tasks, return_exceptions=True)
                if self._page_pause_s > 0:
                    await asyncio.sleep(self._page_pause_s)
        # full 模式再扫一次窗口内缺口（上界卡在 t0），避免“to_t0”并行回灌后仍有小洞
        if self._bootstrap_mode == "full":
            for inst in (self._valid_insts or self.inst_ids):
                try:
                    await self._fix_gaps_full_window(inst, upper_ts_exclusive=int(t0))
                except Exception as e:
                    logging.warning("[OKX][GAPFIX][to_t0] %s: %s", inst, e)
        self._bootstrapped = True
        logging.info("[OKX][BOOTSTRAP][to_t0] all backfills done (upper<t0); realtime WS kept running.")

class _RateLimiter:
    """
    极简令牌桶（异步安全）：
      - rate_per_sec: 令牌生成速率（每秒）
      - capacity: 桶容量（允许短暂突发）
    所有需要限流的协程在发请求前 await acquire() 即可。
    """
    def __init__(self, rate_per_sec: float, capacity: int):
        self.rate = max(1e-6, float(rate_per_sec))
        self.capacity = max(1, int(capacity))
        self.tokens = float(self.capacity)
        self.updated = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self, n: float = 1.0):
        n = max(0.0, float(n))
        while True:
            async with self._lock:
                now = time.monotonic()
                elapsed = now - self.updated
                if elapsed > 0:
                    self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
                    self.updated = now
                if self.tokens >= n:
                    self.tokens -= n
                    return
                needed = n - self.tokens
                wait = needed / self.rate if self.rate > 0 else 0.05
            await asyncio.sleep(min(0.25, max(0.001, wait)))

    # ======================= 新增：SWAP 清单与过滤 =======================
    async def _load_okx_swaps(self) -> set[str]:
        """
        读取 OKX 全量 SWAP instId 清单并缓存 10 分钟。
        失败时返回已有缓存（可能为空）。
        """
        now = time.time()
        if self._okx_swaps and (now - self._okx_swaps_loaded_at < 600.0):
            return self._okx_swaps
        url = f"{self.rest_base_url}/api/v5/public/instruments"
        params = {"instType": "SWAP"}
        try:
            data = await self._rest_get(url, params)
            rows = (data or {}).get("data") or []
            swaps = {str(r.get("instId")) for r in rows if r.get("instId")}
            if swaps:
                self._okx_swaps = swaps
                self._okx_swaps_loaded_at = now
                logging.info("[OKX][INSTR] loaded %d SWAP instIds", len(swaps))
        except Exception as e:
            logging.warning("[OKX][INSTR] load swaps error: %s", e)
        return self._okx_swaps

    async def _filter_configured_insts(self, insts: list[str]) -> list[str]:
        """
        仅保留 OKX instruments(SWAP) 中存在的 instId。
        对不存在者写入负缓存 (inst_resolve, ttl=1800s) 并跳过后续 WS/REST。
        """
        swaps = await self._load_okx_swaps()
        if not swaps:
            # 取不到清单就不动原配置，避免误杀；后续仍有 WS error/fallback 兜底
            return insts
        keep: list[str] = []
        for inst in insts:
            if inst in swaps:
                keep.append(inst)
            else:
                # 负缓存，供消费侧也可读取相同 key 语义（inst_resolve）
                self._miss_until[(inst, "inst_resolve")] = time.time() + self._inst_not_found_ttl
                logging.info("[OKX][INSTR] skip %s (no SWAP on OKX) — muted %ds",
                             inst, int(self._inst_not_found_ttl))
        return keep

    # —— 新增：通知消费端“该 inst 未监控，可回退 ATR” ——
    async def _notify_unmonitored(self, inst: str, reason: str, once: bool = True):
        """
        统一发出“未监控”信号：消费端据此切换到 ATR 动态利润/亏损保护。
        reason: "no_swap_on_okx" / f"ws_subscribe_error:{code}" 等
        """
        try:
            now = time.time()
            if once and (now - self._unmonitored_emit_ts.get(inst, 0.0) < self._unmonitored_emit_ttl):
                return
            # 新统一事件：MONITOR_STATUS（反向同通道更好做路由）
            await self._emit_status({
                "event": "MONITOR_STATUS",
                "instId": inst,
                "coin": self._inst_coin(inst),
                "monitored": False,
                "reason": reason,
                "ts": int(now * 1000)
            })
            self._unmonitored_emit_ts[inst] = now
            try:
                self._monitored_sent.discard(inst)
            except Exception:
                pass
        except Exception:
            pass
    # —— 新增：从 instId 提取 coin（如 BTC-USDT-SWAP → BTC） ——
    @staticmethod
    def _inst_coin(inst: str) -> str:
        try:
            return str(inst).split("-", 1)[0].upper()
        except Exception:
            return ""