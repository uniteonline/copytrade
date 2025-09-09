"""
业务层：监控 assetPositions 变化
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
"""
import yaml, logging, copy, time, json, tempfile
from pathlib import Path
import asyncio, aio_pika
from ws_manager import HLWebSocketManager
from mq_producer import MQProducer
from sqlite_manager import SQLiteManager
import threading, signal
from telegram_ops import TelegramPusher
import re
# ---------------- Snapshot → Markdown  ---------------- #
# 必须转义 MarkdownV2 的 _ * [ ] ( ) ~ ` > # + - = | { } . !
_MD_ESC = r'([_*[\]()~`>#+\-=|{}.!])'      # Bot API 文档列出的 16 个字符
ESC_RE  = re.compile(_MD_ESC)

# 支持 str / int / float，避免忘记调用 str()
def _esc(txt) -> str:
    return ESC_RE.sub(r"\\\1", str(txt))

# ---------- 事件 → Emoji 映射 ---------- #
EVENT_ICON = {
    "OPEN":    "✅",
    "CLOSE":   "❌",
    "INC":     "➕",
    "DEC":     "➖",
    "REVERSE": "🔄",
}

ticker: "SnapshotTicker|None" = None  # 全局句柄，供回调里触发 ready()
class SnapshotTicker(threading.Thread):
    def __init__(self, interval_sec: int = 3600):
        super().__init__(daemon=True)
        self.interval = interval_sec
        self._stop    = threading.Event()
        # ⬇ 首包准备完毕后由 on_position_update 触发
        self._ready   = threading.Event()

    def run(self):
        # ① 等仓位快照就绪（ready_evt.set()）——每 0.5 s 轮询，可被 Ctrl-C 打断
        while not self._stop.is_set() and not self._ready.wait(0.5):
            pass

        if self._stop.is_set():        # 已退出
            return

        # ② 首帧就绪 → 立即推送一次快照
        self._push_snapshot()

        # ③ 之后严格每 interval 推送
        while not self._stop.wait(self.interval):
            self._push_snapshot()

    def _push_snapshot(self):
        txt = build_full_snapshot()
        if TG and TGCFG.get("chats"):
            for alias in TGCFG.get("chats", []):
                for chunk in _chunks(txt):
                    TG.send_message(alias, chunk)
    def stop(self):
        self._ready.set()
        self._stop.set()
    # 供外部调用：标记首帧已收到
    def ready(self):
        self._ready.set()

def _chunks(text: str, cap: int = 4096):
    """
    Telegram 的 sendMessage 文本限制是“解析实体后的 1–4096 个*字符*”；这里按“字符”切分，
    并尽量在换行处分包，避免把 emoji/多字节字符切半。
    """
    buf: list[str] = []
    size = 0
    for line in text.splitlines(keepends=True):
        ln = len(line)                         # 以“字符”为单位
        if ln > cap:                           # 行本身超长 – 逐段硬切（字符安全）
            for i in range(0, ln, cap):
                yield line[i:i + cap]
            continue
        if size + ln > cap and buf:
            yield "".join(buf)
            buf, size = [], 0
        buf.append(line)
        size += ln
    if buf:
        yield "".join(buf)

def build_full_snapshot() -> str:
    """
    汇总所有地址的最新持仓 → MarkdownV2 文本。
    """
    parts = []
    for addr, alias in ALIAS_BY_ADDR.items():
        snap = _snapshots.get(addr, {})
        pos  = snap.get("positions", [])
        if not pos:
            parts.append(f"*{_esc(alias)}* `{addr[:6]}…`: _no position_")
            continue
        rows = []
        for p in pos:
            ps  = p["position"]
            sz  = float(ps["szi"])
            if abs(sz) < EPS:
                continue
            coin  = _esc(ps["coin"])
            side  = _esc("LONG" if sz > 0 else "SHORT")
            entry    = ps.get("entryPx") or "-"
            lev_val  = ps.get("leverage", {}).get("value")  # ← 取 cross/isolated 数字
            lev_disp = lev_val if lev_val is not None else "-"
            # ① 取实时 mid 价，可为空
            sym = ps["coin"].upper()
            mid = _mids.get(sym)
            if mid is None:
                logging.info("mid missing when building snapshot: %s "
                              "(_mids size=%d)", sym, len(_mids))
            mid_disp = f"`{_esc(f'{mid:.6g}')}`" if mid is not None else "_N/A_"

            rows.append(
                f"{coin} {side} 📏 `{_esc(sz)}`  🎯 `{_esc(entry)}` "
                f"lev `{_esc(lev_disp)}x`  💰 {mid_disp}"
            )
        parts.append(f"*{_esc(alias)}* `{_esc(addr)}…`\n" + "\n".join(rows))
    return "\n\n".join(parts) or "_snapshot not ready yet_"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

with open("config.yaml", encoding="utf-8") as f:
    cfg = yaml.safe_load(f)
    # 是否在“首帧 snapshot 合并完成”时把现有仓位作为 OPEN 立即推送到 queue_ops
    # 为避免和 snapshot_server 回放重复，默认关闭
    EMIT_INIT_OPEN = bool(cfg.get("emit_init_open", False))
    # ---------------- Wallet/Chat 映射（带校验） ---------------- #
    _alias_addr: dict[str, str] = {}
    for item in cfg["users"]:
        for raw_alias, raw_addr in item.items():
            alias = raw_alias.strip()                 # 去掉隐藏空格
            addr  = raw_addr.strip().lower()

            # ① 别名重复？
            if alias in _alias_addr:
                raise ValueError(f"Duplicate alias “{alias}” in config.yaml")

            # ② 地址重复？（-> 谁晚写谁错）
            if addr in _alias_addr.values():
                old_alias = [k for k, v in _alias_addr.items() if v == addr][0]
                raise ValueError(
                    f"Address {addr} already bound to alias “{old_alias}”, "
                    f"but appears again for “{alias}”"
                )

            _alias_addr[alias] = addr

    ALIAS_BY_ADDR = {addr: alias for alias, addr in _alias_addr.items()}

    logging.info("🗺️ Alias → Address map loaded: %s", _alias_addr)

    ADDR_LIST = list(ALIAS_BY_ADDR)           # 仅地址列表，供 WS 订阅
    MQCFG = cfg["rabbitmq"]          # 约定 config.yaml 里已有 rabbitmq 节点
    TGCFG = cfg.get("telegram", {})  # ← NEW

    MQ_WATCH_ALIAS = set(a.strip() for a in cfg.get("mq_watch", []) or [])

    # 校验 —— 列表里的别名必须出现在 users 区域
    invalid = MQ_WATCH_ALIAS - set(_alias_addr)
    if invalid:
        raise ValueError(f"mq_watch contains undefined alias: {', '.join(invalid)}")

    # 转成地址集合，后面做 O(1) 判断
    MQ_ADDRS = ({ _alias_addr[alias].lower() for alias in MQ_WATCH_ALIAS }
                or { addr.lower() for addr in _alias_addr.values() })

# -------------------------------------------------
# Telegram —— 单例推送器（缺配置则置空，调用处判空）
# -------------------------------------------------
_tg_token = TGCFG.get("token")
_tg_chats = TGCFG.get("chats") or []
TG = TelegramPusher(_tg_token, _tg_chats) if (_tg_token and _tg_chats) else None

def tg_push(addr: str | None, text: str):
    """
    **广播版**  
    不再根据钱包地址筛选，只要在 ``telegram.chats`` 里配置的用户都会收到增量消息。

    *addr* 参数现在仅用于外部生成消息内容（例如 alias 展示），
    对发送目标不再起过滤作用。
    """
    if not (TG and TGCFG.get("chats")):
        return                               # 未配置任何 chat – 直接忽略
    for alias in TGCFG["chats"]:
        TG.send_message(alias, text)         # 广播到每个 chat_id

# -------------------------------------------------
# RabbitMQ —— 线程局部 Producer（防止并发误用）
# -------------------------------------------------
QUEUE_OPS  = MQCFG.get("queue_ops",  "queue_ops")
QUEUE_INFO = MQCFG.get("queue_info", "queue_info")
QUEUE_SNAP_REQ = MQCFG.get("queue_snap_req", "queue_snap_req")

_tls = threading.local()

def get_producer(queue_name: str) -> MQProducer:
    """
    同一线程内复用一条连接；不同线程各拿各的。
    """
    attr = f"_prod_{queue_name}"
    producer = getattr(_tls, attr, None)
    if producer is None:
        producer = MQProducer(MQCFG["url"], queue_name)
        setattr(_tls, attr, producer)
    return producer
# ---------------- 新增：净头寸持久化 ---------------- #
STATE_FILE = Path("net_state.json")
EPS = 1e-8
# ────────────────────────────────────────────────────────────
#  全市场 mid 价缓存
# ────────────────────────────────────────────────────────────
_mids: dict[str, float] = {}          # symbol → price
_last_info_ts: dict[tuple[str, str], float] = {}  # (user, COIN) → last ts
_last_info_px_sent: dict[tuple[str, str], float] = {}  # (user, COIN) → last midPx sent
INFO_INTERVAL = float(cfg.get("info_interval_sec", 2.0))  # 每个币/地址最小 INFO 周期(秒)
# 新增：只有价格相对上次「已发送」INFO 超过该百分比才推（0=关闭）
INFO_PX_MIN_MOVE_PCT = float(cfg.get("info_px_min_move_pct", 0.0))
def on_mid_update(mids: dict[str, str]) -> None:
    """
    收到 allMids 报文时更新全局报价表。
    形如 '@123' 的内部合约 id 忽略，只记录诸如 'ETH' / 'SOL' 这类符号。
    """
    global _mids                              # ◀️ 显式声明（可选但推荐）
    for sym, raw in mids.items():
        if sym.startswith("@"):          # 跳过内部 id
            continue
        try:                               # 空串 / None 防护
            _mids[sym.upper()] = float(raw) if raw else None
        except (ValueError, TypeError):
            continue

    if not _mid_ready_evt.is_set():
        _mid_ready_evt.set()
        if (ticker
            and not ticker._ready.is_set()
            and all(addr in _first_done for addr in ADDR_LIST)):
            ticker.ready()
    # ▶ 新增：对所有有持仓的 (user, coin) 按节流推送 INFO（带 midPx/snapshot/side）
    now = time.time()
    for user, snap in _snapshots.items():
        poss = snap.get("positions", []) or []
        for p in poss:
            pos = p.get("position") or {}
            coin = pos.get("coin")
            if not coin:
                continue
            try:
                szi = float(pos.get("szi") or 0)
            except Exception:
                continue
            if abs(szi) < EPS:
                continue  # 只对有持仓的币推 INFO
            key = (user.lower(), coin.upper())
            if now - _last_info_ts.get(key, 0.0) < INFO_INTERVAL:
                continue
            mid_now = _mids.get(coin.upper())
            if mid_now is None:
                continue
            # 价格最小变动门槛（相对上次 *已发送* 的 mid）
            if INFO_PX_MIN_MOVE_PCT > 0:
                last_sent = _last_info_px_sent.get(key)
                if last_sent:
                    rel = abs(mid_now - last_sent) / max(1e-12, abs(last_sent)) * 100.0
                    if rel < INFO_PX_MIN_MOVE_PCT:
                        continue
            _last_info_px_sent[key] = mid_now
            _last_info_ts[key] = now
            # 归一杠杆为数值
            lv_raw = (pos.get("leverage") or {}).get("value")
            try:
                lev_num = float(lv_raw) if lv_raw is not None else None
            except Exception:
                lev_num = None
            side = "LONG" if szi > 0 else "SHORT"
            msg_info = {
                "msg_id": f"{user}.{coin}.{int(now*1_000_000)}",
                "ts":     int(now * 1e3),
                "user":   user,
                "wallet": user,
                "coin":   coin,
                "side":   side,          # ◀ 必带
                "midPx":  mid_now,       # ◀ 必带
                "etype":  "INFO",
                "changed": ["midPx"],    # 表示这是一条价格心跳
                "lev":    lev_num,
                "source": "HL",
                "schema": 2,
                "snapshot": {            # ◀ 必带（消费端用来算 ROE）
                    "szi":            szi,
                    "entryPx":        pos.get("entryPx"),
                    "unrealizedPnl":  pos.get("unrealizedPnl"),
                    "returnOnEquity": pos.get("returnOnEquity"),
                    "liquidationPx":  pos.get("liquidationPx"),
                    "marginUsed":     pos.get("marginUsed"),
                    "lev":            lev_num,
                    "leverage":       lev_num
                }
            }
            if user.lower() in MQ_ADDRS:
                try:
                    get_producer(QUEUE_INFO).publish(msg_info)
                except Exception as e:
                    logging.error("MQ publish mid INFO failed: %s", e, exc_info=True)

def _load_net() -> dict[str, dict[str, float]]:
    try:
        return json.loads(STATE_FILE.read_text())
    except Exception:
        return {}

def _save_net(net: dict):
    """原子写：先写 tmp 再替换"""
    try:
        with tempfile.NamedTemporaryFile("w", delete=False, dir=".") as fp:
            json.dump(net, fp)
        Path(fp.name).replace(STATE_FILE)
    except Exception as e:
        logging.error("Save net_state fail: %s", e)

_net: dict[str, dict[str, float]] = _load_net()      # user -> {coin: netSz}
# -------------------------------------------------
#  记录每个地址是否已收到“首帧快照”
# -------------------------------------------------
_snapshots: dict[str, dict] = {}
_first_done: set[str] = set()           # 每个地址首帧到达
_mid_ready_evt = threading.Event()      # ← 新增：全市场 mid 表就绪

db = SQLiteManager()  
# ------------------------------------------------------------------
# ① 需要比对的字段
# ------------------------------------------------------------------
#    • 持仓级 (Position-level)
#    • 市场环境级 (assetCtxs / funding 等)
# ------------------------------------------------------------------
POSITION_FIELDS = (
    "szi",             # 仓位大小
    "entryPx",         # 入场价
    "unrealizedPnl",   # 浮盈浮亏
    "returnOnEquity",  # ROE
    "liquidationPx",   # 强平价
    "marginUsed",      # 保证金占用
)

ENV_FIELDS = (
    "funding",         # 资金费率
    "openInterest",    # 未平仓量
    "premium",         # 基差 / 溢价
)
# ------------------------------------------------------------------
# ② 不想打印但仍需监控的字段
# ------------------------------------------------------------------
SUPPRESS_LOG_FIELDS = {
    "unrealizedPnl",
    "liquidationPx",
    "marginUsed",
    "returnOnEquity",
    "funding",
    "openInterest",
    "premium",
}
def _side(pos: dict | None) -> str | None:
    if pos is None:
        return None
    return "Short" if float(pos["szi"]) < 0 else "Long"
def on_position_update(user: str, payload: dict):
    # ----------------------------------------------------------------
    # 0️⃣ 只要该 user 尚未缓存快照，就把收到的第一包视为快照
    #    （Hyperliquid 协议新版本已取消 isSnapshot 字段）
    # ----------------------------------------------------------------
    is_snapshot = payload.get("isSnapshot") or (user not in _snapshots)
    if is_snapshot:
        # -------------------------------------------------------------
        # ① 快照包也可能被分片多次推送 → 必须 **合并** 而不是整包覆盖
        # -------------------------------------------------------------
        snap = _snapshots.setdefault(user, {"positions": [], "assetCtxs": []})
        # -------------------------------------------------------------
        # 🌟 新增：首帧 snapshot 打印 —— 逐 coin 显示当前仓位
        # -------------------------------------------------------------
        for p in payload["clearinghouseState"]["assetPositions"]:
            pos   = p["position"]
            coin  = pos["coin"]
            szi   = float(pos["szi"])
            # 解析杠杆为数值(或 None)，并准备显示占位
            lv_raw = pos.get("leverage", {}).get("value")
            try:
                lev_num = float(lv_raw) if lv_raw is not None else None
            except Exception:
                lev_num = None
            lev_disp = lev_num if lev_num is not None else "-"
            side = "Long" if szi > 0 else ("Short" if szi < 0 else "-")
            entry_px = pos.get("entryPx") or "-"
            logging.info("[SNAP] %s %-4s %-5s sz=%s px=%s lev=%s",
                         user, coin, side, szi, entry_px, lev_disp)
        # positions merge
        pos_by_coin = {p["position"]["coin"]: p for p in snap["positions"]}
        # ② 推送 OPEN 事件（只对第一次出现/新增 coin）
        for p in payload["clearinghouseState"]["assetPositions"]:
            pos_by_coin[p["position"]["coin"]] = copy.deepcopy(p)
        snap["positions"] = list(pos_by_coin.values())

        # assetCtxs merge / replace（同增量逻辑）
        new_ctxs = payload.get("assetCtxs", [])
        if new_ctxs:
            if all(isinstance(c, dict) and "coin" in c for c in new_ctxs):
                ctx_map = {
                    c["coin"]: c
                    for c in snap["assetCtxs"]
                    if isinstance(c, dict) and "coin" in c
                }
                for c in new_ctxs:
                    ctx_map[c["coin"]] = copy.deepcopy(c)
                snap["assetCtxs"] = list(ctx_map.values())
            else:
                snap["assetCtxs"] = copy.deepcopy(new_ctxs)
        # ---------- 把首帧持仓同步到 _net，防止重启后误判 ---------- #
        if user not in _net:
            _net[user] = {}
        # ✨ 预先计算整包快照的总仓位（USDT）
        _total_usdt_snapshot = _portfolio_total_usdt(payload["clearinghouseState"]["assetPositions"])

        for p in payload["clearinghouseState"]["assetPositions"]:
            pos  = p["position"]
            coin = pos["coin"]
            szi  = float(pos["szi"])
            if abs(szi) >= EPS:
                _net[user][coin] = szi
                # （默认）不再在首帧把现存仓位当 OPEN 推送，避免和 snapshot_server 回放重复
                if EMIT_INIT_OPEN:
                    side = "LONG" if szi > 0 else "SHORT"
                    mid_now = _mids.get(coin.upper())
                    lv_raw = pos.get("leverage", {}).get("value")
                    try:
                        lev_num = float(lv_raw) if lv_raw is not None else None
                    except Exception:
                        lev_num = None
                    msg = {
                        "msg_id": f"{user}.{coin}.{int(time.time()*1_000_000)}",
                        "ts": int(time.time()*1e3),
                        "user":   user,
                        "wallet": payload.get("walletAddr"),
                        "coin":   coin,
                        "side":   side,
                        "midPx":  mid_now,
                        "etype":  "SIZE",
                        "event":  "OPEN",
                        "origin": "init_snapshot",
                        "changed": ["szi"],
                        "lev":    lev_num,
                        "portfolio_total_usdt": _total_usdt_snapshot,
                        "source": "HL",
                        "schema": 2,
                        "snapshot": {
                            "szi":            szi,
                            "entryPx":        pos.get("entryPx"),
                            "unrealizedPnl":  pos.get("unrealizedPnl"),
                            "returnOnEquity": pos.get("returnOnEquity"),
                            "liquidationPx":  pos.get("liquidationPx"),
                            "marginUsed":     pos.get("marginUsed"),
                            "lev":            lev_num,
                            "leverage":       lev_num
                        }
                    }
                    if user.lower() in MQ_ADDRS:
                        try:
                            get_producer(QUEUE_OPS).publish(msg)
                        except Exception as e:
                            logging.error("MQ publish OPEN-snapshot failed: %s", e, exc_info=True)
                else:
                    logging.info("[SNAP] init OPEN publish disabled (emit_init_open=False)")

        _save_net(_net)
        # ← 通知 SnapshotTicker 已拿到首帧
        _first_done.add(user.lower())          # 标记已完成首帧


        # ⭐ 只有全部地址都到齐 且 mids 已经准备好 才启动 SnapshotTicker
        if (ticker
            and not ticker._ready.is_set()
            and all(addr in _first_done for addr in ADDR_LIST)
            and _mid_ready_evt.is_set()):
            ticker.ready()
        # ---------- NEW: Telegram 推送首帧持仓（可多地址合并） ---------- #
        addr = (payload.get("walletAddr") or user).lower()
        alias = ALIAS_BY_ADDR.get(addr, addr[:6])   # fallback 用前 6 位
        return
    new_pos  = payload["clearinghouseState"]["assetPositions"]
    new_env  = payload.get("assetCtxs", [])

    old_snap = _snapshots.get(user, {})
    old_pos  = old_snap.get("positions", [])
    old_env  = old_snap.get("assetCtxs", [])

    # -------- util -------- #
    def pos_map(lst): return {p["position"]["coin"]: p["position"] for p in lst}
    def env_map(ctxs: list, pos_ref: list):
        """
        构造 {coin: ctx} 映射
            • 若 ctx 自带 coin 字段：直接用
            • 否则按序号与持仓列表对齐
        """
        if not ctxs:
            return {}
        # ① ctx 本身有 coin
        if all(isinstance(c, dict) and "coin" in c for c in ctxs):
            return {c["coin"]: c for c in ctxs}

        # ② 无 coin 字段 → 退化到“位置顺序”映射
        mapping = {}
        for p, c in zip(pos_ref, ctxs):           # len 不一致时多余元素自动丢弃
            try:
                coin = p["position"]["coin"]
                mapping[coin] = c
            except Exception:
                continue
        return mapping

    def _val(d: dict|None, path: str):
        """安全取深层字段, 不存在返回 None"""
        if d is None: return None
        for k in path.split("."):
            d = d.get(k) if isinstance(d, dict) else None
        return d
    # ---------- 新增：统一取 old / new 值的辅助 ----------
    def _pick(old: dict|None, new: dict|None, path: str, default="-"):
        """
        返回 (old_val, new_val) 元组。
        open/close 场景下其中一侧为 None，避免打印/落库 None。
        """
        return _val(old, path) or default, _val(new, path) or default
    # 当 np 为 None（close 场景）且 op.szi 非 0 时，把 snapshot.szi 显式写成 0
    def _szi_for_snapshot(op: dict|None, np: dict|None):
        try:
            if np is not None:
                v = np.get("szi")
                return float(v) if v is not None else None
            if op is not None and abs(float(op.get("szi") or 0)) > EPS:
                return 0.0
        except Exception:
            pass
        return _val(np, "szi")
    new_p, old_p = map(pos_map, (new_pos, old_pos))
    new_e = env_map(new_env, new_pos)
    old_e = env_map(old_env, old_pos)

    # 并集 → 发现新/删仓
    coins = set(new_p) | set(old_p) | set(new_e) | set(old_e)

    for coin in coins:
        np, op = new_p.get(coin), old_p.get(coin)
        ne, oe = new_e.get(coin), old_e.get(coin)

        # 用 set 去重，后面再转 list 排好序
        changed: set[str] = set()

        # --- 持仓开/平 --- #
        if np is None or op is None:
            changed.update({"open/close", "szi"})
        else:
            for f in POSITION_FIELDS:
                if str(_val(np, f)) != str(_val(op, f)):
                    changed.add(f)
            # 杠杆字段要单独比较
            if _val(np, "leverage.value") != _val(op, "leverage.value"):
                changed.add("leverage")
        # --- 资金面字段 --- #
        for f in ENV_FIELDS:
            if str(_val(ne, f)) != str(_val(oe, f)):
                changed.add(f)


        if not changed:
            continue

        # ---- 统一处理排序 & 落库判断 ----
        changed_list = sorted(changed)           # set → list，保证日志、SQL 顺序稳定
        write_db    = any(f not in SUPPRESS_LOG_FIELDS for f in changed_list)

        # 过滤掉不打印的字段；若仅出现静默字段且又不用写库 → 直接下一循环
        visible = [f for f in changed_list if f not in SUPPRESS_LOG_FIELDS]

        side_new = _side(np)
        side_old = _side(op)
        # 取当前有效的杠杆数值（优先 new，其次 old），并归一成数字或 None
        lev_now = _val(np, "leverage.value") or _val(op, "leverage.value")
        try:
            lev_now = float(lev_now) if lev_now is not None else None
        except Exception:
            lev_now = None
        if visible: 
            # ---------- 取值时自动处理 open/close ----------
            old_sz, new_sz   = _pick(op, np, "szi", 0)
            old_px, new_px   = _pick(op, np, "entryPx")
            old_lev, new_lev = _pick(op, np, "leverage.value")
            mid_now          = _mids.get(coin.upper())
            logging.info(
                # user   coin  side  变化字段   size(old→new)  px(old→new)  lev(old→new) funding
                "[%s] %-4s %-5s Δ=%s | sz=%s→%s px=%s→%s lev=%s→%s mid=%s",
                user, coin, side_new or side_old or "-",
                ",".join(visible),
                old_sz, new_sz,
                old_px, new_px,
                old_lev, new_lev,
                f"{mid_now:.6g}" if mid_now is not None else "-",
            )
        if write_db:
            db.insert_change({
                # 必备列
                "ts": int(time.time()*1e3),
                "user": user,
                "wallet": payload.get("walletAddr"),   # <-- 新增
                "coin": coin,
                "side": side_new or side_old or "-",
                "changed": ",".join(changed_list),
                # 通用字段（不管是否 suppressed，都完整保存）
                "szi":            _szi_for_snapshot(op, np),
                "entryPx":        _val(np, "entryPx"),
                "liquidationPx":  _val(np, "liquidationPx"),
                "unrealizedPnl":  _val(np, "unrealizedPnl"),
                "returnOnEquity": _val(np, "returnOnEquity"),
                "marginUsed":     _val(np, "marginUsed"),
                "funding":        _val(ne, "funding"),
                "openInterest":   _val(ne, "openInterest"),
                "premium":        _val(ne, "premium"),
            })

        # ------------------------------------------------------------------
        # ❶ 根据变化内容分流：szi → 操作队列；其它 → 信息队列
        # ------------------------------------------------------------------
        mid_now = _mids.get(coin.upper())
        # 先准备公共字段
        msg_common = {
                "msg_id": f"{user}.{coin}.{int(time.time()*1_000_000)}",   # ← 新增
                "ts": int(time.time() * 1e3),
                "user":   user,
                "wallet": payload.get("walletAddr"),
                "coin":   coin,
                "side":   side_new or side_old or "-",
                "midPx":  mid_now,
                "changed": changed_list,      # **包含 SUPPRESS_LOG_FIELDS**
                "lev":    lev_now,          # 顶层带一份
                "source": "HL",
                "schema": 2,
                "snapshot": {                 # 全量字段快照
                    "szi":            _szi_for_snapshot(op, np),
                    "entryPx":        _val(np, "entryPx") or _val(op, "entryPx"),
                    "unrealizedPnl":  _val(np, "unrealizedPnl"),
                    "returnOnEquity": _val(np, "returnOnEquity"),
                    "liquidationPx":  _val(np, "liquidationPx"),
                    "marginUsed":     _val(np, "marginUsed"),
                    "funding":        _val(ne, "funding"),
                    "openInterest":   _val(ne, "openInterest"),
                    "premium":        _val(ne, "premium"),
                    "lev":            lev_now,     # 扁平化数值
                    "leverage":       lev_now
                }}

        # ——— 分类逻辑：只判断 szi 变化 ——— #
        # ⭐ 按 szi 变化计算事件类型，默认 FALLBACK = ""
        ev: str = ""
        # open/close 也要触发事件判定
        if "szi" in changed_list or "open/close" in changed_list:
            old_sz = float(_val(op, "szi") or 0)
            new_sz = float(_val(np, "szi") or 0)
            if abs(old_sz) < EPS and abs(new_sz) >= EPS:
                ev = "OPEN"
            elif abs(old_sz) >= EPS and abs(new_sz) < EPS:
                # 改法一：仓位从非零变为零，统一视为“减仓”事件
                ev = "DEC"
            elif old_sz * new_sz > 0:
                ev = "INC" if abs(new_sz) > abs(old_sz) else "DEC"
            else:
                ev = "REVERSE"

            # 为本次变更对应的新快照计算最新总仓位（USDT）
            # ⚠️ 兼容上游 assetPositions 可能是“增量列表”的情况：
            #    用 old_pos 与 new_pos 的合并视图计算，避免低估总额
            try:
                # old_pos/new_pos 形如 [{'position': {...}}, ...]
                merged = {p["position"]["coin"]: copy.deepcopy(p) for p in old_pos}
                for p in new_pos:
                    merged[p["position"]["coin"]] = p  # 新值覆盖旧值
                # _portfolio_total_usdt 接受 position 包装后的列表
                _total_usdt_now = _portfolio_total_usdt(list(merged.values()))
            except Exception:
                _total_usdt_now = None
            msg_common.update({
                "etype": "SIZE",
                "event": ev,
                # 🌟 新增：变更后的总仓位（USDT名义）
                "portfolio_total_usdt": _total_usdt_now
            })
            if user.lower() in MQ_ADDRS:               # ← 修正变量名
                try:
                    get_producer(QUEUE_OPS).publish(msg_common)
                except Exception as e:
                    logging.error("MQ publish OPS failed: %s", e, exc_info=True)
        # ——— ② 只要还有其它字段变化 ⇒ 再发 INFO ——— #
        non_size_changes = [f for f in changed_list if f != "szi"]
        if non_size_changes and user.lower() in MQ_ADDRS:
            msg_info = copy.deepcopy(msg_common)
            msg_info.update({
                "etype": "INFO",
                "changed": non_size_changes
            })
            try:
                get_producer(QUEUE_INFO).publish(msg_info)
            except Exception as e:
                logging.error("MQ publish INFO failed: %s", e, exc_info=True)
        # ---------- NEW: Telegram 推送增量变化 ---------- #
        if any(f not in SUPPRESS_LOG_FIELDS for f in changed_list):
            # payload 里可能没有 walletAddr（webData2 报文常见）
            # 退化到 user 本身，避免 addr == "" 而无法推送
            addr   = (payload.get("walletAddr") or user).lower()
            alias  = ALIAS_BY_ADDR.get(addr, addr[:6])
            vis    = [f for f in changed_list if f not in SUPPRESS_LOG_FIELDS]
            old_sz, new_sz = _pick(op, np, "szi", 0)
            old_px, new_px = _pick(op, np, "entryPx")   # 新增：提取入场价
            side_disp = side_new or side_old or "-"
            # ➜ 若本轮没有 szi 变化(ev="")，尝试从上一步 msg_common 里取；仍取不到则用 📈
            icon = EVENT_ICON.get(ev or msg_common.get("event", ""), "📈")

            # -------- 组装推送正文 -------- #
            mid_now  = _mids.get(coin.upper())
            # 始终展示入场价与杠杆（无则用 "-" 占位）
            entry_px = _val(np, "entryPx") or _val(op, "entryPx") or "-"
            lev_val  = _val(np, "leverage.value") or _val(op, "leverage.value") or "-"

            lines = [
                f"sz: `{_esc(old_sz)}` → `{_esc(new_sz)}`",
                f"mid: `{_esc(f'{mid_now:.6g}')}`" if mid_now is not None else "mid: _N/A_",
                f"px: `{_esc(entry_px)}`",
                f"lev: `{_esc(lev_val)}x`"
            ]

            tg_push(
                addr,
                f"{icon} *{_esc(alias)}* `{_esc(addr)}…`  "
                f"{_esc(coin)} {_esc(side_disp)}\n"
                f"Δ: `{_esc(', '.join(vis))}`\n"
                + "\n".join(lines)
            )
    # -------------------------------------------------------------
    # 更新快照（⚠️ 增量合并，不能直接整包覆盖）
    #   · 持仓 assetPositions —— 按 coin 覆盖 / 新增；未变动的保留
    #   · 资产环境 assetCtxs  —— 若 ctx 带 coin 字段同样按 coin 合并；
    #                           否则（顺序对齐模式）只能整体替换
    # -------------------------------------------------------------
    snap = _snapshots.setdefault(user, {"positions": [], "assetCtxs": []})

    # ---------- 1) positions 合并 ---------- #
    # ① 先把新报文转成 {coin: position}
    pos_by_coin = {p["position"]["coin"]: copy.deepcopy(p) for p in new_pos}

    # ② 如果某 coin 在旧快照里有，但这次报文 **没带**，说明仓位已彻底关闭 → 移除
    #    （否则会导致每轮都判定 open/close）
    snap["positions"] = list(pos_by_coin.values())

   # ---------- 2) assetCtxs 合并 / 覆盖 ---------- #
    if new_env:
        # ②-a ctx 自带 coin 字段 → 按 coin 合并
        if all(isinstance(c, dict) and "coin" in c for c in new_env):
            ctx_map = {
                c["coin"]: c
                for c in snap["assetCtxs"]
                if isinstance(c, dict) and "coin" in c
            }
            for c in new_env:
                ctx_map[c["coin"]] = copy.deepcopy(c)
            snap["assetCtxs"] = list(ctx_map.values())
        else:
            # ②-b 无 coin 字段（顺序对齐模式） → 只能整体替换
            snap["assetCtxs"] = copy.deepcopy(new_env)
# ------------------------------------------------------------------ #
# 新增：处理 WsUserEventFill，实时计算净头寸                     #
# ------------------------------------------------------------------ #
def on_fill_update(user: str, payload: dict):
    """
    payload 形如：
      { "type":"WsUserEventFill",
        "fills":[{ "coin":"ETH","side":"B","sz":"0.2","price":"3500",... }, ...] }
    """
    # ① 兼容外层 "WsUserEvent" 批量模式
    if payload.get("type") == "WsUserEvent":
        events = payload.get("events", [])
        for ev in events:
            if ev.get("type") == "WsUserEventFill":
                _process_fills(user, ev["fills"])
    # ② 旧结构：直接就是 Fill
    elif payload.get("type") == "WsUserEventFill":
        _process_fills(user, payload.get("fills", []))
    # 其余类型直接返回
    return

# ---- 具体处理逻辑抽成函数 ---- #
def _process_fills(user: str, fills: list):
    global _net
    if user not in _net:
        _net[user] = {}

    for fill in fills:
        coin = fill["coin"]
        sz   = float(fill["sz"])
        side = fill.get("side", "").upper()
        sign = +1 if side in ("B", "BID", "BUY") else -1   # 兜底多枚举

        prev = _net[user].get(coin, 0.0)
        curr = prev + sign * sz

        # --- 容差处理，消除浮点尾差 --- #
        if abs(curr) < EPS:
            curr = 0.0
        _net[user][coin] = curr
        _save_net(_net)
        # ----------- 判定事件类型 ------------- #
        if abs(prev) < EPS and abs(curr) >= EPS:
            etype = "OPEN"
        elif abs(prev) >= EPS and abs(curr) < EPS:
            etype = "CLOSE"
        elif prev * curr > 0:                # 同号 → 加/减仓
            etype = "INC" if abs(curr) > abs(prev) else "DEC"
        else:                                # 异号 → 反手
            etype = "REVERSE"

        logging.info("[FILL] %s %-4s %s  prev=%s → curr=%s",
                     user, coin, etype, prev, curr)

        # 推送到 MQ（与 clearinghouseState 统一格式但加字段 etype="FILL"）
        if user.lower() in MQ_ADDRS:               # 直接用 user（钱包地址）
            try:
                mid_now = _mids.get(coin.upper())
                # 从快照里找最近的杠杆
                lev_from_snap = None
                try:
                    for pp in _snapshots.get(user, {}).get("positions", []):
                        if pp.get("position", {}).get("coin") == coin:
                            lv = pp["position"].get("leverage", {}).get("value")
                            lev_from_snap = float(lv) if lv is not None else None
                            break
                except Exception:
                    pass
                get_producer(QUEUE_OPS).publish({
                    "msg_id": f"{user}.{coin}.{int(time.time()*1_000_000)}",   # ← 新增
                    "ts": int(time.time()*1e3),
                    "user":   user,
                    "coin":   coin,
                    "etype":  "FILL",
                    "side":   "LONG" if curr >  EPS else ("SHORT" if curr < -EPS else "-"),
                    "event":  etype,          # OPEN/CLOSE/INC/DEC/REVERSE
                    "prevSz": prev,
                    "currSz": curr,
                    "midPx":  mid_now, 
                    "lev":    lev_from_snap,
                    "source": "HL",
                    "schema": 2,
                    "snapshot": {"szi": curr, "lev": lev_from_snap, "leverage": lev_from_snap}
                })
            except Exception as e:
                logging.error("MQ publish OPS failed (fill): %s", e, exc_info=True)

# ────────────────────────────────────────────────────────────
#  计算当前地址的“总仓位（USDT名义）” = Σ |szi| * entryPx
#  · 仅统计 entryPx>0 且 |szi|>EPS 的持仓
#  · 返回 float，消费端可直接使用
# ────────────────────────────────────────────────────────────
def _portfolio_total_usdt(pos_list: list | None) -> float:
    total = 0.0
    if not pos_list:
        return total
    for p in pos_list:
        try:
            pos   = p.get("position") or {}
            szi   = float(pos.get("szi") or 0.0)
            entry = float(pos.get("entryPx") or 0.0)
            if abs(szi) >= EPS and entry > 0:
                total += abs(szi) * entry
        except Exception:
            # 单条异常不影响整体统计
            continue
    return total

if __name__ == "__main__":
    # 传入两个回调：位置更新 & 成交更新
    mgr = HLWebSocketManager(
        ADDR_LIST,
        on_position_update,
        on_fill=on_fill_update,
        on_mids=on_mid_update,         # ← 新增
        extra_subs=["userEvents"]
    )
    mgr.start()
    ticker = SnapshotTicker(14400)
    globals()["ticker"] = ticker        # 供 on_position_update 调用
    ticker.start()
    # ────────────────────────────────────────────────────────────────
    # 🆕 Snapshot-Server：监听 queue_snap_req → 回放 OPEN/SIZE 消息
    # ────────────────────────────────────────────────────────────────
    async def _snapshot_server() -> None:
        conn = await aio_pika.connect_robust(MQCFG["url"])
        ch   = await conn.channel()
        await ch.set_qos(1)
        req_q = await ch.declare_queue(QUEUE_SNAP_REQ, durable=True)

        async with req_q.iterator() as it:
            async for req_msg in it:
                async with req_msg.process():
                    try:
                        req = json.loads(req_msg.body)
                    except Exception:
                        continue
                    if req.get("type") != "SNAPSHOT_REQ":
                        continue

                    now_ms = int(time.time()*1e3)
                    for addr, snap in _snapshots.items():
                        # ✨ 每个地址一次性预计算总仓位（USDT）
                        _total_usdt_snapshot = _portfolio_total_usdt(snap.get("positions", []))
 
                        for p in snap.get("positions", []):
                            pos = p["position"]
                            szi = float(pos["szi"])
                            if abs(szi) < EPS:
                                continue
                            lv_raw = pos.get("leverage", {}).get("value")
                            try:
                                lev = float(lv_raw) if lv_raw is not None else None
                            except Exception:
                                lev = None
                            side = "LONG" if szi > 0 else "SHORT"
                            mid_now = _mids.get(pos["coin"].upper())
                            mq_msg = {
                                "msg_id": f"{addr}.{pos['coin']}.{int(time.time()*1_000_000)}",
                                "ts":     now_ms,
                                "user":   addr,
                                "wallet": addr,
                                "coin":   pos["coin"],
                                "side":   side,
                                "midPx":  mid_now, 
                                "etype":  "SIZE",
                                "event":  "OPEN",
                                "origin": "snapshot_server",
                                "changed":["szi"],
                                "lev":    lev,
                                # 🌟 新增：整包快照的总仓位（USDT名义）
                                "portfolio_total_usdt": _total_usdt_snapshot,
                                "source": "HL",
                                "schema": 2,
                                "snapshot":{
                                    "szi":            szi,
                                    "entryPx":        pos.get("entryPx"),
                                    "unrealizedPnl":  pos.get("unrealizedPnl"),
                                    "returnOnEquity": pos.get("returnOnEquity"),
                                    "liquidationPx":  pos.get("liquidationPx"),
                                    "marginUsed":     pos.get("marginUsed"),
                                    "lev":            lev,
                                    "leverage":       lev
                                }
                            }
                            await ch.default_exchange.publish(
                                aio_pika.Message(
                                    body=json.dumps(mq_msg, separators=(",",":")).encode(),
                                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT
                                ),
                                routing_key=QUEUE_OPS
                            )

    # 后台线程常驻
    threading.Thread(
        target=lambda: asyncio.run(_snapshot_server()),
        daemon=True
    ).start()
    logging.info("Watcher started for %d addresses, snapshot tick every %.0f min.",
                 len(ADDR_LIST), ticker.interval / 60.0)

    try:
        threading.Event().wait()     # 阻塞主线程
    except KeyboardInterrupt:
        ticker.stop()
        mgr.stop()
