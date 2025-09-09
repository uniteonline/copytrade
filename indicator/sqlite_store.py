# -*- coding: utf-8 -*-
from __future__ import annotations
import sqlite3
from pathlib import Path
import threading
from typing import Dict, Iterable, Tuple, Optional, List

class SqliteStore:
    """
    轻量本地缓存：
      - trades(inst_id, trade_id, ts_ms, px, sz, side)  主键(inst_id, trade_id)
      - vp(inst_id, bin_px, buy, sell)                  主键(inst_id, bin_px)
      - tpo(inst_id, ts_min, mid)                       主键(inst_id, ts_min)  (ts_min = 分钟起始ms)
      - state(inst_id, last_trade_ts, last_tpo_min_ts, last_trade_id)
    说明：
      * WAL  synchronous=NORMAL，读写并发更友好；UPSERT 去重与累加。
    """
    def __init__(self, db_path: Path):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL;")
        self._conn.execute("PRAGMA synchronous=NORMAL;")
        self._conn.execute("PRAGMA foreign_keys=ON;")
        self._conn.execute("PRAGMA busy_timeout=5000;")
        self._lock = threading.RLock()
        self._init_schema()

    def _init_schema(self) -> None:
        cur = self._conn.cursor()
        cur.executescript(
            """
            CREATE TABLE IF NOT EXISTS trades(
              inst_id   TEXT NOT NULL,
              trade_id  TEXT NOT NULL,
              ts_ms     INTEGER NOT NULL,
              px        REAL NOT NULL,
              sz        REAL NOT NULL,
              side      TEXT NOT NULL,
              PRIMARY KEY(inst_id, trade_id)
            );
            CREATE INDEX IF NOT EXISTS idx_trades_inst_ts ON trades(inst_id, ts_ms);
            CREATE TABLE IF NOT EXISTS vp(
              inst_id   TEXT NOT NULL,
              bin_px    REAL NOT NULL,
              buy       REAL NOT NULL DEFAULT 0,
              sell      REAL NOT NULL DEFAULT 0,
              PRIMARY KEY(inst_id, bin_px)
            );
            CREATE INDEX IF NOT EXISTS idx_vp_inst ON vp(inst_id);
            CREATE TABLE IF NOT EXISTS tpo(
              inst_id   TEXT NOT NULL,
              ts_min    INTEGER NOT NULL,
              mid       REAL NOT NULL,
              PRIMARY KEY(inst_id, ts_min)
            );
            CREATE TABLE IF NOT EXISTS state(
              inst_id          TEXT PRIMARY KEY,
              last_trade_ts    INTEGER,
              last_tpo_min_ts  INTEGER,
              last_trade_id    TEXT
            );
            -- 记录每次成功回补/落库的覆盖区间（用于发现 72h 窗内“缺块”）
            CREATE TABLE IF NOT EXISTS backfill_chunks(
              inst_id   TEXT NOT NULL,
              start_ms  INTEGER NOT NULL,
              end_ms    INTEGER NOT NULL,
              kind      TEXT NOT NULL DEFAULT 'trades',
              PRIMARY KEY(inst_id, start_ms, end_ms, kind)
            );
            CREATE INDEX IF NOT EXISTS idx_chunks_inst_kind
              ON backfill_chunks(inst_id, kind, start_ms, end_ms);
            -- 记录“最早已覆盖到的毫秒时间戳”，供 full 模式决定还需回补多少
            CREATE TABLE IF NOT EXISTS backfill_progress(
              inst_id     TEXT PRIMARY KEY,
              earliest_ms INTEGER
            );
            """
        )
        self._conn.commit()

    # ------------------- state -------------------
    def get_last_trade_ts(self, inst_id: str) -> Optional[int]:
        # 先查 state；若不存在则从 trades 回推并自愈写回 state（避免重头回补）
        with self._lock:
            cur = self._conn.execute(
                "SELECT last_trade_ts FROM state WHERE inst_id=?", (inst_id,)
            )
            row = cur.fetchone()
            if row and row[0] is not None:
                return int(row[0])
            # fallback：用 trades 中的最大 ts_ms 自愈
            cur = self._conn.execute(
                "SELECT MAX(ts_ms) FROM trades WHERE inst_id=?", (inst_id,)
            )
            r = cur.fetchone()
            if r and r[0] is not None:
                ts = int(r[0])
                self._conn.execute(
                    "INSERT INTO state(inst_id, last_trade_ts) VALUES(?, ?) "
                    "ON CONFLICT(inst_id) DO UPDATE SET last_trade_ts=excluded.last_trade_ts",
                    (inst_id, ts),
                )
                self._conn.commit()
                return ts
            return None

    def set_last_trade_state(self, inst_id: str, ts_ms: Optional[int], trade_id: Optional[str]) -> None:
        """
        只在新时间戳更晚时才更新 last_trade_ts/last_trade_id，确保单调不回退。
        """
        if ts_ms is None:
            return
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO state(inst_id, last_trade_ts, last_trade_id)
                VALUES(?, ?, ?)
                ON CONFLICT(inst_id) DO UPDATE SET
                last_trade_ts = CASE
                                    WHEN excluded.last_trade_ts >= COALESCE(state.last_trade_ts, 0)
                                    THEN excluded.last_trade_ts
                                    ELSE state.last_trade_ts
                                END,
                last_trade_id = CASE
                                    WHEN excluded.last_trade_ts >= COALESCE(state.last_trade_ts, 0)
                                    THEN excluded.last_trade_id
                                    ELSE state.last_trade_id
                                END
                """,
                (inst_id, ts_ms, trade_id),
            )
            self._conn.commit()

    def set_last_tpo_min(self, inst_id: str, ts_min: int) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO state(inst_id, last_tpo_min_ts)
                VALUES(?, ?)
                ON CONFLICT(inst_id) DO UPDATE SET
                last_tpo_min_ts=excluded.last_tpo_min_ts
                """,
                (inst_id, ts_min),
            )
            self._conn.commit()

    # ------------------- trades -------------------
    def upsert_trade(self, inst_id: str, trade_id: str, ts_ms: int, px: float, sz: float, side: str) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO trades(inst_id, trade_id, ts_ms, px, sz, side)
                VALUES(?, ?, ?, ?, ?, ?)
                ON CONFLICT(inst_id, trade_id) DO NOTHING
                """,
                (inst_id, trade_id, ts_ms, px, sz, side),
            )

    def bulk_upsert_trades(self, inst_id: str, rows: Iterable[Dict]) -> None:
        # rows: [{'tradeId': str, 'ts': int, 'px': float, 'sz': float, 'side': 'buy'|'sell'}, ...]
        with self._lock:
            self._conn.execute("BEGIN;")
            for r in rows:
                self._conn.execute(
                    """
                    INSERT INTO trades(inst_id, trade_id, ts_ms, px, sz, side)
                    VALUES(?, ?, ?, ?, ?, ?)
                    ON CONFLICT(inst_id, trade_id) DO NOTHING
                    """,
                    (inst_id, r.get("tradeId",""), int(r["ts"]), float(r["px"]), float(r["sz"]), str(r.get("side",""))),
                )
            self._conn.commit()

    # ------------------- vp (bin 累加) -------------------
    def vp_accumulate(self, inst_id: str, bin_px: float, buy: float, sell: float) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO vp(inst_id, bin_px, buy, sell)
                VALUES(?, ?, ?, ?)
                ON CONFLICT(inst_id, bin_px) DO UPDATE SET
                buy  = vp.buy  + excluded.buy,
                sell = vp.sell + excluded.sell
                """,
                (inst_id, float(bin_px), float(buy), float(sell)),
            )

    def bulk_vp_accumulate(self, inst_id: str, bin_to_buy_sell: Dict[float, Tuple[float, float]]) -> None:
        with self._lock:
            self._conn.execute("BEGIN;")
            for bin_px, (buy, sell) in bin_to_buy_sell.items():
                self._conn.execute(
                    """
                    INSERT INTO vp(inst_id, bin_px, buy, sell)
                    VALUES(?, ?, ?, ?)
                    ON CONFLICT(inst_id, bin_px) DO UPDATE SET
                    buy  = vp.buy  + excluded.buy,
                    sell = vp.sell + excluded.sell
                    """,
                    (inst_id, float(bin_px), float(buy), float(sell)),
                )
            self._conn.commit()

    # ------------------- tpo(分钟) -------------------
    def tpo_upsert_minute(self, inst_id: str, ts_min: int, mid: float) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO tpo(inst_id, ts_min, mid)
                VALUES(?, ?, ?)
                ON CONFLICT(inst_id, ts_min) DO UPDATE SET
                mid = excluded.mid
                """,
                (inst_id, int(ts_min), float(mid)),
            )

    def bulk_tpo_upsert(self, inst_id: str, rows: Iterable[Tuple[int, float]]) -> None:
        # rows: [(ts_min, mid), ...]
        with self._lock:
            self._conn.execute("BEGIN;")
            for ts_min, mid in rows:
                self._conn.execute(
                    """
                    INSERT INTO tpo(inst_id, ts_min, mid)
                    VALUES(?, ?, ?)
                    ON CONFLICT(inst_id, ts_min) DO UPDATE SET
                    mid = excluded.mid
                    """,
                    (inst_id, int(ts_min), float(mid)),
                )
            self._conn.commit()

    # ------------------- housekeeping -------------------
    def commit(self) -> None:
        self._conn.commit()

    def close(self) -> None:
        try:
            self._conn.commit()
        finally:
            self._conn.close()

    # ------------------- queries -------------------
    def get_vp_top_price(self, inst_id: str) -> Optional[Tuple[float, float, float]]:
        """
        返回 (bin_px, buy, sell) —— buy+sell 最大的价位；没有数据返回 None
        """
        cur = self._conn.execute(
            """
            SELECT bin_px, buy, sell
            FROM vp
            WHERE inst_id = ?
            ORDER BY (buy + sell) DESC
            LIMIT 1
            """,
            (inst_id,),
        )
        row = cur.fetchone()
        return (float(row[0]), float(row[1]), float(row[2])) if row else None

    # =================== backfill 进度 & 区间 ===================
    # —— 与 okx_client.py 的 full/gap 回补配合使用 —— 

    # 记录“最早覆盖到”的时间戳（毫秒）
    def set_backfill_earliest_ms(self, inst_id: str, earliest_ms: int) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO backfill_progress(inst_id, earliest_ms)
                VALUES(?, ?)
                ON CONFLICT(inst_id) DO UPDATE SET earliest_ms=excluded.earliest_ms
                """,
                (inst_id, int(earliest_ms)),
            )
            self._conn.commit()

    def get_backfill_earliest_ms(self, inst_id: str) -> Optional[int]:
        cur = self._conn.execute(
            "SELECT earliest_ms FROM backfill_progress WHERE inst_id=?",
            (inst_id,),
        )
        row = cur.fetchone()
        return int(row[0]) if row and row[0] is not None else None

    # 记录一次成功写入（回补）覆盖到的时间段（毫秒区间）
    def add_backfill_chunk(self, inst_id: str, start_ms: int, end_ms: int, kind: str = "trades") -> None:
        if start_ms is None or end_ms is None:
            return
        s, e = int(start_ms), int(end_ms)
        if s > e:
            s, e = e, s
        with self._lock:
            self._conn.execute(
                """
                INSERT OR IGNORE INTO backfill_chunks(inst_id, start_ms, end_ms, kind)
                VALUES(?, ?, ?, ?)
                """,
                (inst_id, s, e, str(kind)),
            )
            self._conn.commit()

    # 读取并合并窗口内已覆盖的区间（闭区间并集）
    def get_covered_intervals(
        self,
        inst_id: str,
        window_start_ms: int,
        window_end_ms: int,
        kind: str = "trades",
    ) -> List[Tuple[int, int]]:
        ws, we = int(window_start_ms), int(window_end_ms)
        rows = self._conn.execute(
            """
            SELECT start_ms, end_ms
            FROM backfill_chunks
            WHERE inst_id=? AND kind=? AND end_ms>=? AND start_ms<=?
            ORDER BY start_ms ASC
            """,
            (inst_id, str(kind), ws, we),
        ).fetchall()
        if not rows:
            return []
        merged: List[List[int]] = []
        for s, e in rows:
            s, e = int(s), int(e)
            if not merged or s > merged[-1][1] + 1:
                merged.append([s, e])
            else:
                merged[-1][1] = max(merged[-1][1], e)
        return [(a, b) for a, b in merged]

    # 求窗口 [window_start_ms, window_end_ms) 内的“显著缺口”
    def get_window_gaps(
        self,
        inst_id: str,
        window_start_ms: int,
        window_end_ms: int,
        min_gap_ms: int,
        kind: str = "trades",
    ) -> List[Tuple[int, int]]:
        ws, we = int(window_start_ms), int(window_end_ms)
        cov = self.get_covered_intervals(inst_id, ws, we, kind)
        gaps: List[Tuple[int, int]] = []
        cur = ws
        for s, e in cov:
            if s > cur and (s - cur) >= int(min_gap_ms):
                gaps.append((cur, s))
            cur = max(cur, e + 1)
        if we > cur and (we - cur) >= int(min_gap_ms):
            gaps.append((cur, we))
        return gaps