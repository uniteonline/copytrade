"""SQLite 管理类
~~~~~~~~~~~~~~~~
线程安全、自动建表、简单批量写入。
"""

import sqlite3, threading, pathlib, time
DB_FILE = pathlib.Path(__file__).with_name("positions.db")


class SQLiteManager:
    _DDL = """
    CREATE TABLE IF NOT EXISTS position_changes (
        id      INTEGER PRIMARY KEY AUTOINCREMENT,
        ts      INTEGER,          -- 13-位毫秒时间戳
        user    TEXT,
        wallet    TEXT,          -- 新增：钱包地址
        coin    TEXT,
        side    TEXT,
        changed TEXT,             -- 逗号分隔字段名
        szi             TEXT,
        entryPx         TEXT,
        liquidationPx   TEXT,
        unrealizedPnl   TEXT,
        returnOnEquity  TEXT,
        marginUsed      TEXT,
        funding         TEXT,
        openInterest    TEXT,
        premium         TEXT
    );
    """

    def __init__(self, db_path: str | None = None):
        self._db = sqlite3.connect(
            db_path or str(DB_FILE),
            check_same_thread=False,     # 允许跨线程 :contentReference[oaicite:0]{index=0}
        )
        self._db.execute("PRAGMA journal_mode = WAL")      # 提升并发 :contentReference[oaicite:1]{index=1}
        self._db.execute(self._DDL)
        self._lock = threading.Lock()

    def insert_change(self, row: dict) -> None:
        """row: 字段名 → 值；缺失的列会自动填 NULL"""
        cols, vals = zip(*row.items())
        sql = f"INSERT INTO position_changes ({','.join(cols)}) VALUES ({','.join(['?']*len(vals))})"
        with self._lock:
            self._db.execute(sql, vals)
            self._db.commit()

    # 可选：批量写入
    def executemany(self, rows: list[dict]) -> None:
        if not rows:
            return
        cols = rows[0].keys()
        sql = f"INSERT INTO position_changes ({','.join(cols)}) VALUES ({','.join(['?']*len(cols))})"
        data = [tuple(r[c] for c in cols) for r in rows]
        with self._lock:
            self._db.executemany(sql, data)                # executemany :contentReference[oaicite:2]{index=2}
            self._db.commit()


# 单元测试（直接运行时）
if __name__ == "__main__":
    db = SQLiteManager()
    db.insert_change(
        {"ts": int(time.time()*1e3), "user": "demo", "coin": "BTC", "side": "Long",
         "changed": "entryPx", "szi": "1", "entryPx": "100"}
    )
    print("⭐ 写入成功，当前行数：",
          db._db.execute("SELECT COUNT(*) FROM position_changes").fetchone()[0])
