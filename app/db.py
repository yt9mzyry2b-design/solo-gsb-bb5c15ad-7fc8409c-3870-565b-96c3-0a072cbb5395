"""SQLite 连接与建表。

数据库文件路径由环境变量 DB_PATH 决定，默认 ./lab.db。
应用启动时调用 init_db() 自动创建数据库与表结构。
"""

import os
import sqlite3
import threading
from contextlib import contextmanager

DB_PATH = os.environ.get("DB_PATH", os.path.join(os.path.dirname(__file__), "..", "lab.db"))

# 摄入操作使用进程级锁串行化，保证"缓冲-排水-提交"在同一事务中原子完成。
ingest_lock = threading.Lock()

SCHEMA = """
CREATE TABLE IF NOT EXISTS devices (
    device_id      TEXT PRIMARY KEY,
    committed_upto INTEGER NOT NULL,          -- 已连续提交到的最大序号
    created_at     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

CREATE TABLE IF NOT EXISTS idempotency_keys (
    key           TEXT PRIMARY KEY,
    device_id     TEXT NOT NULL,
    seq           INTEGER NOT NULL,
    payload_hash  TEXT NOT NULL,              -- 请求体规范化后的 sha256，用于检测同键不同载荷
    response_json TEXT NOT NULL,              -- 首次处理结果，重放时原样返回
    created_at    TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

CREATE TABLE IF NOT EXISTS buffer (
    device_id    TEXT NOT NULL,
    seq          INTEGER NOT NULL,
    payload_json TEXT NOT NULL,
    received_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    PRIMARY KEY (device_id, seq)
);

CREATE TABLE IF NOT EXISTS committed (
    device_id    TEXT NOT NULL,
    seq          INTEGER NOT NULL,
    payload_json TEXT NOT NULL,
    committed_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    PRIMARY KEY (device_id, seq)
);

CREATE TABLE IF NOT EXISTS duplicates (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    device_id       TEXT NOT NULL,
    seq             INTEGER,
    idempotency_key TEXT,
    reason          TEXT NOT NULL,            -- idempotent_replay / already_committed / already_buffered
    received_at     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

CREATE INDEX IF NOT EXISTS idx_duplicates_device ON duplicates (device_id, id);
"""


def init_db() -> None:
    """创建数据库文件与全部表（幂等）。"""
    directory = os.path.dirname(os.path.abspath(DB_PATH))
    os.makedirs(directory, exist_ok=True)
    with get_conn() as conn:
        conn.executescript(SCHEMA)


@contextmanager
def get_conn():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=30000")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
