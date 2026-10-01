"""SQLite 访问层：连接管理与建表。

数据库路径由环境变量 DB_PATH 指定，默认 ./lightdose.db。
"""
from __future__ import annotations

import os
import sqlite3
from collections.abc import Iterator

SCHEMA = """
CREATE TABLE IF NOT EXISTS exhibits (
    id                   TEXT PRIMARY KEY,
    name                 TEXT NOT NULL,
    dose_limit_luxh      REAL NOT NULL CHECK (dose_limit_luxh > 0),
    max_interval_seconds REAL NOT NULL CHECK (max_interval_seconds > 0),
    created_at           TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

-- 读数以 (exhibit_id, reading_id) 为主键，reading_id 即客户端幂等键
CREATE TABLE IF NOT EXISTS readings (
    exhibit_id  TEXT NOT NULL REFERENCES exhibits(id) ON DELETE CASCADE,
    reading_id  TEXT NOT NULL,
    ts          REAL NOT NULL,           -- 采样时刻，Unix 秒（UTC）
    lux         REAL NOT NULL CHECK (lux >= 0),
    received_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    PRIMARY KEY (exhibit_id, reading_id)
);

CREATE INDEX IF NOT EXISTS idx_readings_exhibit_ts ON readings (exhibit_id, ts);

-- 归并后来源 ID 成为目标（规范）ID 的永久别名；所有写入与查询先解析别名。
-- 归并提交时会把来源名下的旧别名一并改指向目标，因此该表始终只有一跳，
-- 结构上不可能形成别名环。
CREATE TABLE IF NOT EXISTS exhibit_aliases (
    alias_id     TEXT PRIMARY KEY,
    canonical_id TEXT NOT NULL REFERENCES exhibits(id) ON DELETE RESTRICT,
    created_at   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

-- 归并预演令牌：绑定预演时刻两边的元数据与读数集合版本（SHA-256）。
-- 提交时重算四个哈希，任一不一致即 409；提交成功后固化 result_json，
-- 同一令牌重复提交直接回放首次结果。
CREATE TABLE IF NOT EXISTS merge_previews (
    token            TEXT PRIMARY KEY,
    source_id        TEXT NOT NULL,   -- 预演时解析出的规范来源 ID
    target_id        TEXT NOT NULL,   -- 预演时解析出的规范目标 ID
    source_meta_hash TEXT NOT NULL,
    target_meta_hash TEXT NOT NULL,
    source_read_hash TEXT NOT NULL,
    target_read_hash TEXT NOT NULL,
    conflict_count   INTEGER NOT NULL,
    created_at       TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    committed_at     TEXT,
    result_json      TEXT
);

-- 别名永久且不可复用：禁止用已归并的别名 ID 重新建档
CREATE TRIGGER IF NOT EXISTS trg_exhibit_no_alias_id
BEFORE INSERT ON exhibits
WHEN EXISTS (SELECT 1 FROM exhibit_aliases WHERE alias_id = NEW.id)
BEGIN
    SELECT RAISE(ABORT, 'exhibit id is a permanent merge alias');
END;

-- 展陈开放日历：每件展品最多一份，随档案删除级联清除；
-- 归并删除来源档案时其日历一并级联删除，目标档案日历保持不变。
CREATE TABLE IF NOT EXISTS exhibit_calendars (
    exhibit_id TEXT PRIMARY KEY REFERENCES exhibits(id) ON DELETE CASCADE,
    timezone   TEXT NOT NULL,            -- IANA 时区名，如 Asia/Shanghai
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

-- 每周开放时段：weekday 取 ISO 8601（1=周一 … 7=周日）；
-- 时刻为本地当日秒偏移 [0, 86400)；end_sec <= start_sec 表示跨午夜到次日。
CREATE TABLE IF NOT EXISTS calendar_weekly_windows (
    exhibit_id TEXT NOT NULL REFERENCES exhibit_calendars(exhibit_id) ON DELETE CASCADE,
    weekday    INTEGER NOT NULL CHECK (weekday BETWEEN 1 AND 7),
    start_sec  INTEGER NOT NULL CHECK (start_sec >= 0 AND start_sec < 86400),
    end_sec    INTEGER NOT NULL CHECK (end_sec >= 0 AND end_sec < 86400),
    PRIMARY KEY (exhibit_id, weekday, start_sec, end_sec)
);

-- 例外日：按本地日期整体替换当日每周规则；例外日可以没有任何窗口（当日闭馆），
-- 因此日期本身需要单独一行存在。
CREATE TABLE IF NOT EXISTS calendar_exception_days (
    exhibit_id TEXT NOT NULL REFERENCES exhibit_calendars(exhibit_id) ON DELETE CASCADE,
    date       TEXT NOT NULL,            -- 本地日期 YYYY-MM-DD
    PRIMARY KEY (exhibit_id, date)
);

CREATE TABLE IF NOT EXISTS calendar_exception_windows (
    exhibit_id TEXT NOT NULL,
    date       TEXT NOT NULL,
    start_sec  INTEGER NOT NULL CHECK (start_sec >= 0 AND start_sec < 86400),
    end_sec    INTEGER NOT NULL CHECK (end_sec >= 0 AND end_sec < 86400),
    PRIMARY KEY (exhibit_id, date, start_sec, end_sec),
    FOREIGN KEY (exhibit_id, date) REFERENCES calendar_exception_days(exhibit_id, date) ON DELETE CASCADE
);

-- 照度上报存证收据链：每次批量上报在写入读数的同一事务内生成一张收据。
-- 链以写入时的规范档案 ID 为标识（chain_id），seq 从 1 连续递增；
-- prev_hash 衔接上一张收据的链哈希（首条为 64 个 '0'），
-- chain_hash = SHA-256(prev_hash + content_hash)。
-- 档案归并只删除档案与读数，收据链必须原样保留供事后独立核查，
-- 因此本表故意不设外键、不级联删除，已有行绝不 UPDATE（旧哈希不可重写）。
CREATE TABLE IF NOT EXISTS ingest_receipts (
    chain_id     TEXT NOT NULL,            -- 链标识：写入时的规范档案 ID
    seq          INTEGER NOT NULL CHECK (seq >= 1),
    receipt_id   TEXT NOT NULL UNIQUE,
    prev_hash    TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    chain_hash   TEXT NOT NULL,
    payload_json TEXT NOT NULL,            -- 收据内容（请求 + 逐条结果）的 JSON
    created_at   TEXT NOT NULL,
    PRIMARY KEY (chain_id, seq)
);

-- RFID 馆藏盘点轮次：round_id 与规范化请求摘要（request_digest）一一绑定。
-- 首次对账在写事务内固化结果 JSON；相同请求重放回放原结果，不同请求 409；
-- round_id 主键 + BEGIN IMMEDIATE 保证并发提交只能固化一份结果。
-- 对账只读展品/别名表、不写读数、不接触收据链，本表是对账唯一写入点。
CREATE TABLE IF NOT EXISTS inventory_rounds (
    round_id       TEXT PRIMARY KEY,
    request_digest TEXT NOT NULL,          -- 规范化请求摘要 SHA-256（时间 UTC、别名解析后）
    result_json    TEXT NOT NULL,          -- InventoryRoundOut 的 JSON（首次固化结果）
    created_at     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);
"""


def db_path() -> str:
    return os.environ.get("DB_PATH", "lightdose.db")


def connect() -> sqlite3.Connection:
    # check_same_thread=False：FastAPI 的线程池不保证依赖的创建与关闭落在
    # 同一线程；连接本身只在单个请求生命周期内顺序使用，并发安全由
    # SQLite 自身的锁（WAL + BEGIN IMMEDIATE）保证。
    conn = sqlite3.connect(db_path(), timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    return conn


def init_db() -> None:
    with connect() as conn:
        conn.executescript(SCHEMA)


def get_conn() -> Iterator[sqlite3.Connection]:
    """FastAPI 依赖：每请求一个连接。"""
    conn = connect()
    try:
        yield conn
    finally:
        conn.close()
