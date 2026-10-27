#!/usr/bin/env python3
"""
Migrate the live CN CALL SQLite database to PostgreSQL.

Safety rules:
- Reads SQLite; never modifies the source database.
- Refuses to overwrite a non-empty PostgreSQL database unless --force is used.
- Imports all five CN CALL tables in one PostgreSQL transaction.
- Verifies row counts and SHA-256 fingerprints before commit.
- The source SQLite file can remain on the Railway Volume for rollback.

Usage:
  DATABASE_URL='postgresql://...' python migrate_sqlite_to_postgres.py
  DATABASE_URL='postgresql://...' python migrate_sqlite_to_postgres.py --sqlite /app/data/cn_call.db

Do not set DATABASE_URL in the running API to PostgreSQL until this migration
has completed successfully and the verification output says all tables match.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
from pathlib import Path

import psycopg
from psycopg.rows import dict_row


TABLES = {
    "users": {
        "columns": [
            "user_id",
            "username",
            "password_hash",
            "password_salt",
            "created_at",
        ],
        "order_by": "user_id",
    },
    "fcm_tokens": {
        "columns": [
            "user_id",
            "token",
            "updated_at",
        ],
        "order_by": "user_id",
    },
    "access_tokens": {
        "columns": [
            "token",
            "user_id",
            "created_at",
        ],
        "order_by": "token",
    },
    "call_records": {
        "columns": [
            "call_id",
            "caller_id",
            "target_id",
            "caller_name",
            "created_at",
            "expires_at",
            "status",
            "negotiation_expires_at",
            "connection_expires_at",
            "media_ready_users",
            "state_version",
        ],
        "order_by": "call_id",
    },
    "durable_terminal_events": {
        "columns": [
            "event_id",
            "call_id",
            "source_user_id",
            "target_user_id",
            "event_type",
            "created_at",
            "acknowledged_at",
            "state_version",
            "last_attempt_at",
            "attempt_count",
        ],
        "order_by": "event_id",
    },
}


SCHEMA = [
    """
    CREATE TABLE IF NOT EXISTS users (
        user_id TEXT PRIMARY KEY,
        username TEXT NOT NULL,
        password_hash TEXT NOT NULL,
        password_salt TEXT NOT NULL,
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP::text
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS fcm_tokens (
        user_id TEXT PRIMARY KEY,
        token TEXT NOT NULL,
        updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP::text
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS access_tokens (
        token TEXT PRIMARY KEY,
        user_id TEXT NOT NULL UNIQUE,
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP::text
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS call_records (
        call_id TEXT PRIMARY KEY,
        caller_id TEXT NOT NULL,
        target_id TEXT NOT NULL,
        caller_name TEXT NOT NULL,
        created_at BIGINT NOT NULL,
        expires_at BIGINT NOT NULL,
        status TEXT NOT NULL,
        negotiation_expires_at BIGINT,
        connection_expires_at BIGINT,
        media_ready_users TEXT NOT NULL DEFAULT '[]',
        state_version INTEGER NOT NULL DEFAULT 1
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS durable_terminal_events (
        event_id TEXT PRIMARY KEY,
        call_id TEXT NOT NULL,
        source_user_id TEXT NOT NULL,
        target_user_id TEXT NOT NULL,
        event_type TEXT NOT NULL,
        created_at BIGINT NOT NULL,
        acknowledged_at BIGINT,
        state_version INTEGER NOT NULL DEFAULT 1,
        last_attempt_at BIGINT,
        attempt_count INTEGER NOT NULL DEFAULT 0
    )
    """,
    """
    CREATE UNIQUE INDEX IF NOT EXISTS idx_durable_terminal_pending
    ON durable_terminal_events (
        call_id,
        target_user_id,
        event_type
    )
    WHERE acknowledged_at IS NULL
    """,
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="CN CALL SQLite -> PostgreSQL migration")
    parser.add_argument(
        "--sqlite",
        default=os.getenv("CN_CALL_SQLITE_PATH", "/app/data/cn_call.db"),
        help="Path to the live SQLite database (read-only source).",
    )
    parser.add_argument(
        "--database-url",
        default=os.getenv("DATABASE_URL", "").strip(),
        help="PostgreSQL connection URL. Defaults to DATABASE_URL.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Allow replacing existing PostgreSQL table contents.",
    )
    return parser.parse_args()


def row_fingerprint(rows: list[dict], columns: list[str]) -> str:
    digest = hashlib.sha256()
    for row in rows:
        payload = [
            row.get(column)
            for column in columns
        ]
        digest.update(
            (
                json.dumps(
                    payload,
                    ensure_ascii=False,
                    separators=(",", ":"),
                    default=str,
                )
                + "\n"
            ).encode("utf-8")
        )
    return digest.hexdigest()


def read_sqlite_rows(sqlite_db: sqlite3.Connection, table: str) -> list[dict]:
    spec = TABLES[table]
    columns = spec["columns"]
    query = (
        f"SELECT {', '.join(columns)} "
        f"FROM {table} ORDER BY {spec['order_by']}"
    )
    return [
        dict(row)
        for row in sqlite_db.execute(query).fetchall()
    ]


def read_postgres_rows(postgres_db, table: str) -> list[dict]:
    spec = TABLES[table]
    columns = spec["columns"]
    query = (
        f"SELECT {', '.join(columns)} "
        f"FROM {table} ORDER BY {spec['order_by']}"
    )
    return [
        dict(row)
        for row in postgres_db.execute(query).fetchall()
    ]


def target_counts(postgres_db) -> dict[str, int]:
    counts: dict[str, int] = {}
    for table in TABLES:
        row = postgres_db.execute(
            f"SELECT COUNT(*) AS count FROM {table}"
        ).fetchone()
        counts[table] = int(row["count"])
    return counts


def create_schema(postgres_db) -> None:
    for statement in SCHEMA:
        postgres_db.execute(statement)


def clear_target(postgres_db) -> None:
    for table in reversed(list(TABLES)):
        postgres_db.execute(f"TRUNCATE TABLE {table}")


def import_table(postgres_db, table: str, rows: list[dict]) -> None:
    if not rows:
        return

    columns = TABLES[table]["columns"]
    placeholders = ", ".join(["%s"] * len(columns))
    query = (
        f"INSERT INTO {table} ({', '.join(columns)}) "
        f"VALUES ({placeholders})"
    )

    values = [
        tuple(row[column] for column in columns)
        for row in rows
    ]

    with postgres_db.cursor() as cursor:
        cursor.executemany(query, values)


def main() -> int:
    args = parse_args()

    sqlite_path = Path(args.sqlite)
    database_url = args.database_url.strip()

    if not sqlite_path.exists():
        raise SystemExit(f"ERROR: SQLite source not found: {sqlite_path}")

    if not database_url:
        raise SystemExit("ERROR: DATABASE_URL is required.")

    sqlite_db = sqlite3.connect(f"file:{sqlite_path}?mode=ro", uri=True)
    sqlite_db.row_factory = sqlite3.Row

    try:
        source_rows = {
            table: read_sqlite_rows(sqlite_db, table)
            for table in TABLES
        }
    finally:
        sqlite_db.close()

    print("=== CN CALL SQLite -> PostgreSQL migration ===")
    print(f"SQLite source: {sqlite_path}")
    for table, rows in source_rows.items():
        print(f"  {table}: source_rows={len(rows)}")

    postgres_db = psycopg.connect(
        database_url,
        row_factory=dict_row,
        connect_timeout=10,
    )

    try:
        create_schema(postgres_db)
        existing = target_counts(postgres_db)

        if any(existing.values()) and not args.force:
            raise SystemExit(
                "ERROR: PostgreSQL already contains data. "
                "Use a fresh database or rerun with --force after reviewing it."
            )

        if args.force:
            clear_target(postgres_db)

        for table, rows in source_rows.items():
            import_table(postgres_db, table, rows)

        print("=== Verification before commit ===")
        for table, rows in source_rows.items():
            target_rows = read_postgres_rows(postgres_db, table)
            source_hash = row_fingerprint(rows, TABLES[table]["columns"])
            target_hash = row_fingerprint(target_rows, TABLES[table]["columns"])

            count_ok = len(rows) == len(target_rows)
            hash_ok = source_hash == target_hash

            print(
                f"{table}: "
                f"source={len(rows)} target={len(target_rows)} "
                f"count={'OK' if count_ok else 'FAIL'} "
                f"hash={'OK' if hash_ok else 'FAIL'}"
            )

            if not count_ok or not hash_ok:
                raise RuntimeError(
                    f"Verification failed for {table}; transaction will be rolled back."
                )

        postgres_db.commit()
        print("=== Migration committed successfully ===")
        print("SQLite source was not modified.")
        return 0

    except BaseException:
        postgres_db.rollback()
        raise
    finally:
        postgres_db.close()


if __name__ == "__main__":
    raise SystemExit(main())
