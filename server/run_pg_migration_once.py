#!/usr/bin/env python3
"""
One-time safe runner for migrating the live CN CALL SQLite database.

The API is expected to be in CN_CALL_MIGRATION_MODE while this runs, so no
application writes are accepted. The SQLite source is opened read-only and
copied with SQLite's online backup API before migration. A marker file on the
same volume prevents accidental re-import on a retry after a successful
migration.
"""

from __future__ import annotations

import hashlib
import os
import sqlite3
from pathlib import Path

from migrate_sqlite_to_postgres import main as migration_main


SOURCE = Path("/app/data/cn_call.db")
BACKUP = Path("/app/data/cn_call-pre-postgres-backup.sqlite")
MARKER = Path("/app/data/.cn-call11-postgres-migration-complete-v1")


def backup_live_sqlite() -> str:
    if not SOURCE.exists():
        raise SystemExit(f"ERROR: live SQLite source not found: {SOURCE}")

    source = sqlite3.connect(f"file:{SOURCE}?mode=ro", uri=True)
    try:
        backup = sqlite3.connect(BACKUP)
        try:
            source.backup(backup)
            backup.commit()
        finally:
            backup.close()
    finally:
        source.close()

    digest = hashlib.sha256()
    with BACKUP.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)

    size = BACKUP.stat().st_size
    sha256 = digest.hexdigest()
    print(f"[CN CALL][MIGRATION BACKUP] path={BACKUP} size={size} sha256={sha256}")
    return sha256


def main() -> int:
    if MARKER.exists():
        print(f"[CN CALL][MIGRATION] marker exists: {MARKER}; skipping re-import")
        return 0

    database_url = os.getenv("MIGRATION_DATABASE_URL", "").strip()
    if not database_url:
        raise SystemExit("ERROR: MIGRATION_DATABASE_URL is required for migration")

    backup_live_sqlite()

    original_argv = os.sys.argv[:]
    try:
        os.sys.argv = [
            "migrate_sqlite_to_postgres.py",
            "--sqlite",
            str(BACKUP),
            "--database-url",
            database_url,
        ]
        result = migration_main()
    finally:
        os.sys.argv = original_argv

    if result != 0:
        raise SystemExit(result)

    MARKER.write_text("migration completed successfully\n", encoding="utf-8")
    print(f"[CN CALL][MIGRATION] marker created: {MARKER}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
