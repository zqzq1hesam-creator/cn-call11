# Railway deploy marker: keep runtime behavior unchanged; force GitHub source refresh.
from fastapi import FastAPI, Header, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from pathlib import Path
import hashlib
import asyncio
import hmac
import json
import os
import re
import sqlite3
import time
import psycopg
from psycopg.rows import dict_row
import shutil
import uuid
from datetime import timedelta

import firebase_admin
from firebase_admin import credentials, messaging
from livekit import api


app = FastAPI(title="CN CALL Server")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def migration_maintenance_middleware(request, call_next):
    if CN_CALL_MIGRATION_MODE and request.url.path != "/health":
        return JSONResponse(
            status_code=503,
            content={
                "success": False,
                "message": "CN CALL is temporarily under maintenance",
            },
        )
    return await call_next(request)


BASE_DIR = Path(__file__).resolve().parent
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
CN_CALL_MIGRATION_MODE = os.getenv("CN_CALL_MIGRATION_MODE", "").strip().lower() in {"1", "true", "yes"}
VOLUME_DIR = Path("/app/data")
DB_PATH = VOLUME_DIR / "cn_call.db"
LEGACY_DB_PATH = BASE_DIR / "cn_call.db"

# PostgreSQL is the preferred persistent database. SQLite/Volume remains as
# a temporary fallback until DATABASE_URL is configured in Railway.
if not DATABASE_URL:
    VOLUME_DIR.mkdir(parents=True, exist_ok=True)

    if not DB_PATH.exists() and LEGACY_DB_PATH.exists():
        shutil.copy2(LEGACY_DB_PATH, DB_PATH)
        print("[CN CALL][DB] Legacy database migrated to Railway Volume")
else:
    print("[CN CALL][DB] PostgreSQL backend selected via DATABASE_URL")


connections: dict[str, WebSocket] = {}
active_calls: dict[str, dict[str, object]] = {}
active_call_users: dict[str, str] = {}
access_tokens: dict[str, str] = {}
user_access_tokens: dict[str, str] = {}
call_expiry_task: asyncio.Task | None = None

# Once the media leg is usable (status "connected") the signaling socket may
# legitimately drop (network blip, app backgrounded) without ending the call,
# and the terminal frame may then never arrive. That used to pin the caller
# AND the target in `active_call_users` forever, so every later attempt was
# rejected early with "duplicate_or_busy" before `call_started`. This is the
# connected-idle grace: while BOTH sockets are alive the deadline keeps being
# extended (a real conversation is never auto-cut); if one side's socket stays
# gone, the call is released after this window and both users are freed.
CONNECTED_IDLE_TIMEOUT_MS = 60_000
TERMINAL_EVENT_FCM_AFTER_MS = 30000

_UNSET = object()

terminal_outbox_task: asyncio.Task | None = None
terminal_outbox_lock = asyncio.Lock()


def _mark_active_user(user_id: str, call_id: str, role: str) -> None:
    active_call_users[user_id] = call_id
    print(
        "[CN CALL][ACTIVE_USERS] add role=",
        role,
        "user=",
        user_id,
        "call=",
        call_id,
    )


def _unmark_active_user(user_id: str, call_id: str, reason: str) -> None:
    if active_call_users.get(user_id) != call_id:
        return
    active_call_users.pop(user_id, None)
    print(
        "[CN CALL][ACTIVE_USERS] remove user=",
        user_id,
        "call=",
        call_id,
        "reason=",
        reason,
    )


def _ready_users_to_json(users) -> str:
    """Serialize the media-ready user set stored in call_records."""
    if users is None:
        return "[]"
    if isinstance(users, str):
        try:
            parsed = json.loads(users)
        except (TypeError, ValueError):
            return "[]"
        users = parsed
    if not isinstance(users, (list, tuple, set)):
        return "[]"
    return json.dumps(
        sorted(
            {
                str(user_id).strip()
                for user_id in users
                if str(user_id).strip()
            }
        )
    )


def _ready_users_from_json(value) -> set[str]:
    """Deserialize call_records.media_ready_users without raising."""
    if value is None:
        return set()
    if isinstance(value, (list, tuple, set)):
        raw_values = value
    else:
        try:
            raw_values = json.loads(str(value))
        except (TypeError, ValueError):
            return set()
    if not isinstance(raw_values, (list, tuple, set)):
        return set()
    return {
        str(user_id).strip()
        for user_id in raw_values
        if str(user_id).strip()
    }


def transition_call_state(
    call_id: str,
    new_status: str,
    *,
    negotiation_expires_at=_UNSET,
    connection_expires_at=_UNSET,
    media_ready_users=_UNSET,
) -> int | None:
    """Persist authoritative call state and mirror it in active_calls."""

    db = get_db()
    try:
        if not db.is_postgres:
            db.execute("BEGIN")

        row = db.execute(
            """
            SELECT status,
                   negotiation_expires_at,
                   connection_expires_at,
                   media_ready_users,
                   state_version
            FROM call_records
            WHERE call_id = ?
            FOR UPDATE
            """,
            (call_id,),
        ).fetchone()

        if row is None:
            db.rollback()
            return None

        current_version = int(row["state_version"] or 1)

        next_negotiation = (
            row["negotiation_expires_at"]
            if negotiation_expires_at is _UNSET
            else negotiation_expires_at
        )
        next_connection = (
            row["connection_expires_at"]
            if connection_expires_at is _UNSET
            else connection_expires_at
        )
        next_ready_json = (
            row["media_ready_users"] or "[]"
            if media_ready_users is _UNSET
            else _ready_users_to_json(media_ready_users)
        )

        changed = (
            str(row["status"]) != str(new_status)
            or row["negotiation_expires_at"] != next_negotiation
            or row["connection_expires_at"] != next_connection
            or (row["media_ready_users"] or "[]") != next_ready_json
        )

        version = current_version + 1 if changed else current_version

        if changed:
            db.execute(
                """
                UPDATE call_records
                SET status = ?,
                    negotiation_expires_at = ?,
                    connection_expires_at = ?,
                    media_ready_users = ?,
                    state_version = ?
                WHERE call_id = ?
                """,
                (
                    str(new_status),
                    next_negotiation,
                    next_connection,
                    next_ready_json,
                    version,
                    call_id,
                ),
            )

        db.commit()

    except Exception:
        db.rollback()
        raise
    finally:
        db.close()

    record = active_calls.get(call_id)
    if record is not None:
        record["status"] = str(new_status)
        record["negotiation_expires_at"] = next_negotiation
        record["connection_expires_at"] = next_connection
        record["media_ready_users"] = _ready_users_from_json(next_ready_json)
        record["state_version"] = version

    return version


def _insert_terminal_event_in_db(
    db,
    record: dict[str, object],
    target_id: str,
    message_type: str,
    from_id: str,
    state_version: int,
    reason: str | None = None,
) -> str:
    call_id = str(record["call_id"])
    target_id = str(target_id)
    message_type = str(message_type)
    from_id = str(from_id)

    existing = db.execute(
        """
        SELECT event_id
        FROM durable_terminal_events
        WHERE call_id = ?
          AND target_user_id = ?
          AND event_type = ?
          AND acknowledged_at IS NULL
        LIMIT 1
        """,
        (call_id, target_id, message_type),
    ).fetchone()

    if existing is not None:
        return str(existing["event_id"])

    event_id = uuid.uuid4().hex

    cursor = db.execute(
        """
        INSERT OR IGNORE INTO durable_terminal_events
        (
            event_id,
            call_id,
            source_user_id,
            target_user_id,
            event_type,
            reason,
            created_at,
            acknowledged_at,
            state_version,
            last_attempt_at,
            attempt_count
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, NULL, ?, NULL, 0)
        """,
        (
            event_id,
            call_id,
            from_id,
            target_id,
            message_type,
            reason,
            int(time.time() * 1000),
            state_version,
        ),
    )

    if cursor.rowcount != 1:
        existing = db.execute(
            """
            SELECT event_id
            FROM durable_terminal_events
            WHERE call_id = ?
              AND target_user_id = ?
              AND event_type = ?
              AND acknowledged_at IS NULL
            LIMIT 1
            """,
            (call_id, target_id, message_type),
        ).fetchone()

        if existing is None:
            raise RuntimeError(
                "terminal event insert lost race without existing row"
            )

        return str(existing["event_id"])

    return event_id


def finalize_call_terminal(
    call_id: str,
    new_status: str,
    terminal_events: list[tuple[str, str, str]],
    terminal_reason: str | None = None,
) -> list[str]:
    """
    Atomically:
      1. commits the terminal call state,
      2. records every required durable terminal event,
      3. updates the in-memory record,
      4. releases active-user locks.

    Transport delivery happens only AFTER this function commits.
    Therefore a process crash cannot leave a terminal DB state without
    its durable outbox event.
    """
    record = active_calls.get(call_id)
    if record is None:
        return []

    db = get_db()
    event_ids: list[str] = []

    try:
        db.execute("BEGIN")

        row = db.execute(
            """
            SELECT status,
                   negotiation_expires_at,
                   connection_expires_at,
                   media_ready_users,
                   state_version
            FROM call_records
            WHERE call_id = ?
            FOR UPDATE
            """,
            (call_id,),
        ).fetchone()

        if row is None:
            db.rollback()
            return []

        current_version = int(row["state_version"] or 1)
        next_ready_json = row["media_ready_users"] or "[]"

        changed = (
            str(row["status"]) != str(new_status)
            or row["negotiation_expires_at"] is not None
            or row["connection_expires_at"] is not None
        )

        next_version = current_version + 1 if changed else current_version

        if changed:
            db.execute(
                """
                UPDATE call_records
                SET status = ?,
                    negotiation_expires_at = NULL,
                    connection_expires_at = NULL,
                    media_ready_users = ?,
                    state_version = ?
                WHERE call_id = ?
                """,
                (
                    str(new_status),
                    next_ready_json,
                    next_version,
                    call_id,
                ),
            )

        durable_record = dict(record)
        durable_record["state_version"] = next_version

        for target_id, message_type, from_id in terminal_events:
            event_ids.append(
                _insert_terminal_event_in_db(
                    db,
                    durable_record,
                    target_id,
                    message_type,
                    from_id,
                    next_version,
                    terminal_reason,
                )
            )

        db.commit()

    except Exception:
        db.rollback()
        raise
    finally:
        db.close()

    record = active_calls.pop(call_id, None)
    if record is None:
        return event_ids

    record["status"] = str(new_status)
    record["negotiation_expires_at"] = None
    record["connection_expires_at"] = None
    record["media_ready_users"] = _ready_users_from_json(next_ready_json)
    record["state_version"] = next_version

    caller_id = str(record["caller_id"])
    target_id = str(record["target_id"])

    _unmark_active_user(caller_id, call_id, "terminal")
    _unmark_active_user(target_id, call_id, "terminal")

    print(
        "[CN CALL][ATOMIC TERMINAL]",
        "call_id=", call_id,
        "status=", new_status,
        "state_version=", next_version,
        "event_ids=", event_ids,
    )

    return event_ids






def _mark_terminal_attempt(event_id: str) -> None:
    db = get_db()
    db.execute(
        """
        UPDATE durable_terminal_events
        SET last_attempt_at = ?,
            attempt_count = attempt_count + 1
        WHERE event_id = ?
          AND acknowledged_at IS NULL
        """,
        (int(time.time() * 1000), event_id),
    )
    db.commit()
    db.close()


async def _deliver_terminal_event(event_id: str) -> bool:
    """Attempt delivery of a terminal event exactly once.

    A durable terminal event is created once per terminal interaction.
    Once delivery is attempted (attempt_count becomes 1), the outbox will
    never attempt that same event again. This avoids duplicate notifications
    and repeated retries for the same interaction.
    """
    db = get_db()
    row = db.execute(
        """
        SELECT event_id, call_id, source_user_id, target_user_id,
               event_type, reason, created_at, attempt_count
        FROM durable_terminal_events
        WHERE event_id = ? AND acknowledged_at IS NULL
        """,
        (event_id,),
    ).fetchone()
    db.close()

    if row is None:
        return True

    if int(row["attempt_count"] or 0) > 0:
        return False

    target_id = str(row["target_user_id"])

    # Consume the one allowed delivery attempt before touching the transport.
    _mark_terminal_attempt(event_id)

    target_socket = connections.get(target_id)

    if target_socket is not None:
        payload = {
            "type": str(row["event_type"]),
            "call_id": str(row["call_id"]),
            "target_id": target_id,
            "from_id": str(row["source_user_id"]),
            "event_id": str(row["event_id"]),
            **(
                {"reason": str(row["reason"])}
                if row["reason"]
                else {}
            ),
        }

        try:
            await target_socket.send_json(payload)
            print(
                "[CN CALL][DURABLE TERMINAL WS SENT]",
                row["event_type"],
                "call_id=", row["call_id"],
                "event_id=", event_id,
            )
            return True
        except Exception as exc:
            print(
                "[CN CALL][DURABLE TERMINAL WS ERROR]",
                "call_id=", row["call_id"],
                "event_id=", event_id,
                "error=", exc,
            )
            # The WS may have disappeared between lookup and send(). Reuse
            # the same durable event and fall back to one FCM transport attempt.
            return await send_call_notification_async(
                target_id=target_id,
                caller_id=str(row["source_user_id"]),
                caller_name="مستخدم CN CALL",
                call_id=str(row["call_id"]),
                message_type=str(row["event_type"]),
                event_id=str(row["event_id"]),
                reason=str(row["reason"]) if row["reason"] else None,
            )

    # No live signaling socket: use the single FCM attempt instead.
    return await send_call_notification_async(
        target_id=target_id,
        caller_id=str(row["source_user_id"]),
        caller_name="مستخدم CN CALL",
        call_id=str(row["call_id"]),
        message_type=str(row["event_type"]),
        event_id=str(row["event_id"]),
        reason=str(row["reason"]) if row["reason"] else None,
    )


async def deliver_pending_terminal_events(target_user_id: str | None = None) -> None:
    async with terminal_outbox_lock:
        now = int(time.time() * 1000)
        db = get_db()

        if target_user_id is None:
            rows = db.execute(
                """
                SELECT event_id, last_attempt_at
                FROM durable_terminal_events
                WHERE acknowledged_at IS NULL
                  AND attempt_count = 0
                ORDER BY created_at ASC
                """
            ).fetchall()
        else:
            rows = db.execute(
                """
                SELECT event_id, last_attempt_at
                FROM durable_terminal_events
                WHERE acknowledged_at IS NULL
                  AND target_user_id = ?
                  AND attempt_count = 0
                ORDER BY created_at ASC
                """,
                (target_user_id,),
            ).fetchall()

        db.close()

        for row in rows:
            await _deliver_terminal_event(str(row["event_id"]))


async def _terminal_outbox_loop():
    while True:
        try:
            await deliver_pending_terminal_events()
        except Exception as exc:
            print("[CN CALL][TERMINAL OUTBOX ERROR]", exc)
        await asyncio.sleep(1)


async def acknowledge_terminal_event(
    user_id: str,
    event_id: str,
) -> bool:
    event_id = event_id.strip()

    if not event_id:
        return False

    db = get_db()
    cursor = db.execute(
        """
        UPDATE durable_terminal_events
        SET acknowledged_at = ?
        WHERE event_id = ?
          AND target_user_id = ?
          AND acknowledged_at IS NULL
        """,
        (int(time.time() * 1000), event_id, user_id),
    )
    db.commit()
    db.close()

    acknowledged = cursor.rowcount == 1

    if acknowledged:
        print(
            "[CN CALL][DURABLE TERMINAL ACK]",
            "user=", user_id,
            "event_id=", event_id,
        )

    return acknowledged




async def release_calls_for_user(user_id: str, token: str | None = None):
    call_ids = [
        call_id
        for call_id, record in active_calls.items()
        if user_id in {str(record["caller_id"]), str(record["target_id"])}
        and (
            token is None
            or (
                user_id == str(record["caller_id"])
                and token == record["caller_token"]
            )
            or (
                user_id == str(record["target_id"])
                and token == record["target_token"]
            )
        )
    ]
    for call_id in call_ids:
        record = active_calls.get(call_id)
        if record is None:
            continue
        caller_id = str(record["caller_id"])
        target_id = str(record["target_id"])
        status = str(record["status"])
        peer_id = target_id if user_id == caller_id else caller_id
        message_type = (
            "call_cancelled"
            if user_id == caller_id and status == "ringing"
            else "call_reject" if status == "ringing" else "hangup"
        )
        terminal_status = (
            "cancelled"
            if message_type == "call_cancelled"
            else "rejected"
            if message_type == "call_reject"
            else "ended"
        )

        event_ids = finalize_call_terminal(
            call_id,
            terminal_status,
            [
                (peer_id, message_type, user_id),
            ],
        )

        for event_id in event_ids:
            await _deliver_terminal_event(event_id)


async def expire_active_calls():
    now = int(time.time() * 1000)
    expired_ids = set()

    for call_id, record in active_calls.items():
        status = str(record["status"])

        if status == "ringing":
            if int(record["ring_expires_at"]) <= now:
                expired_ids.add(call_id)
            continue

        if status == "accepted":
            if (
                record["negotiation_expires_at"] is not None
                and int(record["negotiation_expires_at"]) <= now
            ):
                expired_ids.add(call_id)
            continue

        if status == "negotiating":
            if (
                record["connection_expires_at"] is not None
                and int(record["connection_expires_at"]) <= now
            ):
                expired_ids.add(call_id)
            continue

        if status == "connected":
            caller_id = str(record["caller_id"])
            target_id = str(record["target_id"])
            both_online = (
                caller_id in connections
                and target_id in connections
            )
            if both_online:
                # Both endpoints are still reachable: keep the call alive by
                # re-arming the idle deadline every sweep. Only a genuinely
                # missing party lets the countdown reach release.
                transition_call_state(
                    call_id,
                    "connected",
                    connection_expires_at=now + CONNECTED_IDLE_TIMEOUT_MS,
                    media_ready_users=record.get("media_ready_users") or set(),
                )
                continue
            if (
                record["connection_expires_at"] is not None
                and int(record["connection_expires_at"]) <= now
            ):
                expired_ids.add(call_id)
            continue

    for call_id in expired_ids:
        record = active_calls.get(call_id)
        if record is None:
            continue
        caller_id = str(record["caller_id"])
        target_id = str(record["target_id"])
        terminal_status = (
            "missed"
            if str(record["status"]) == "ringing"
            else "timeout"
        )

        terminal_events = (
            [
                # Target: stop any still-ringing Telecom call.
                (target_id, "call_cancelled", caller_id),
                # Caller: close the outgoing Telecom leg even when no
                # call_delivered ever arrived. This prevents an indefinite
                # dialing state when FCM was delayed/unavailable.
                (caller_id, "timeout", target_id),
            ]
            if terminal_status == "missed"
            else [
                (target_id, "hangup", caller_id),
                (caller_id, "hangup", target_id),
            ]
        )

        event_ids = finalize_call_terminal(
            call_id,
            terminal_status,
            terminal_events,
        )

        for event_id in event_ids:
            await _deliver_terminal_event(event_id)

        if terminal_status == "missed":
            missed_fcm_sent = await send_call_notification_async(
                target_id=target_id,
                caller_id=caller_id,
                caller_name=str(record.get("caller_name", "مستخدم CN CALL")),
                call_id=call_id,
                message_type="missed_call",
            )
            print(
                "[CN CALL][MISSED CALL FCM AFTER EXPIRY] "
                f"call_id={call_id} target={target_id} "
                f"sent={missed_fcm_sent}"
            )


async def _call_expiry_loop():
    while True:
        try:
            await expire_active_calls()
        except Exception as exc:
            # Never let a transient FCM/WebSocket failure disable expiry for
            # all following calls.
            print("[CN CALL][CALL EXPIRY ERROR]", exc)
        await asyncio.sleep(1)


@app.on_event("startup")
async def start_call_expiry_loop():
    global call_expiry_task, terminal_outbox_task
    load_fcm_tokens()
    load_access_tokens()
    rebuild_active_calls_from_db()
    call_expiry_task = asyncio.create_task(_call_expiry_loop())
    terminal_outbox_task = asyncio.create_task(_terminal_outbox_loop())


@app.on_event("shutdown")
async def stop_call_expiry_loop():
    global call_expiry_task, terminal_outbox_task
    if call_expiry_task is not None:
        call_expiry_task.cancel()
        call_expiry_task = None
    if terminal_outbox_task is not None:
        terminal_outbox_task.cancel()
        terminal_outbox_task = None

FCM_TOKENS: dict[str, str] = {}

firebase_key = BASE_DIR / "secrets" / "firebase-service-account.json"
firebase_json = os.getenv("FIREBASE_SERVICE_ACCOUNT_JSON")

if not firebase_admin._apps:
    if firebase_json:
        cred = credentials.Certificate(
            __import__("json").loads(firebase_json)
        )
        firebase_admin.initialize_app(cred)
    elif firebase_key.exists():
        cred = credentials.Certificate(str(firebase_key))
        firebase_admin.initialize_app(cred)



# ============================================================
# DATABASE
# ============================================================

class DatabaseHandle:
    """Small compatibility wrapper for PostgreSQL and temporary SQLite fallback."""

    def __init__(self):
        self.is_postgres = bool(DATABASE_URL)

        if self.is_postgres:
            self._db = psycopg.connect(
                DATABASE_URL,
                row_factory=dict_row,
                connect_timeout=10,
            )
        else:
            self._db = sqlite3.connect(DB_PATH)
            self._db.row_factory = sqlite3.Row

    def execute(self, sql, params=None):
        if self.is_postgres:
            sql = sql.replace("?", "%s")
            sql = re.sub(
                r"\bCURRENT_TIMESTAMP\b",
                "CURRENT_TIMESTAMP::text",
                sql,
            )
            if re.match(
                r"^\s*INSERT\s+OR\s+IGNORE\s+INTO\b",
                sql,
                flags=re.IGNORECASE,
            ):
                sql = re.sub(
                    r"^\s*INSERT\s+OR\s+IGNORE\s+INTO\b",
                    "INSERT INTO",
                    sql,
                    count=1,
                    flags=re.IGNORECASE,
                ).rstrip()
                sql += "\nON CONFLICT DO NOTHING"

        return self._db.execute(sql, params or ())

    def commit(self):
        self._db.commit()

    def rollback(self):
        self._db.rollback()

    def close(self):
        self._db.close()


def get_db():
    return DatabaseHandle()


def init_db():
    db = get_db()

    db.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            user_id TEXT PRIMARY KEY,
            username TEXT NOT NULL,
            password_hash TEXT NOT NULL,
            password_salt TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    db.execute(
        """
        CREATE TABLE IF NOT EXISTS fcm_tokens (
            user_id TEXT PRIMARY KEY,
            token TEXT NOT NULL,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    db.execute(
        """
        CREATE TABLE IF NOT EXISTS access_tokens (
            token TEXT PRIMARY KEY,
            user_id TEXT NOT NULL UNIQUE,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    db.execute(
        """
        CREATE TABLE IF NOT EXISTS call_records (
            call_id TEXT PRIMARY KEY,
            caller_id TEXT NOT NULL,
            target_id TEXT NOT NULL,
            caller_name TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            expires_at INTEGER NOT NULL,
            status TEXT NOT NULL,
            negotiation_expires_at INTEGER,
            connection_expires_at INTEGER,
            media_ready_users TEXT NOT NULL DEFAULT '[]',
            state_version INTEGER NOT NULL DEFAULT 1
        )
        """
    )

    db.execute(
        """
        CREATE TABLE IF NOT EXISTS durable_terminal_events (
            event_id TEXT PRIMARY KEY,
            call_id TEXT NOT NULL,
            source_user_id TEXT NOT NULL,
            target_user_id TEXT NOT NULL,
            event_type TEXT NOT NULL,
            reason TEXT,
            created_at INTEGER NOT NULL,
            acknowledged_at INTEGER,
            state_version INTEGER NOT NULL DEFAULT 1,
            last_attempt_at INTEGER,
            attempt_count INTEGER NOT NULL DEFAULT 0
        )
        """
    )

    db.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_durable_terminal_pending
        ON durable_terminal_events (
            call_id,
            target_user_id,
            event_type
        )
        WHERE acknowledged_at IS NULL
        """
    )

    # Safe migration for pre-existing schemas.
    # PostgreSQL supports ADD COLUMN IF NOT EXISTS, so each check is
    # idempotent and cannot abort the whole transaction when the column
    # already exists. SQLite keeps the legacy try/except path because its
    # ALTER TABLE syntax differs.
    if db.is_postgres:
        for statement in (
            "ALTER TABLE call_records ADD COLUMN IF NOT EXISTS negotiation_expires_at INTEGER",
            "ALTER TABLE call_records ADD COLUMN IF NOT EXISTS connection_expires_at INTEGER",
            "ALTER TABLE call_records ADD COLUMN IF NOT EXISTS state_version INTEGER NOT NULL DEFAULT 1",
            "ALTER TABLE call_records ADD COLUMN IF NOT EXISTS media_ready_users TEXT NOT NULL DEFAULT '[]'",
            "ALTER TABLE durable_terminal_events ADD COLUMN IF NOT EXISTS reason TEXT",
            "ALTER TABLE durable_terminal_events ADD COLUMN IF NOT EXISTS last_attempt_at INTEGER",
            "ALTER TABLE durable_terminal_events ADD COLUMN IF NOT EXISTS attempt_count INTEGER NOT NULL DEFAULT 0",
        ):
            db.execute(statement)
    else:
        try:
            db.execute("ALTER TABLE call_records ADD COLUMN negotiation_expires_at INTEGER")
        except Exception:
            pass
        try:
            db.execute("ALTER TABLE call_records ADD COLUMN connection_expires_at INTEGER")
        except Exception:
            pass
        try:
            db.execute("ALTER TABLE call_records ADD COLUMN state_version INTEGER NOT NULL DEFAULT 1")
        except Exception:
            pass
        try:
            db.execute("ALTER TABLE call_records ADD COLUMN media_ready_users TEXT NOT NULL DEFAULT '[]'")
        except Exception:
            pass
        try:
            db.execute("ALTER TABLE durable_terminal_events ADD COLUMN reason TEXT")
        except Exception:
            pass
        try:
            db.execute("ALTER TABLE durable_terminal_events ADD COLUMN last_attempt_at INTEGER")
        except Exception:
            pass
        try:
            db.execute("ALTER TABLE durable_terminal_events ADD COLUMN attempt_count INTEGER NOT NULL DEFAULT 0")
        except Exception:
            pass

    db.commit()
    db.close()


init_db()


def rebuild_active_calls_from_db():
    """Restores active calls and atomically finalizes already-expired calls."""
    db = get_db()
    now = int(time.time() * 1000)

    try:
        rows = db.execute(
            """
            SELECT call_id, caller_id, target_id, caller_name, created_at,
                   expires_at, status, negotiation_expires_at,
                   connection_expires_at, media_ready_users, state_version
            FROM call_records
            WHERE status IN ('ringing', 'accepted', 'negotiating', 'connected')
            """
        ).fetchall()



        restored = 0
        recovered_terminals = 0
        restored_records: list[dict[str, object]] = []

        for row in rows:
            call_id = str(row["call_id"])
            caller_id = str(row["caller_id"])
            target_id = str(row["target_id"])
            status = str(row["status"])
            expires_at = row["expires_at"]
            negotiation_expires_at = row["negotiation_expires_at"]
            connection_expires_at = row["connection_expires_at"]
            current_version = int(row["state_version"] or 1)
            next_version = current_version

            expired = (
                status == "ringing"
                and expires_at
                and int(expires_at) <= now
            ) or (
                status == "accepted"
                and negotiation_expires_at
                and int(negotiation_expires_at) <= now
            ) or (
                status in ("negotiating", "connected")
                and connection_expires_at
                and int(connection_expires_at) <= now
            )

            if expired:
                terminal_status = (
                    "missed"
                    if status == "ringing"
                    else "timeout"
                )

                next_version = current_version + 1

                db.execute(
                    """
                    UPDATE call_records
                    SET status = ?,
                        negotiation_expires_at = NULL,
                        connection_expires_at = NULL,
                        state_version = ?
                    WHERE call_id = ?
                    """,
                    (
                        terminal_status,
                        next_version,
                        call_id,
                    ),
                )

                durable_record = {
                    "call_id": call_id,
                    "state_version": next_version,
                }

                if terminal_status == "missed":
                    terminal_events = [
                        (
                            target_id,
                            "call_cancelled",
                            caller_id,
                        ),
                    ]
                else:
                    terminal_events = [
                        (
                            target_id,
                            "hangup",
                            caller_id,
                        ),
                        (
                            caller_id,
                            "hangup",
                            target_id,
                        ),
                    ]

                for event_target, event_type, event_source in terminal_events:
                    event_id = _insert_terminal_event_in_db(
                        db,
                        durable_record,
                        event_target,
                        event_type,
                        event_source,
                        next_version,
                    )
                    print(
                        "[CN CALL][DB RECOVERY TERMINAL]",
                        "call_id=", call_id,
                        "status=", terminal_status,
                        "event_id=", event_id,
                        "state_version=", next_version,
                    )

                recovered_terminals += 1
                continue

            restored_records.append({
                "call_id": call_id,
                "caller_id": caller_id,
                "target_id": target_id,
                "caller_name": row["caller_name"],
                "status": status,
                "created_at": row["created_at"],
                "ring_expires_at": expires_at,
                "negotiation_expires_at": negotiation_expires_at,
                "connection_expires_at": connection_expires_at,
                "caller_token": user_access_tokens.get(caller_id),
                "target_token": user_access_tokens.get(target_id),
                "media_ready_users": _ready_users_from_json(
                    row["media_ready_users"]
                ),
                "state_version": current_version,
            })
            restored += 1

        db.commit()

        for restored_record in restored_records:
            restored_call_id = str(restored_record["call_id"])
            active_calls[restored_call_id] = restored_record

            _mark_active_user(
                str(restored_record["caller_id"]),
                restored_call_id,
                "caller",
            )
            _mark_active_user(
                str(restored_record["target_id"]),
                restored_call_id,
                "callee",
            )

    except Exception:
        db.rollback()
        raise

    finally:
        db.close()

    print(
        "[CN CALL][DB RECOVERY]",
        "restored_active=", restored,
        "finalized_expired=", recovered_terminals,
    )

def load_fcm_tokens():
    db = get_db()
    rows = db.execute(
        "SELECT user_id, token FROM fcm_tokens"
    ).fetchall()
    db.close()

    for row in rows:
        FCM_TOKENS[row["user_id"]] = row["token"]


def load_access_tokens():
    db = get_db()
    rows = db.execute(
        "SELECT token, user_id FROM access_tokens"
    ).fetchall()
    db.close()

    for row in rows:
        access_tokens[row["token"]] = row["user_id"]
        user_access_tokens[row["user_id"]] = row["token"]


load_fcm_tokens()
load_access_tokens()


# ============================================================
# PASSWORD SECURITY
# ============================================================

def hash_password(password: str, salt: bytes | None = None):
    if salt is None:
        salt = os.urandom(16)

    password_hash = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        salt,
        200_000,
    )

    return (
        password_hash.hex(),
        salt.hex(),
    )


def verify_password(
    password: str,
    password_hash: str,
    password_salt: str,
):
    try:
        salt = bytes.fromhex(password_salt)
    except ValueError:
        return False

    calculated, _ = hash_password(password, salt)

    return hmac.compare_digest(
        calculated,
        password_hash,
    )


# ============================================================
# MODELS
# ============================================================

class RegisterRequest(BaseModel):
    user_id: str
    username: str
    password: str


class LoginRequest(BaseModel):
    user_id: str
    password: str


class FcmTokenRequest(BaseModel):
    token: str


def authenticated_user(authorization: str | None) -> str | None:
    if not authorization or not authorization.startswith("Bearer "):
        return None

    token = authorization[7:].strip()
    return access_tokens.get(token)


def refresh_active_call_token(user_id: str, token: str) -> int:
    """Refresh the signaling credential cached by any active call for a user.

    Login intentionally rotates the user's access token and closes the old
    WebSocket. The active call itself must survive that session handoff, so
    its in-memory caller/target token is refreshed to the newly issued token.
    The durable call record does not store credentials; startup recovery reads
    the current user_access_tokens map instead.
    """
    updated = 0

    for record in active_calls.values():
        caller_id = str(record.get("caller_id", ""))
        target_id = str(record.get("target_id", ""))

        if caller_id == user_id:
            record["caller_token"] = token
            updated += 1

        if target_id == user_id:
            record["target_token"] = token
            updated += 1

    if updated:
        print(
            "[CN CALL][LOGIN TOKEN REFRESH]",
            "user_id=", user_id,
            "active_call_token_slots_updated=", updated,
        )

    return updated


def issue_access_token(user_id: str) -> str:
    old_token = user_access_tokens.get(user_id)
    if old_token:
        access_tokens.pop(old_token, None)

    token = uuid.uuid4().hex + uuid.uuid4().hex
    access_tokens[token] = user_id
    user_access_tokens[user_id] = token

    db = get_db()
    db.execute(
        "DELETE FROM access_tokens WHERE user_id = ?",
        (user_id,),
    )
    db.execute(
        "INSERT INTO access_tokens (token, user_id) VALUES (?, ?)",
        (token, user_id),
    )
    db.commit()
    db.close()

    return token



# ============================================================
# FCM TOKEN
# ============================================================

@app.post("/fcm-token")
async def save_fcm_token(
    request: FcmTokenRequest,
    authorization: str | None = Header(default=None),
):
    user_id = authenticated_user(authorization)
    token = request.token.strip()

    if user_id is None or not token:
        raise HTTPException(status_code=401, detail="غير مصرح")

    FCM_TOKENS[user_id] = token

    db = get_db()
    db.execute(
        """
        INSERT INTO fcm_tokens (user_id, token, updated_at)
        VALUES (?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(user_id)
        DO UPDATE SET
            token=excluded.token,
            updated_at=CURRENT_TIMESTAMP
        """,
        (user_id, token),
    )
    db.commit()
    db.close()

    return {
        "success": True,
        "message": "تم حفظ FCM Token",
    }


# ============================================================
# BASIC
# ============================================================

@app.get("/")
async def root():
    return {
        "app": "CN CALL",
        "status": "online",
    }




@app.get("/fcm-debug")
def fcm_debug(authorization: str | None = Header(default=None)):
    if authenticated_user(authorization) is None:
        raise HTTPException(status_code=401, detail="غير مصرح")

    return {
        "success": True,
        "count": len(FCM_TOKENS),
    }

@app.get("/health")
async def health():
    return {
        "status": "ok",
        "users": len(connections),
    }


# ============================================================
# REGISTER
# ============================================================

@app.post("/register")
async def register(request: RegisterRequest):
    user_id = request.user_id.strip()
    username = request.username.strip()
    password = request.password

    if not user_id:
        return {
            "success": False,
            "message": "ID المستخدم مطلوب",
        }

    if not re.fullmatch(r"\d+", user_id):
        return {
            "success": False,
            "message": "ID المستخدم يجب أن يكون أرقامًا فقط",
        }

    if len(username) < 3:
        return {
            "success": False,
            "message": "اسم المستخدم يجب أن يكون 3 أحرف على الأقل",
        }

    if len(password) < 6:
        return {
            "success": False,
            "message": "كلمة المرور يجب أن تكون 6 أحرف على الأقل",
        }

    db = get_db()

    existing = db.execute(
        "SELECT user_id FROM users WHERE user_id = ?",
        (user_id,),
    ).fetchone()

    if existing is not None:
        db.close()

        return {
            "success": False,
            "message": "ID المستخدم مستخدم بالفعل",
        }

    password_hash, password_salt = hash_password(password)

    db.execute(
        """
        INSERT INTO users (
            user_id,
            username,
            password_hash,
            password_salt
        )
        VALUES (?, ?, ?, ?)
        """,
        (
            user_id,
            username,
            password_hash,
            password_salt,
        ),
    )

    db.commit()
    db.close()

    return {
        "success": True,
        "message": "تم إنشاء الحساب بنجاح",
        "user": {
            "user_id": user_id,
            "username": username,
        },
    }


# ============================================================
# LOGIN
# ============================================================

@app.post("/login")
async def login(request: LoginRequest):
    user_id = request.user_id.strip()
    password = request.password

    db = get_db()

    user = db.execute(
        """
        SELECT
            user_id,
            username,
            password_hash,
            password_salt
        FROM users
        WHERE user_id = ?
        """,
        (user_id,),
    ).fetchone()

    db.close()

    if user is None:
        return {
            "success": False,
            "message": "ID المستخدم أو كلمة المرور غير صحيحة",
        }

    if not verify_password(
        password,
        user["password_hash"],
        user["password_salt"],
    ):
        return {
            "success": False,
            "message": "ID المستخدم أو كلمة المرور غير صحيحة",
        }

    token = issue_access_token(user["user_id"])
    refresh_active_call_token(user["user_id"], token)

    old_connection = connections.get(user["user_id"])
    if old_connection is not None:
        try:
            await old_connection.close(code=4001)
        except Exception:
            pass
        if connections.get(user["user_id"]) is old_connection:
            del connections[user["user_id"]]

    return {
        "success": True,
        "message": "تم تسجيل الدخول بنجاح",
        "user": {
            "user_id": user["user_id"],
            "username": user["username"],
        },
        "access_token": token,
    }


# ============================================================
# USER LOOKUP
# ============================================================

@app.get("/users/{user_id}")
async def get_user(
    user_id: str,
    authorization: str | None = Header(default=None),
):
    if authenticated_user(authorization) is None:
        raise HTTPException(status_code=401, detail="غير مصرح")
    db = get_db()

    user = db.execute(
        """
        SELECT user_id, username, created_at
        FROM users
        WHERE user_id = ?
        """,
        (user_id.strip(),),
    ).fetchone()

    db.close()

    if user is None:
        return {
            "success": False,
            "message": "المستخدم غير موجود",
        }

    return {
        "success": True,
        "user": {
            "user_id": user["user_id"],
            "username": user["username"],
            "online": user["user_id"] in connections,
        },
    }


@app.get("/calls/missed/{user_id}")
async def get_missed_calls(
    user_id: str,
    authorization: str | None = Header(default=None),
):
    authenticated_id = authenticated_user(authorization)
    if authenticated_id != user_id.strip():
        raise HTTPException(status_code=403, detail="?????? ???????")

    user_id = user_id.strip()
    now = int(time.time() * 1000)

    # First materialize expired ringing call ids, then perform the
    # authoritative transition through the same state machine used by
    # WebSocket/expiry paths.
    db = get_db()
    expired_rows = db.execute(
        """
        SELECT call_id
        FROM call_records
        WHERE target_id = ?
          AND status = 'ringing'
          AND expires_at <= ?
        """,
        (user_id, now),
    ).fetchall()
    db.close()

    for row in expired_rows:
        transition_call_state(
            str(row["call_id"]),
            "missed",
            negotiation_expires_at=None,
            connection_expires_at=None,
        )

    db = get_db()
    rows = db.execute(
        """
        SELECT call_id, caller_id, caller_name, created_at
        FROM call_records
        WHERE target_id = ? AND status = 'missed'
        ORDER BY created_at DESC
        """,
        (user_id,),
    ).fetchall()
    db.close()

    # Mark returned missed records as delivered through the same transition
    # helper so state_version remains authoritative.
    for row in rows:
        transition_call_state(
            str(row["call_id"]),
            "missed_delivered",
            negotiation_expires_at=None,
            connection_expires_at=None,
        )


    return {
        "success": True,
        "calls": [dict(row) for row in rows],
    }



# ============================================================
# FCM NOTIFICATIONS
# ============================================================

def send_call_notification(
    target_id: str,
    caller_id: str,
    caller_name: str,
    call_id: str,
    message_type: str = "incoming_call",
    event_id: str | None = None,
    reason: str | None = None,
) -> bool:
    token = FCM_TOKENS.get(target_id)

    print("FCM TARGET:", target_id)
    print("FCM TOKEN FOUND:", bool(token))

    if not token:
        db = get_db()
        row = db.execute(
            "SELECT token FROM fcm_tokens WHERE user_id = ?",
            (target_id,),
        ).fetchone()
        db.close()

        if row:
            token = row["token"]
            FCM_TOKENS[target_id] = token

    if not token:
        print(
            "[CN CALL][FCM FAILED] "
            f"call_id={call_id} target={target_id} reason=no_token"
        )
        return False

    if not firebase_admin._apps:
        print(
            "[CN CALL][FCM FAILED] "
            f"call_id={call_id} target={target_id} reason=firebase_unavailable"
        )
        return False

    try:
        message = messaging.Message(
            token=token,
            data={
                "type": message_type,
                "call_id": call_id,
                "caller_id": caller_id,
                "caller_name": caller_name,
                "target_id": target_id,
                **(
                    {"event_id": event_id}
                    if event_id
                    else {}
                ),
                **(
                    {"reason": reason}
                    if reason
                    else {}
                ),
            },
            android=messaging.AndroidConfig(
                priority="high",
                collapse_key=(
                    f"missed-call-{call_id}"
                    if message_type == "missed_call"
                    else f"call-{call_id}"
                ),
                ttl=timedelta(
                    seconds=(28 * 24 * 60 * 60)
                    if message_type == "missed_call"
                    else 95
                ),
            ),
        )

        response = messaging.send(message)
        print('FCM SENT:', response)
        return True

    except Exception as e:
        print(f"FCM send error: {e}")
        print(
            "[CN CALL][FCM FAILED] "
            f"call_id={call_id} target={target_id} error={e}"
        )
        return False


async def send_call_notification_async(
    *,
    target_id: str,
    caller_id: str,
    caller_name: str,
    call_id: str,
    message_type: str = "incoming_call",
    event_id: str | None = None,
    reason: str | None = None,
) -> bool:
    """Run the blocking Firebase Admin SDK send outside FastAPI's event loop."""
    return await asyncio.to_thread(
        send_call_notification,
        target_id=target_id,
        caller_id=caller_id,
        caller_name=caller_name,
        call_id=call_id,
        message_type=message_type,
        event_id=event_id,
        reason=reason,
    )


# ============================================================
# WEBSOCKET / CALLS
# ============================================================


@app.get("/turn-credentials")
async def get_turn_credentials(
    authorization: str | None = Header(default=None),
):
    if authenticated_user(authorization) is None:
        raise HTTPException(status_code=401, detail="غير مصرح")

    turn_url = os.getenv("TURN_URL", "").strip()
    turn_username = os.getenv("TURN_USERNAME", "").strip()
    turn_password = os.getenv("TURN_PASSWORD", "").strip()

    if not turn_url or not turn_username or not turn_password:
        return {
            "success": False,
            "message": "TURN credentials are not configured",
        }

    base_url = turn_url
    if not base_url.startswith("turn:"):
        base_url = f"turn:{base_url}"

    return {
        "success": True,
        "iceServers": [
            {
                "urls": [
                    f"{base_url}?transport=udp",
                    f"{base_url}?transport=tcp",
                ],
                "username": turn_username,
                "credential": turn_password,
            }
        ],
    }


@app.get("/livekit/token")
def _generate_livekit_token_for_user(user_id: str, call_id: str) -> dict[str, str] | None:
    """Generates LiveKit join credentials for a given participant with 10-min TTL.

    Secrets and tokens are never logged. Returns dict with url, token, room or
    None if LiveKit is unconfigured.
    """
    livekit_url = os.getenv("LIVEKIT_URL")
    livekit_key = os.getenv("LIVEKIT_API_KEY")
    livekit_secret = os.getenv("LIVEKIT_API_SECRET")

    if not livekit_url or not livekit_key or not livekit_secret:
        return None

    room_name = f"call-{call_id}"

    jwt = (
        api.AccessToken(
            livekit_key,
            livekit_secret,
        )
        .with_identity(user_id)
        .with_ttl(timedelta(minutes=10))
        .with_grants(
            api.VideoGrants(
                room_join=True,
                room=room_name,
            )
        )
    )

    return {
        "url": livekit_url,
        "token": jwt.to_jwt(),
        "room": room_name,
    }


def livekit_token(
    user_id: str,
    call_id: str,
    authorization: str = Header(None),
):
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="missing token")

    token = authorization.replace("Bearer ", "", 1).strip()

    if access_tokens.get(token) != user_id:
        raise HTTPException(status_code=401, detail="invalid session")

    # A token must never create or revive a room for an ended/unknown call.
    # Both endpoints can request their own token only while the server owns
    # this exact active call.
    record = active_calls.get(call_id.strip())
    if (
        record is None
        or user_id not in {str(record["caller_id"]), str(record["target_id"])}
        or str(record["status"]) not in {"accepted", "negotiating", "connected"}
    ):
        raise HTTPException(status_code=409, detail="unknown_or_ended_call")

    creds = _generate_livekit_token_for_user(user_id, call_id.strip())
    if not creds:
        raise HTTPException(status_code=500, detail="livekit not configured")

    return {
        "success": True,
        "url": creds["url"],
        "token": creds["token"],
        "room": creds["room"],
    }


@app.websocket("/ws/{user_id}")
async def websocket_endpoint(
    websocket: WebSocket,
    user_id: str,
):
    if CN_CALL_MIGRATION_MODE:
        await websocket.accept()
        await websocket.send_json({
            "type": "server_maintenance",
        })
        await websocket.close(code=1013)
        return

    token = websocket.query_params.get("token", "").strip()
    if access_tokens.get(token) != user_id:
        await websocket.accept()
        await websocket.send_json({
            "type": "session_invalid",
            "reason": "invalid_session",
        })
        await websocket.close(code=1008)
        return

    if user_id in connections:
        # Detach the old socket before awaiting close. Its `finally` handler
        # must not see itself as the active connection and release a call that
        # belongs to the replacement WebSocket.
        old_socket = connections.pop(user_id)
        print("[CN CALL][SOCKET REPLACE] user_id=", user_id)
        try:
            await old_socket.close()
        except Exception:
            pass

    await websocket.accept()

    connections[user_id] = websocket
    print("[CN CALL][SOCKET READY] user_id=", user_id)

    # Reconcile active calls on WebSocket reconnect
    async def reconcile_user_calls_on_connect():
        for active_call_id, rec in list(active_calls.items()):
            caller_id = str(rec["caller_id"])
            target_id = str(rec["target_id"])
            status = str(rec["status"])

            if user_id in (caller_id, target_id):
                peer_id = target_id if user_id == caller_id else caller_id
                print(f"[CN CALL][RECONCILE] user={user_id} call_id={active_call_id} status={status}")

                if status == "ringing" and user_id == target_id:
                    # Replay incoming call for callee
                    await websocket.send_json({
                        "type": "call",
                        "call_id": active_call_id,
                        "target_id": target_id,
                        "from_id": caller_id,
                        "caller_name": str(rec.get("caller_name", "مستخدم CN CALL")),
                        "ring_expires_at": rec.get("ring_expires_at"),
                    })
                elif status in ("accepted", "negotiating", "connected"):
                    # Replay accepted credentials for reconnecting participant
                    user_creds = _generate_livekit_token_for_user(user_id, active_call_id)
                    replay_type = "call_accept_ack" if user_id == target_id else "call_accept"
                    payload = {
                        "type": replay_type,
                        "call_id": active_call_id,
                        "target_id": peer_id if replay_type == "call_accept_ack" else user_id,
                        "from_id": peer_id,
                        "replayed": True,
                    }
                    if user_creds:
                        payload.update({
                            "livekit_url": user_creds["url"],
                            "livekit_token": user_creds["token"],
                            "room": user_creds["room"],
                        })
                    await websocket.send_json(payload)

        # Missed calls remain durable in call_records. Replay them when an
        # authenticated user reconnects so the native layer can notify them
        # even when the original FCM delivery was unavailable.
        missed_db = get_db()
        missed_rows = missed_db.execute(
            """
            SELECT call_id, caller_id, caller_name, created_at
            FROM call_records
            WHERE target_id = ?
              AND status = 'missed'
            ORDER BY created_at DESC
            LIMIT 50
            """,
            (user_id,),
        ).fetchall()
        missed_db.close()

        for missed_row in missed_rows:
            await websocket.send_json({
                "type": "missed_call",
                "call_id": str(missed_row["call_id"]),
                "target_id": user_id,
                "from_id": str(missed_row["caller_id"]),
                "caller_id": str(missed_row["caller_id"]),
                "caller_name": str(
                    missed_row["caller_name"] or "مستخدم CN CALL"
                ),
                "created_at": missed_row["created_at"],
            })

    try:
        await websocket.send_json({
            "type": "connected",
            "user_id": user_id,
        })

        await reconcile_user_calls_on_connect()
        await deliver_pending_terminal_events(user_id)

        while True:
            message = await websocket.receive_json()

            # A replacement connection uses the same token. Ignore anything
            # the closing socket manages to receive after it has been detached
            # so it cannot mutate or release the new connection's call.
            if connections.get(user_id) is not websocket:
                await websocket.close()
                return

            if access_tokens.get(token) != user_id:
                await release_calls_for_user(user_id, token)
                await websocket.send_json({
                    "type": "session_invalid",
                    "reason": "session_revoked",
                })
                await websocket.close(code=1008)
                return

            message_type = str(message.get("type", "")).strip()
            call_id = str(message.get("call_id", "")).strip()
            target_id = str(message.get("target_id", "")).strip()
            print("[CN CALL][CALL MESSAGE] type=", message_type, "call_id=", call_id, "from=", user_id, "target=", target_id)

            await expire_active_calls()

            if message_type == "terminal_ack":
                event_id = str(message.get("event_id", "")).strip()
                await acknowledge_terminal_event(user_id, event_id)
                continue

            if message_type == "call":
                if not call_id:
                    call_id = str(uuid.uuid4())

                if not target_id or target_id == user_id:
                    await websocket.send_json({
                        "type": "call_reject",
                        "call_id": call_id,
                        "target_id": user_id,
                        "reason": "self_call_not_allowed",
                    })
                    print(
                        "[CN CALL][CALL REJECTED] "
                        f"call_id={call_id} from={user_id} target={target_id} reason=self_call_not_allowed"
                    )
                    continue

                db = get_db()
                target_user = db.execute(
                    "SELECT user_id FROM users WHERE user_id = ?",
                    (target_id,),
                ).fetchone()
                existing = db.execute(
                    "SELECT status FROM call_records WHERE call_id = ?",
                    (call_id,),
                ).fetchone()
                db.close()

                if target_user is None:
                    await websocket.send_json({
                        "type": "call_reject",
                        "call_id": call_id,
                        "target_id": target_id,
                        "reason": "user_not_found",
                    })
                    print(
                        "[CN CALL][CALL REJECTED] "
                        f"call_id={call_id} from={user_id} target={target_id} reason=user_not_found"
                    )
                    continue

                if existing is not None:
                    await websocket.send_json({
                        "type": "call_reject",
                        "call_id": call_id,
                        "target_id": user_id,
                        "reason": "duplicate_or_busy",
                    })
                    print(
                        "[CN CALL][CALL REJECTED] "
                        f"call_id={call_id} from={user_id} target={target_id} reason=duplicate_or_busy existing_call_id={active_call_users.get(target_id) if target_id in active_call_users else 'none'}"
                    )
                    continue

                # Keep the normal busy protection for calls that are
                # actually in progress. However, an offline target may
                # have a stale FCM ringing call. Replace that stale
                # ringing call instead of forcing the caller to wait
                # for the 90-second ring timeout.
                if target_id in active_call_users:
                    previous_call_id = active_call_users.get(target_id)
                    previous_record = (
                        active_calls.get(previous_call_id)
                        if previous_call_id
                        else None
                    )

                    if (
                        previous_record is not None
                        and previous_record.get("status") == "ringing"
                        and target_id not in connections
                    ):
                        await send_call_notification_async(
                            target_id=target_id,
                            caller_id=str(previous_record["caller_id"]),
                            caller_name=str(
                                previous_record.get(
                                    "caller_name",
                                    "مستخدم CN CALL",
                                )
                            ),
                            call_id=str(previous_record["call_id"]),
                            message_type="call_cancelled",
                        )
                        previous_caller_id = str(previous_record["caller_id"])
                        previous_target_id = str(previous_record["target_id"])

                        event_ids = finalize_call_terminal(
                            str(previous_call_id),
                            "missed",
                            [
                                (
                                    previous_caller_id,
                                    "call_cancelled",
                                    previous_target_id,
                                ),
                            ],
                        )

                        for event_id in event_ids:
                            await _deliver_terminal_event(event_id)
                    else:
                        await websocket.send_json({
                            "type": "call_reject",
                            "call_id": call_id,
                            "target_id": target_id,
                            "reason": "busy",
                        })
                        print(
                            "[CN CALL][CALL REJECTED] "
                            f"call_id={call_id} from={user_id} target={target_id} reason=busy"
                        )
                        continue

                if user_id in active_call_users:
                    await websocket.send_json({
                        "type": "call_reject",
                        "call_id": call_id,
                        "target_id": target_id,
                        "reason": "busy",
                    })
                    print(
                        "[CN CALL][CALL REJECTED] "
                        f"call_id={call_id} from={user_id} target={target_id} reason=busy"
                    )
                    continue

                target_socket = connections.get(target_id)

                # A missing WebSocket is NOT proof that the recipient is offline.
                # Keep one authoritative ringing call alive and use FCM as the
                # wake-up/fallback channel. A real delivery is established only
                # by the recipient's call_delivered frame after Telecom enters
                # RINGING. The ring expiry below remains a safety timeout, not an
                # online/offline detector.
                ring_expires_at = message.get("ring_expires_at")
                if not ring_expires_at:
                    ring_expires_at = int(time.time() * 1000) + 90000
                try:
                    ring_expires_at = int(ring_expires_at)
                except (TypeError, ValueError):
                    ring_expires_at = int(time.time() * 1000) + 90000

                created_at = int(time.time() * 1000)
                db = get_db()
                db.execute(
                    """
                    INSERT INTO call_records
                    (call_id, caller_id, target_id, caller_name,
                     created_at, expires_at, status, media_ready_users,
                     state_version)
                    VALUES (?, ?, ?, ?, ?, ?, 'ringing', '[]', 1)
                    """,
                    (
                        call_id,
                        user_id,
                        target_id,
                        str(message.get("caller_name", "مستخدم CN CALL")),
                        created_at,
                        ring_expires_at,
                    ),
                )
                db.commit()
                db.close()

                active_calls[call_id] = {
                    "call_id": call_id,
                    "caller_id": user_id,
                    "target_id": target_id,
                    "status": "ringing",
                    "created_at": created_at,
                    "ring_expires_at": ring_expires_at,
                    "negotiation_expires_at": None,
                    "connection_expires_at": None,
                    "caller_token": token,
                    "target_token": user_access_tokens.get(target_id),
                    "media_ready_users": set(),
                    "state_version": 1,
                    "delivery_confirmed": False,
                }
                _mark_active_user(user_id, call_id, "caller")
                _mark_active_user(target_id, call_id, "callee")

                # call_started only means that the server created the ringing call.
                # It is deliberately NOT a delivery confirmation and must not start
                # caller ringback.
                await websocket.send_json({
                    "type": "call_started",
                    "call_id": call_id,
                    "target_id": target_id,
                    "from_id": user_id,
                    "ring_expires_at": ring_expires_at,
                    "target_online": target_socket is not None,
                })

                delivered = False
                if target_socket is not None:
                    print(
                        "[CN CALL][CALL INITIAL WS ATTEMPT] "
                        f"call_id={call_id} target={target_id} socket_present=True"
                    )
                    try:
                        await target_socket.send_json({
                            **message,
                            "call_id": call_id,
                            "ring_expires_at": ring_expires_at,
                            "from_id": user_id,
                        })
                        delivered = True
                        print(
                            "[CN CALL][CALL INITIAL WS SENT] "
                            f"call_id={call_id} target={target_id}"
                        )
                    except Exception as exc:
                        print(
                            "[CN CALL][CALL INITIAL WS FAILED] "
                            f"call_id={call_id} target={target_id} error={exc}"
                        )
                        # Do not leave a known-dead socket in the presence map.
                        if connections.get(target_id) is target_socket:
                            connections.pop(target_id, None)
                        try:
                            await target_socket.close()
                        except Exception:
                            pass

                if delivered:
                    # The WebSocket delivery remains the primary low-latency path.
                    # Also send one FCM copy for the same call_id. Android's native
                    # presentation ticket makes the duplicate transport idempotent,
                    # so an FCM wake-up cannot create a second Telecom call.
                    print(
                        "[CN CALL][CALL DELIVERY MODE] "
                        f"call_id={call_id} mode=websocket"
                    )

                    fcm_sent = await send_call_notification_async(
                        target_id=target_id,
                        caller_id=user_id,
                        caller_name=str(
                            message.get("caller_name", "مستخدم CN CALL")
                        ),
                        call_id=call_id,
                        message_type="incoming_call",
                    )
                    print(
                        "[CN CALL][PARALLEL FCM] "
                        f"call_id={call_id} target={target_id} "
                        f"sent={'SENT' if fcm_sent else 'FAILED'}"
                    )
                else:
                    # No usable live socket: FCM is the fallback. If the token is
                    # missing/FCM temporarily fails, keep the call ringing so a
                    # later WebSocket reconnect can reconcile the authoritative call.
                    fcm_sent = await send_call_notification_async(
                        target_id=target_id,
                        caller_id=user_id,
                        caller_name=str(
                            message.get("caller_name", "مستخدم CN CALL")
                        ),
                        call_id=call_id,
                        message_type="incoming_call",
                    )
                    print(
                        "[CN CALL][CALL DELIVERY MODE] "
                        f"call_id={call_id} mode=fcm "
                        f"fcm={'SENT' if fcm_sent else 'FAILED'}"
                    )

                    if not fcm_sent:
                        # FCM failed, so this is the only case where the server
                        # has positive evidence that the fallback delivery path
                        # is unavailable. Restore the caller-side offline voice
                        # without classifying a missing WebSocket alone as offline.
                        event_ids = finalize_call_terminal(
                            call_id,
                            "missed",
                            [
                                (user_id, "call_reject", target_id),
                            ],
                            terminal_reason="offline",
                        )

                        for event_id in event_ids:
                            await _deliver_terminal_event(event_id)
                        print(
                            "[CN CALL][CALL OFFLINE ANNOUNCEMENT] "
                            f"call_id={call_id} target={target_id}"
                        )

                continue

            record = active_calls.get(call_id)
            if record is None:
                await websocket.send_json({
                    "type": "signaling_rejected",
                    "call_id": call_id,
                    "message_type": message_type,
                    "reason": "unknown_or_ended_call",
                })
                continue

            caller_id = str(record["caller_id"])
            receiver_id = str(record["target_id"])
            expected_target = receiver_id if user_id == caller_id else caller_id
            sender_role = (
                "caller" if user_id == caller_id else
                "target" if user_id == receiver_id else None
            )
            expected_token = (
                record["caller_token"] if sender_role == "caller" else
                record["target_token"] if sender_role == "target" else None
            )
            if sender_role is None or expected_token != token:
                await websocket.send_json({
                    "type": "signaling_rejected",
                    "call_id": call_id,
                    "message_type": message_type,
                    "reason": "sender_not_call_owner",
                })
                continue

            if target_id != expected_target:
                await websocket.send_json({
                    "type": "signaling_rejected",
                    "call_id": call_id,
                    "message_type": message_type,
                    "reason": "invalid_target",
                })
                continue

            status = str(record["status"])
            allowed = False
            next_status = status
            terminal = False
            if message_type == "call_delivered":
                # Target confirms that Android Telecom accepted the incoming
                # call presentation. Keep the call ringing and forward the
                # delivery ACK to the caller exactly once. This is deliberately
                # idempotent so an FCM/WS duplicate cannot restart ringback.
                allowed = sender_role == "target" and status == "ringing"
                next_status = "ringing"
                if allowed and bool(record.get("delivery_confirmed")):
                    print(
                        "[CN CALL][CALL DELIVERED DUPLICATE] "
                        f"call_id={call_id} target={user_id}"
                    )
                    continue
                if allowed:
                    record["delivery_confirmed"] = True
            elif message_type == "call_accept":
                allowed = sender_role == "target" and status == "ringing"
                next_status = "accepted"
            elif message_type == "call_reject":
                allowed = sender_role == "target" and status == "ringing"
                next_status = "rejected"
                terminal = True
            elif message_type == "call_cancelled":
                # The caller can race the callee's acceptance: the callee may
                # have committed "accepted" or even started negotiation before
                # the caller receives the call_accept frame. Treat a caller-side
                # cancel during any pre-connected state as a terminal cancel.
                # Once connected, the correct terminal frame is "hangup"; do not
                # broaden call_cancelled that far because a stale cancel must not
                # tear down an already-connected conversation.
                allowed = sender_role == "caller" and status in {
                    "ringing", "accepted", "negotiating"
                }
                next_status = "cancelled"
                terminal = True
            elif message_type == "hangup":
                allowed = status in {"ringing", "accepted", "negotiating", "connected"}
                next_status = "ended"
                terminal = True
            elif message_type == "offer":
                allowed = sender_role == "caller" and status in {
                    "accepted", "negotiating", "connected"
                }
                next_status = "negotiating"
            elif message_type == "answer":
                allowed = sender_role == "target" and status in {
                    "accepted", "negotiating", "connected"
                }
                next_status = "negotiating"
            elif message_type == "ice_candidate":
                allowed = status in {"accepted", "negotiating", "connected"}
            elif message_type == "connected":
                allowed = status in {"accepted", "negotiating"}
                next_status = "connected"
            elif message_type == "timeout":
                allowed = status == "ringing"
                next_status = "timeout"
                terminal = True

            if not allowed:
                await websocket.send_json({
                    "type": "signaling_rejected",
                    "call_id": call_id,
                    "message_type": message_type,
                    "reason": "invalid_state_or_direction",
                })
                continue

            if message_type == "call_accept":
                transition_call_state(
                    call_id,
                    "accepted",
                    negotiation_expires_at=int(time.time() * 1000) + 30000,
                )
                print("[CN CALL][CALL_ACCEPT SERVER] call_id=", call_id)

                # Design C: Generate distinct LiveKit credentials for callee and caller.
                # Secrets and tokens are kept out of server logs.
                callee_creds = _generate_livekit_token_for_user(user_id, call_id)
                caller_creds = _generate_livekit_token_for_user(caller_id, call_id)

                # 1. Send call_accept_ack to the accepting endpoint (callee - user_id)
                callee_ack_payload = {
                    "type": "call_accept_ack",
                    "call_id": call_id,
                    "target_id": expected_target,
                    "from_id": user_id,
                }
                if callee_creds:
                    callee_ack_payload.update({
                        "livekit_url": callee_creds["url"],
                        "livekit_token": callee_creds["token"],
                        "room": callee_creds["room"],
                    })

                try:
                    await websocket.send_json(callee_ack_payload)
                    print(
                        "[CN CALL][CALL_ACCEPT ACK SENT] "
                        f"call_id={call_id} target={user_id} embedded_creds={callee_creds is not None}"
                    )
                except Exception as exc:
                    print(
                        "[CN CALL][CALL_ACCEPT ACK FAILED] "
                        f"call_id={call_id} target={user_id} error={exc}"
                    )

                # 2. Forward call_accept to caller (caller_id) with caller's token
                caller_socket = connections.get(caller_id)
                if caller_socket is not None:
                    caller_accept_payload = {
                        "type": "call_accept",
                        "call_id": call_id,
                        "target_id": caller_id,
                        "from_id": user_id,
                    }
                    if caller_creds:
                        caller_accept_payload.update({
                            "livekit_url": caller_creds["url"],
                            "livekit_token": caller_creds["token"],
                            "room": caller_creds["room"],
                        })
                    try:
                        await caller_socket.send_json(caller_accept_payload)
                        print(
                            "[CN CALL][CALL_ACCEPT FORWARDED TO CALLER] "
                            f"call_id={call_id} caller={caller_id} embedded_creds={caller_creds is not None}"
                        )
                    except Exception as exc:
                        print(
                            "[CN CALL][CALL_ACCEPT FORWARD FAILED] "
                            f"call_id={call_id} caller={caller_id} error={exc}"
                        )
            elif message_type in {"offer", "answer"}:
                transition_call_state(
                    call_id,
                    "negotiating",
                    connection_expires_at=int(time.time() * 1000) + 30000,
                )
            elif message_type == "connected":
                ready_users = set(record.get("media_ready_users") or set())
                ready_users.add(user_id)

                both_ready = {caller_id, receiver_id}.issubset(ready_users)
                connected_status = "connected" if both_ready else "negotiating"

                transition_call_state(
                    call_id,
                    connected_status,
                    negotiation_expires_at=None,
                    connection_expires_at=(
                        int(time.time() * 1000) + CONNECTED_IDLE_TIMEOUT_MS
                        if both_ready
                        else record.get("connection_expires_at")
                    ),
                    media_ready_users=ready_users,
                )

                # Send server confirmation ACK back to reporting endpoint
                try:
                    await websocket.send_json({
                        "type": "connected_ack",
                        "call_id": call_id,
                        "status": str(record["status"]),
                        "from_id": user_id,
                    })
                except Exception as exc:
                    print(f"[CN CALL][CONNECTED ACK FAILED] call_id={call_id} user={user_id} error={exc}")
            # Any valid frame that touches a connected call is activity: it
            # re-arms the connected-idle deadline instead of counting toward
            # it, so an active conversation is never auto-cut.
            if (
                str(record["status"]) == "connected"
                and message_type != "connected"
            ):
                transition_call_state(
                    call_id,
                    "connected",
                    connection_expires_at=(
                        int(time.time() * 1000)
                        + CONNECTED_IDLE_TIMEOUT_MS
                    ),
                    media_ready_users=record.get("media_ready_users") or set(),
                )
            forwarded = {
                **message,
                "call_id": call_id,
                "target_id": expected_target,
                "from_id": user_id,
            }
            if terminal:
                event_ids = finalize_call_terminal(
                    call_id,
                    next_status,
                    [
                        (expected_target, message_type, user_id),
                    ],
                )

                for event_id in event_ids:
                    await _deliver_terminal_event(event_id)

            elif message_type == "call_accept":
                # Design C: call_accept and its credentials were already forwarded
                # to caller_socket above; skip double-forwarding here.
                pass
            elif expected_target in connections:
                try:
                    await connections[expected_target].send_json(forwarded)
                except Exception as exc:
                    print("CALL FORWARD WS ERROR:", exc)

    except WebSocketDisconnect:
        pass

    finally:
        if connections.get(user_id) is websocket:
            del connections[user_id]
            # A network reconnect is not a call hangup.  Keep ownership and
            # let the call's explicit terminal signal or its expiry timer end
            # it; the next socket for this logical user can safely resume.
            print("[CN CALL][SOCKET CLOSED] user_id=", user_id)
# cn-call2 railway test
