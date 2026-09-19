from fastapi import FastAPI, Header, HTTPException, WebSocket, WebSocketDisconnect
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


BASE_DIR = Path(__file__).resolve().parent
VOLUME_DIR = Path("/app/data")
DB_PATH = VOLUME_DIR / "cn_call.db"
LEGACY_DB_PATH = BASE_DIR / "cn_call.db"

VOLUME_DIR.mkdir(parents=True, exist_ok=True)

if not DB_PATH.exists() and LEGACY_DB_PATH.exists():
    shutil.copy2(LEGACY_DB_PATH, DB_PATH)
    print("[CN CALL][DB] Legacy database migrated to Railway Volume")


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


def release_call(call_id: str, reason: str) -> bool:
    record = active_calls.pop(call_id, None)
    if record is None:
        return False

    caller_id = str(record["caller_id"])
    target_id = str(record["target_id"])
    _unmark_active_user(caller_id, call_id, reason)
    _unmark_active_user(target_id, call_id, reason)

    status = reason
    if reason == "timeout":
        status = "missed" if record["status"] == "ringing" else "timeout"

    db = get_db()
    db.execute(
        "UPDATE call_records SET status = ? WHERE call_id = ?",
        (status, call_id),
    )
    db.commit()
    db.close()
    print("[CN CALL][CALL TERMINAL] call_id=", call_id, "reason=", reason)
    return True


async def _send_terminal_call_event(
    record: dict[str, object],
    target_id: str,
    message_type: str,
    from_id: str,
):
    """Deliver terminal signaling through the same route as the call.

    A terminal event must not depend on a live WebSocket: an incoming Android
    CallKit UI may be the only process left on the target device.
    """
    call_id = str(record["call_id"])
    payload = {
        "type": message_type,
        "call_id": call_id,
        "target_id": target_id,
        "from_id": from_id,
    }
    target_socket = connections.get(target_id)
    if target_socket is not None:
        try:
            await target_socket.send_json(payload)
            print("[CN CALL][CALL TERMINAL WS]", message_type, "call_id=", call_id, "target=", target_id)
            return
        except Exception as exc:
            print("[CN CALL][CALL TERMINAL WS ERROR]", exc)

    # Only the callee has an incoming native call UI to remove.  FCM is the
    # fallback when that UI exists without a WebSocket (background/terminated).
    if message_type in {"call_cancelled", "call_reject", "hangup", "timeout", "disconnected"}:
        print("[CN CALL][CALL TERMINAL FCM]", message_type, "call_id=", call_id, "target=", target_id)
        send_call_notification(
            target_id=target_id,
            caller_id=str(record["caller_id"]),
            caller_name=str(record.get("caller_name", "مستخدم CN CALL")),
            call_id=call_id,
            message_type=message_type,
        )


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
        await _send_terminal_call_event(
            record,
            peer_id,
            message_type,
            user_id,
        )
        release_call(call_id, "cancelled" if message_type == "call_cancelled" else "ended")


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
                record["connection_expires_at"] = (
                    now + CONNECTED_IDLE_TIMEOUT_MS
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
        if str(record["status"]) == "ringing":
            await _send_terminal_call_event(
                record,
                target_id,
                "call_cancelled",
                caller_id,
            )
        else:
            await _send_terminal_call_event(
                record,
                target_id,
                "hangup",
                caller_id,
            )
            await _send_terminal_call_event(
                record,
                caller_id,
                "hangup",
                target_id,
            )
        release_call(call_id, "timeout")


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
    global call_expiry_task
    call_expiry_task = asyncio.create_task(_call_expiry_loop())


@app.on_event("shutdown")
async def stop_call_expiry_loop():
    global call_expiry_task
    if call_expiry_task is not None:
        call_expiry_task.cancel()
        call_expiry_task = None

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

def get_db():
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row
    return db


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
            status TEXT NOT NULL
        )
        """
    )

    db.commit()
    db.close()


init_db()


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
        raise HTTPException(status_code=403, detail="غير مصرح")

    now = int(time.time() * 1000)
    db = get_db()
    db.execute(
        """
        UPDATE call_records
        SET status = 'missed'
        WHERE target_id = ? AND status = 'ringing' AND expires_at <= ?
        """,
        (user_id.strip(), now),
    )
    rows = db.execute(
        """
        SELECT call_id, caller_id, caller_name, created_at
        FROM call_records
        WHERE target_id = ? AND status = 'missed'
        ORDER BY created_at DESC
        """,
        (user_id.strip(),),
    ).fetchall()
    db.execute(
        "UPDATE call_records SET status = 'missed_delivered' "
        "WHERE target_id = ? AND status = 'missed'",
        (user_id.strip(),),
    )
    db.commit()
    db.close()

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
            },
            android=messaging.AndroidConfig(
                priority="high",
                collapse_key=f"call-{call_id}",
                ttl=timedelta(seconds=95),
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

    try:
        await websocket.send_json({
            "type": "connected",
            "user_id": user_id,
        })

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
            print("[CN CALL][CALL MESSAGE] type=", message_type, "call_id=", call_id, "from=", user_id)

            await expire_active_calls()

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
                    continue

                db = get_db()
                existing = db.execute(
                    "SELECT status FROM call_records WHERE call_id = ?",
                    (call_id,),
                ).fetchone()
                db.close()
                if existing is not None:
                    await websocket.send_json({
                        "type": "call_reject",
                        "call_id": call_id,
                        "target_id": user_id,
                        "reason": "duplicate_or_busy",
                    })
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
                        send_call_notification(
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
                        release_call(previous_call_id, "missed")
                    else:
                        await websocket.send_json({
                            "type": "call_reject",
                            "call_id": call_id,
                            "target_id": user_id,
                            "reason": "duplicate_or_busy",
                        })
                        continue

                if user_id in active_call_users:
                    await websocket.send_json({
                        "type": "call_reject",
                        "call_id": call_id,
                        "target_id": user_id,
                        "reason": "duplicate_or_busy",
                    })
                    continue

                ring_expires_at = message.get("ring_expires_at")
                if not ring_expires_at:
                    ring_expires_at = int(time.time() * 1000) + 90000

                created_at = int(time.time() * 1000)
                status = "ringing"
                db = get_db()
                db.execute(
                    """
                    INSERT INTO call_records
                    (call_id, caller_id, target_id, caller_name,
                     created_at, expires_at, status)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        call_id,
                        user_id,
                        target_id,
                        str(message.get("caller_name", "مستخدم CN CALL")),
                        created_at,
                        ring_expires_at,
                        status,
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
                }
                _mark_active_user(user_id, call_id, "caller")
                _mark_active_user(target_id, call_id, "callee")

                target_socket = connections.get(target_id)
                target_online = target_socket is not None

                await websocket.send_json({
                    "type": "call_started",
                    "call_id": call_id,
                    "target_id": target_id,
                    "from_id": user_id,
                    "ring_expires_at": ring_expires_at,
                    "target_online": target_online,
                })

                print(
                    "[CN CALL][CALL INITIAL WS ATTEMPT] "
                    f"call_id={call_id} target={target_id} "
                    f"socket_present={target_socket is not None}"
                )
                delivered = False
                if target_socket is not None:
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

                if not delivered:
                    print(
                        "[CN CALL][CALL INITIAL FCM FALLBACK] "
                        f"call_id={call_id} target={target_id}"
                    )
                    fcm_sent = send_call_notification(
                        target_id=target_id,
                        caller_id=user_id,
                        caller_name=str(
                            message.get("caller_name", "مستخدم CN CALL")
                        ),
                        call_id=call_id,
                    )
                    print(
                        "[CN CALL][CALL INITIAL FCM "
                        f"{'SENT' if fcm_sent else 'FAILED'}] "
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
                # delivery ACK to the caller.
                allowed = sender_role == "target" and status == "ringing"
                next_status = "ringing"
            elif message_type == "call_accept":
                allowed = sender_role == "target" and status == "ringing"
                next_status = "accepted"
            elif message_type == "call_reject":
                allowed = sender_role == "target" and status == "ringing"
                next_status = "rejected"
                terminal = True
            elif message_type == "call_cancelled":
                allowed = sender_role == "caller" and status == "ringing"
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

            record["status"] = next_status
            if message_type == "call_accept":
                record["negotiation_expires_at"] = (
                    int(time.time() * 1000) + 30000
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
            elif message_type == "offer":
                record["connection_expires_at"] = (
                    int(time.time() * 1000) + 30000
                )
            elif message_type == "connected":
                record["negotiation_expires_at"] = None
                ready_users = record.setdefault("media_ready_users", set())
                if isinstance(ready_users, set):
                    ready_users.add(user_id)
                    if {caller_id, receiver_id}.issubset(ready_users):
                        record["status"] = "connected"
                        record["connection_expires_at"] = (
                            int(time.time() * 1000)
                            + CONNECTED_IDLE_TIMEOUT_MS
                        )
                    else:
                        # One endpoint has local media, but the call is not
                        # connected until both have reported a usable path.
                        record["status"] = "negotiating"
            # Any valid frame that touches a connected call is activity: it
            # re-arms the connected-idle deadline instead of counting toward
            # it, so an active conversation is never auto-cut.
            if str(record["status"]) == "connected":
                record["connection_expires_at"] = (
                    int(time.time() * 1000)
                    + CONNECTED_IDLE_TIMEOUT_MS
                )
            forwarded = {
                **message,
                "call_id": call_id,
                "target_id": expected_target,
                "from_id": user_id,
            }
            if terminal:
                await _send_terminal_call_event(
                    record,
                    expected_target,
                    message_type,
                    user_id,
                )
            elif message_type == "call_accept":
                # Design C: call_accept and its credentials were already forwarded
                # to caller_socket above; skip double-forwarding here.
                pass
            elif expected_target in connections:
                try:
                    await connections[expected_target].send_json(forwarded)
                except Exception as exc:
                    print("CALL FORWARD WS ERROR:", exc)

            if terminal:
                release_call(call_id, next_status)

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
