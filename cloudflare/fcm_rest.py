"""Minimal Firebase Cloud Messaging HTTP v1 client for Cloudflare Workers."""
from __future__ import annotations

import base64
import json
import time
from urllib.parse import urlencode

from js import Object, Uint8Array, crypto, fetch
from pyodide.ffi import to_js


_TOKEN_CACHE: dict[str, tuple[str, float]] = {}
_SCOPE = "https://www.googleapis.com/auth/firebase.messaging"
_TOKEN_AUDIENCE = "https://oauth2.googleapis.com/token"


def _js_object(value):
    return to_js(value, dict_converter=Object.fromEntries)


def _uint8(raw: bytes):
    return Uint8Array.new(to_js(list(raw)))


def _b64url(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


async def _sign_service_account_jwt(service_account: dict, issued_at: int) -> str:
    private_key = str(service_account["private_key"]).replace("\\n", "\n")
    lines = [
        line.strip()
        for line in private_key.splitlines()
        if line.strip() and not line.startswith("-----")
    ]
    der = base64.b64decode("".join(lines))
    key = await crypto.subtle.importKey(
        "pkcs8",
        _uint8(der),
        _js_object({"name": "RSASSA-PKCS1-v1_5", "hash": "SHA-256"}),
        False,
        to_js(["sign"]),
    )

    header = {"alg": "RS256", "typ": "JWT"}
    if service_account.get("private_key_id"):
        header["kid"] = service_account["private_key_id"]
    claims = {
        "iss": service_account["client_email"],
        "scope": _SCOPE,
        "aud": _TOKEN_AUDIENCE,
        "iat": issued_at,
        "exp": issued_at + 3600,
    }
    unsigned = (
        _b64url(json.dumps(header, separators=(",", ":"), sort_keys=True).encode("utf-8"))
        + "."
        + _b64url(json.dumps(claims, separators=(",", ":"), sort_keys=True).encode("utf-8"))
    )
    signature_buffer = await crypto.subtle.sign(
        _js_object({"name": "RSASSA-PKCS1-v1_5"}),
        key,
        _uint8(unsigned.encode("ascii")),
    )
    signature_array = Uint8Array.new(signature_buffer)
    signature = bytes(int(signature_array[index]) for index in range(int(signature_array.length)))
    return unsigned + "." + _b64url(signature)


async def _google_access_token(service_account: dict) -> str:
    cache_key = str(service_account["client_email"])
    cached = _TOKEN_CACHE.get(cache_key)
    now = int(time.time())
    if cached and cached[1] > now + 60:
        return cached[0]

    assertion = await _sign_service_account_jwt(service_account, now)
    response = await fetch(
        _TOKEN_AUDIENCE,
        _js_object({
            "method": "POST",
            "headers": {"Content-Type": "application/x-www-form-urlencoded"},
            "body": urlencode({
                "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
                "assertion": assertion,
            }),
        }),
    )
    response_text = await response.text()
    if not response.ok:
        raise RuntimeError(f"Google OAuth token request failed ({response.status})")
    payload = json.loads(response_text)
    access_token = str(payload.get("access_token", ""))
    expires_in = int(payload.get("expires_in", 3600))
    if not access_token:
        raise RuntimeError("Google OAuth response did not include an access token")
    _TOKEN_CACHE[cache_key] = (access_token, now + max(60, expires_in - 60))
    return access_token


async def send_fcm_v1(
    *,
    service_account: dict,
    registration_token: str,
    data: dict[str, str],
    collapse_key: str,
    ttl_seconds: int,
) -> tuple[bool, bool, str]:
    """Return (sent, token_is_unregistered, safe detail)."""

    access_token = await _google_access_token(service_account)
    project_id = str(service_account["project_id"])
    endpoint = f"https://fcm.googleapis.com/v1/projects/{project_id}/messages:send"
    message = {
        "message": {
            "token": registration_token,
            "data": {str(key): str(value) for key, value in data.items()},
            "android": {
                "priority": "HIGH",
                "collapse_key": collapse_key,
                "ttl": f"{int(ttl_seconds)}s",
            },
        }
    }
    response = await fetch(
        endpoint,
        _js_object({
            "method": "POST",
            "headers": {
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json; charset=UTF-8",
            },
            "body": json.dumps(message, separators=(",", ":")),
        }),
    )
    body = await response.text()
    if 200 <= int(response.status) < 300:
        return True, False, "ok"

    unregistered = False
    try:
        error = json.loads(body).get("error", {})
        details = json.dumps(error, ensure_ascii=False)
        unregistered = (
            str(error.get("status", "")).upper() == "NOT_FOUND"
            or "UNREGISTERED" in details.upper()
            or "REGISTRATION-TOKEN-NOT-REGISTERED" in details.upper()
        )
    except Exception:
        pass

    # Never return or log the request URL, bearer token, FCM registration token,
    # or service-account key.
    detail = f"http_{int(response.status)}"
    try:
        error = json.loads(body).get("error", {})
        status = str(error.get("status", "")).strip()
        if status:
            detail += f"_{status[:48]}"
    except Exception:
        pass
    return False, unregistered, detail
