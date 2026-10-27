"""Preflight-only FastAPI entry point for the CN CALL Cloudflare Free migration.

This is intentionally NOT the CN CALL production backend yet.
Do not point mobile clients at this Worker.
"""

from fastapi import FastAPI
from workers import asgi

app = FastAPI(
    title="CN CALL Cloudflare Free Preflight",
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
)


@app.get("/")
async def root() -> dict[str, object]:
    return {
        "service": "cn-call11-cloudflare-free",
        "phase": "preflight-only",
        "production_ready": False,
        "message": "Migration scaffold only; call signaling is not enabled.",
    }


@app.get("/health")
async def health() -> dict[str, object]:
    # Deliberately never claims that the actual CN CALL backend is healthy.
    return {
        "ok": True,
        "service": "cn-call11-cloudflare-free",
        "phase": "preflight-only",
        "production_ready": False,
        "call_signaling_enabled": False,
    }


# Cloudflare's Python Workers runtime supplies the ASGI adapter.
Default = asgi.entrypoint(app)
