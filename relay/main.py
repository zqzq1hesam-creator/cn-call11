"""Small public relay for CN CALL when an ISP blocks Cloudflare IPs.

Clients connect to this Railway service; it forwards HTTP and WebSocket traffic
to the existing Cloudflare Worker. User authentication, call state, and the
database remain authoritative at the Worker. Never log query strings because
the WebSocket URL includes a session token.
"""
from __future__ import annotations

import asyncio
import os
from typing import Any

import httpx
import websockets
from fastapi import FastAPI, Request, WebSocket
from fastapi.responses import JSONResponse, Response
from starlette.websockets import WebSocketDisconnect

UPSTREAM_HOST = os.getenv(
    "CN_CALL_UPSTREAM_HOST",
    "cn-call11.zqzq1hesam-cncall.workers.dev",
).strip()
if not UPSTREAM_HOST or "/" in UPSTREAM_HOST:
    raise RuntimeError("CN_CALL_UPSTREAM_HOST must be a hostname only")
UPSTREAM_HTTPS = f"https://{UPSTREAM_HOST}"
UPSTREAM_WSS = f"wss://{UPSTREAM_HOST}"

_HOP_BY_HOP = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",
    "upgrade",
    "host",
    "content-length",
    "accept-encoding",
    "content-encoding",
}

app = FastAPI(title="CN CALL Network Relay", docs_url=None, redoc_url=None)
_http: httpx.AsyncClient | None = None


@app.on_event("startup")
async def startup() -> None:
    global _http
    _http = httpx.AsyncClient(
        timeout=httpx.Timeout(30.0, connect=10.0),
        follow_redirects=False,
        headers={"Accept-Encoding": "identity"},
    )


@app.on_event("shutdown")
async def shutdown() -> None:
    global _http
    if _http is not None:
        await _http.aclose()
        _http = None


@app.get("/relay-health")
async def relay_health() -> dict[str, Any]:
    return {"ok": True, "relay": "ready"}


@app.api_route(
    "/{path:path}",
    methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"],
)
async def proxy_http(path: str, request: Request) -> Response:
    client = _http
    if client is None:
        return JSONResponse({"success": False, "message": "relay starting"}, status_code=503)

    # Preserve raw query text (including repeated parameters) but never log it:
    # login/session values must not be written to service logs.
    url = f"{UPSTREAM_HTTPS}/{path}"
    if request.url.query:
        url = f"{url}?{request.url.query}"

    forwarded_headers = {
        key: value
        for key, value in request.headers.items()
        if key.lower() not in _HOP_BY_HOP
    }

    try:
        upstream = await client.request(
            request.method,
            url,
            headers=forwarded_headers,
            content=await request.body(),
        )
    except httpx.HTTPError as exc:
        # Log only the exception class, not the URL or query string.
        print(f"[CN CALL RELAY][HTTP ERROR] type={type(exc).__name__}")
        return JSONResponse(
            {"success": False, "message": "upstream temporarily unavailable"},
            status_code=502,
        )

    response_headers = {
        key: value
        for key, value in upstream.headers.items()
        if key.lower() not in _HOP_BY_HOP
    }
    return Response(
        content=upstream.content,
        status_code=upstream.status_code,
        headers=response_headers,
    )


@app.websocket("/{path:path}")
async def proxy_websocket(client_ws: WebSocket, path: str) -> None:
    # This relay is only for the app's authenticated signaling socket.
    if not path.startswith("ws/"):
        await client_ws.close(code=1008, reason="unsupported websocket route")
        return

    url = f"{UPSTREAM_WSS}/{path}"
    if client_ws.url.query:
        url = f"{url}?{client_ws.url.query}"

    try:
        # The session token remains in the query string required by the
        # existing Worker protocol. Never print this URL or its query.
        async with websockets.connect(
            url,
            extra_headers={"User-Agent": "CN-CALL-network-relay"},
            open_timeout=12,
            close_timeout=4,
            ping_interval=20,
            ping_timeout=20,
            max_size=None,
        ) as upstream_ws:
            await client_ws.accept()

            async def client_to_upstream() -> None:
                while True:
                    event = await client_ws.receive()
                    if event["type"] == "websocket.disconnect":
                        return
                    text_value = event.get("text")
                    bytes_value = event.get("bytes")
                    if text_value is not None:
                        await upstream_ws.send(text_value)
                    elif bytes_value is not None:
                        await upstream_ws.send(bytes_value)

            async def upstream_to_client() -> None:
                async for message in upstream_ws:
                    if isinstance(message, bytes):
                        await client_ws.send_bytes(message)
                    else:
                        await client_ws.send_text(message)

            tasks = {
                asyncio.create_task(client_to_upstream()),
                asyncio.create_task(upstream_to_client()),
            }
            try:
                done, pending = await asyncio.wait(
                    tasks,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                for task in pending:
                    task.cancel()
                await asyncio.gather(*pending, return_exceptions=True)
                for task in done:
                    error = task.exception()
                    if error is not None and not isinstance(error, WebSocketDisconnect):
                        print(f"[CN CALL RELAY][WS CLOSED] type={type(error).__name__}")
            finally:
                for task in tasks:
                    if not task.done():
                        task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                try:
                    await client_ws.close(code=1000)
                except Exception:
                    pass
    except Exception as exc:
        # Do not print exception messages: some WebSocket libraries include URLs.
        print(f"[CN CALL RELAY][WS ERROR] type={type(exc).__name__}")
        try:
            await client_ws.close(code=1013, reason="upstream temporarily unavailable")
        except Exception:
            pass
