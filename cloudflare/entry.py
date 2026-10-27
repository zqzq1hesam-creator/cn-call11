"""Cloudflare entry point for CN CALL.

All HTTP and WebSocket requests are routed through one Durable Object so the
existing in-memory signaling state is coordinated by a single authoritative
instance. If persistent database configuration is absent, the app stays in
maintenance mode rather than writing to non-persistent local disk.
"""
import os

from workers import DurableObject, WorkerEntrypoint, asgi, env


def _copy_binding_to_environment(name: str) -> None:
    try:
        value = getattr(env, name)
    except Exception:
        return
    if value is None:
        return
    text_value = str(value)
    if text_value.strip():
        os.environ[name] = text_value


for _setting in (
    "DATABASE_URL",
    "FIREBASE_SERVICE_ACCOUNT_JSON",
    "LIVEKIT_URL",
    "LIVEKIT_API_KEY",
    "LIVEKIT_API_SECRET",
    "CN_CALL_DEVELOPER_ID",
    "CN_CALL_DEVELOPER_NAME",
    "CN_CALL_DEVELOPER_BADGE",
    "CN_CALL_MIGRATION_MODE",
):
    _copy_binding_to_environment(_setting)

os.environ["CN_CALL_PLATFORM"] = "cloudflare"
if not os.environ.get("DATABASE_URL", "").strip():
    os.environ["CN_CALL_MIGRATION_MODE"] = "true"
else:
    os.environ.setdefault("CN_CALL_MIGRATION_MODE", "false")

# Import only after Cloudflare bindings have been copied into os.environ;
# server/main.py reads these settings during module initialization.
from server.main import app  # noqa: E402


class CNCallHub(DurableObject):
    """Single coordinator for CN CALL HTTP and WebSocket signaling."""

    def __init__(self, ctx, env):
        super().__init__(ctx, env)
        self.ctx = ctx
        self.env = env

    async def fetch(self, request):
        return await asgi.fetch(app, request, self.env)


class Default(WorkerEntrypoint):
    async def fetch(self, request):
        hub_id = self.env.CALL_HUB.idFromName("cn-call11-global")
        hub = self.env.CALL_HUB.get(hub_id)
        return await hub.fetch(request)
