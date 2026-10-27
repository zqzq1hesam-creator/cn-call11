"""Cloudflare entry point for the production CN CALL backend."""
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
    "CN_CALL_DB_HOST",
    "CN_CALL_DB_PORT",
    "CN_CALL_DB_NAME",
    "CN_CALL_DB_USER",
    "CN_CALL_DB_PASSWORD",
    "CN_CALL_DB_SSL",
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

# Import after copying bindings because server.main reads its configuration at
# import time. Missing database/Firebase/LiveKit values cause safe maintenance.
from server.main import app  # noqa: E402


class CNCallHub(DurableObject):
    """One authoritative signaling coordinator for the CN CALL service."""

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
