"""Cloudflare entry point for the production CN CALL backend."""
import importlib
import os

from workers import DurableObject, WorkerEntrypoint, asgi

_SETTING_NAMES = (
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
)

_app = None


def _get_app(runtime_env):
    """Load FastAPI only after copying this Durable Object's real bindings.

    Module-level imports run before request-scoped Worker environment bindings
    are reliably available. server_main reads its configuration at import time,
    so importing it earlier caused every setting to look missing even when
    secrets had been saved on the Worker.
    """
    global _app
    if _app is not None:
        return _app

    for name in _SETTING_NAMES:
        try:
            value = getattr(runtime_env, name)
        except Exception:
            continue
        if value is None:
            continue
        text_value = str(value)
        if text_value.strip():
            os.environ[name] = text_value

    os.environ["CN_CALL_PLATFORM"] = "cloudflare"

    # Import after the runtime bindings are available. server_main initializes
    # the configuration and database schema during import.
    module = importlib.import_module("server_main")
    _app = module.app
    return _app


class CNCallHub(DurableObject):
    """One authoritative signaling coordinator for the CN CALL service."""

    def __init__(self, ctx, env):
        super().__init__(ctx, env)
        self.ctx = ctx
        self.env = env

    async def fetch(self, request):
        app = _get_app(self.env)
        return await asgi.fetch(app, request, self.env)


class Default(WorkerEntrypoint):
    async def fetch(self, request):
        hub_id = self.env.CALL_HUB.idFromName("cn-call11-global")
        hub = self.env.CALL_HUB.get(hub_id)
        return await hub.fetch(request)
