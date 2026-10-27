"""Cloudflare entry point for the production CN CALL backend."""
import importlib
import os
import workers

# Runtime compatibility guard: some Workers Python runtime snapshots expose
# waitUntil while the vendored ASGI adapter imports wait_until. Normalize the
# alias before importing ASGI so an SDK/runtime version skew cannot crash every
# Worker invocation with error 1101.
if not hasattr(workers, "wait_until") and hasattr(workers, "waitUntil"):
    workers.wait_until = workers.waitUntil

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

    # Prefer Hyperdrive when the binding is available. Hyperdrive manages TLS
    # to Neon and presents its pooled endpoint to the Worker; the Worker-side
    # pg8000 connection must therefore not initiate a second TLS handshake.
    try:
        hyperdrive = getattr(runtime_env, "HYPERDRIVE")
    except Exception:
        hyperdrive = None

    if hyperdrive is not None:
        binding_fields = {
            "CN_CALL_DB_HOST": "host",
            "CN_CALL_DB_PORT": "port",
            "CN_CALL_DB_NAME": "database",
            "CN_CALL_DB_USER": "user",
            "CN_CALL_DB_PASSWORD": "password",
        }
        missing_fields = []
        for setting_name, binding_name in binding_fields.items():
            try:
                value = getattr(hyperdrive, binding_name)
            except Exception:
                value = None
            if value is None or not str(value).strip():
                missing_fields.append(binding_name)
            else:
                os.environ[setting_name] = str(value).strip()

        if missing_fields:
            raise RuntimeError(
                "HYPERDRIVE binding missing required fields: "
                + ", ".join(missing_fields)
            )

        os.environ["CN_CALL_DB_SSL"] = "false"
        print("[CN CALL][DB] Hyperdrive binding detected; using pooled database connection")

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
        # ASGI's HTTP fetch adapter does not perform WebSocket upgrades.
        # Route signaling sockets through the dedicated ASGI WebSocket adapter;
        # otherwise /ws/{user_id} is treated as an ordinary HTTP request and
        # calls/rejections never reach the connected Android clients.
        if (request.headers.get("upgrade") or "").lower() == "websocket":
            return await asgi.websocket(app, request, self.env)
        return await asgi.fetch(app, request, self.env)


class Default(WorkerEntrypoint):
    async def fetch(self, request):
        hub_id = self.env.CALL_HUB.idFromName("cn-call11-global")
        hub = self.env.CALL_HUB.get(hub_id)
        return await hub.fetch(request)
