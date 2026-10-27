# CN CALL — Cloudflare Free migration preflight

**State: PRE-FLIGHT ONLY. Not production-ready. Do not change mobile server URLs.**

This folder is an isolated Python Workers scaffold to validate the Cloudflare Free
runtime and build path without touching the Railway service or the production
`main` branch. It deliberately does not import `server/main.py`, does not connect
to a database, does not send Firebase messages, does not generate LiveKit tokens,
and does not implement CN CALL WebSocket signaling.

## Intended next stages

1. Verify this small FastAPI worker builds on Cloudflare Workers Free.
2. Inventory/validate Python Workers support for each production dependency.
3. Adapt Firebase FCM and LiveKit token generation without exposing credentials.
4. Adapt the production database connection to a supported Hyperdrive setup when
   the database is available again.
5. Test WebSocket signaling, incoming/outgoing calls, reconnect, FCM, LiveKit
   audio/video, and concurrency on a separate staging Worker.
6. Only after all tests pass, plan any mobile URL change. Do not merge this folder
   into `main` as a production server without that verification.

## Why it is not yet a production server

The existing Railway application in `server/main.py` imports `firebase_admin`,
`livekit-api`, and `psycopg[binary]`, and implements the production protocol
at `/ws/{user_id}`. This scaffold does not pretend those dependencies and
protocols already work under Python Workers.

Cloudflare's Python runtime uses Pyodide/WebAssembly, not the current Docker
container. FastAPI is supported, and Hyperdrive has a Python Workers PostgreSQL
guide, but each other dependency and the WebSocket lifecycle still need explicit
compatibility checks.

## Build locally / in CI

From this directory:

```sh
uv sync
uv run pywrangler deploy --dry-run
```

The dry run builds the Worker bundle; it does not deploy it. The included
GitHub Actions workflow runs the same preflight on this branch.

## Cloudflare dashboard

When ready to create the *staging preflight worker*:

- Import GitHub repository `zqzq1hesam-creator/cn-call11`.
- Select branch `cloudflare-free-migration`.
- Set the project root directory to `cloudflare-free`.
- Worker name must be `cn-call11-free-staging` to match `wrangler.jsonc`.
- Build command: `uv sync`
- Deploy command: `uv run pywrangler deploy`

Do **not** add production secrets to this preflight-only worker yet. They are not
used by the current scaffold, and storing them here would not migrate the
production service. Add them only after the production backend adapter is ready.
