# CN CALL — Cloudflare Workers production configuration

The canonical application branch is `main`. This repository no longer contains the old server/ application entry point,
Railway deployment files, local SQLite runtime, or the Firebase Admin/gRPC SDK.

## Cloudflare Git deployment

- Worker name: `cn-call11`
- Production branch: `main`
- Root directory: repository root (leave blank)
- Build command: `uv sync`
- Deploy command: `uv run pywrangler deploy`
- Preview builds: disabled

## Required Cloudflare secrets / variables

Configure these as **Worker secrets/variables** in the Cloudflare dashboard.
Do not commit their values or paste them into this repository:

- `CN_CALL_DB_HOST`
- `CN_CALL_DB_PORT` (default `5432`)
- `CN_CALL_DB_NAME`
- `CN_CALL_DB_USER`
- `CN_CALL_DB_PASSWORD` (secret)
- `CN_CALL_DB_SSL` (default `true`)
- `FIREBASE_SERVICE_ACCOUNT_JSON` (secret JSON for the service account)
- `LIVEKIT_URL`
- `LIVEKIT_API_KEY`
- `LIVEKIT_API_SECRET` (secret)

The PostgreSQL endpoint must be reachable from Cloudflare Workers. The
implementation uses the pure-Python `pg8000` driver; Cloudflare Hyperdrive is
recommended for the database connection if a supported PostgreSQL instance is
available.

Until the database, Firebase service account, and LiveKit credentials are all
configured, `/health` reports `ready: false` and the API remains in
maintenance mode. WebSocket clients receive the maintenance signal. This is
intentional: no local, ephemeral SQLite database is created.

Firebase Cloud Messaging uses Google's HTTP v1 REST API with the Workers Web
Crypto API for service-account JWT signing. LiveKit room tokens are signed with
HS256 using Python's standard library. The legacy Firebase Admin SDK, its gRPC
dependency, the LiveKit Python SDK, Railway's Docker configuration, and the
SQLite volume fallback are no longer used.

Keep the previous database file only for recovery/migration until user records
are safely migrated into the new PostgreSQL database. The old SQLite data file is retained at `recovery/cn_call_legacy.db` only as a
migration/recovery artifact. The running Cloudflare application never reads
that file.
