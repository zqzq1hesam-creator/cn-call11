# CN CALL on Cloudflare Workers (main)

This deployment configuration lives on `main`; no separate staging branch is
used. Cloudflare Worker name must be `cn-call11`.

## Cloudflare Workers Builds

- Production branch: `main`
- Root directory: leave empty (repository root)
- Build command: `uv sync`
- Deploy command: `uv run pywrangler deploy`
- Preview builds: disabled

The Worker routes requests through the `CNCallHub` Durable Object so the
server's in-memory WebSocket/call coordination is handled by one authoritative
instance. The existing Railway service and mobile endpoint are not changed by
this configuration.

## Runtime configuration

Sensitive values must be stored as Cloudflare Secrets, never committed here:
`DATABASE_URL`, `FIREBASE_SERVICE_ACCOUNT_JSON`, `LIVEKIT_URL`,
`LIVEKIT_API_KEY`, and `LIVEKIT_API_SECRET`.

If `DATABASE_URL` is not set, the Worker enters maintenance mode and does not
attempt to create a local SQLite database. That is intentional: Workers do not
provide the persistent `/app/data` disk used by the Railway fallback. A
persistent PostgreSQL URL and valid secrets are required before calls can be
enabled.

Do not change the mobile app's server URL or disable the prior service until
the database, Firebase messaging, LiveKit token issuance, WebSocket signaling,
reconnect behavior, and real incoming/outgoing calls have been verified.
