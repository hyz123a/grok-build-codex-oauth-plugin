# Grok Codex OAuth

An unofficial, local companion that lets Grok Build send its Responses API traffic through a
personal ChatGPT/Codex login. Grok keeps ownership of the agent loop, tools, sessions, permissions,
and TUI; this project owns only OAuth, protocol adaptation, and the loopback proxy.

This integration targets personal development. It uses a ChatGPT backend that is not a public,
stable API. For production or multi-user systems, use the OpenAI Platform API instead.

## What is implemented

- Browser OAuth with PKCE/state validation and device-code login
- OS keyring credential storage with a `~/.grok/codex-oauth/credentials.json` fallback
- Private fallback permissions (`0700` directory, `0600` file) and atomic writes
- Fixed-upstream loopback proxy at `127.0.0.1:1456`
- Local random proxy credential, request-size limit, and concurrency limit
- Responses JSON adaptation and streaming SSE bridging for Grok terminal-output compatibility
- Filtering of backend-only JSON `keepalive` events that Grok's Responses decoder cannot accept
- Expiry-window refresh plus one refresh/retry after an upstream `401`
- Usage-limit error normalization, per-session prompt caching, and model-aware reasoning clamps
- Account-aware Codex model discovery with private local cache and built-in offline fallback
- Idempotent, scoped edits to managed `[model.codex*]` entries with a config backup
- Grok skills and a `SessionStart` daemon hook

## Build and install

```bash
cargo test --workspace
cargo install --path crates/grok-codex-oauth-cli
grok-codex-oauth setup
grok-codex-oauth login
grok-codex-oauth models sync
grok-codex-oauth ensure-daemon
```

Then install the local plugin directory:

```bash
grok plugin install ./plugin --trust
```

To pick up later local plugin changes, reinstall it with the same command (or uninstall
`codex-oauth` first if the installed Grok version requires it).

Inside Grok:

```text
/codex-oauth:status
/model
/effort high
```

For a headless machine:

```bash
grok-codex-oauth login --device-code
```

## Commands

```text
login [--device-code]  Authenticate and store credentials
logout                 Delete credentials and stop the daemon
status [--json]        Report login/config/daemon state
setup                  Install or update the built-in selectable model catalog
models sync            Discover this account's models and update /model
uninstall-config       Remove only managed [model.codex*] entries
serve                  Run the foreground loopback proxy
ensure-daemon          Start the proxy if needed
doctor                 Check the local installation
logs                   Show or follow proxy diagnostic logs
```

`setup` creates `~/.grok/config.toml.bak` before changing an existing configuration. OAuth tokens
are never written into Grok configuration, environment variables, command arguments, or normal
logs.

`models sync` queries the authenticated Codex `/models` endpoint and adds every visible,
API-supported model to Grok's `/model` picker. A successful result is cached at
`~/.grok/codex-oauth/models-cache.json`. If the endpoint is temporarily unavailable, sync uses the
last cache; on a first-run offline machine it uses the built-in catalog.

The sync also installs each model's reasoning-effort menu and default. Grok 0.2.114 supports
`low`, `medium`, `high`, `xhigh`, and `max`; a backend-only `ultra` level is omitted until Grok can
represent it.

## Proxy diagnostics

The daemon prefers the Codex Responses WebSocket v2 transport and automatically falls back to
HTTP/SSE when the WebSocket handshake or initial send is unavailable. Grok still receives an SSE
response, so no Grok-side protocol change is required. Requests from the same Grok session reuse
one serialized WebSocket connection. A background connection pump continues reading between turns,
answers WebSocket Ping frames with Pong, and safely queues or filters private keepalive events.
When the complete next input is a safe extension of the previous turn and all other request
properties are unchanged, the proxy sends only the input delta with `previous_response_id`;
otherwise it conservatively sends the full request. Codex turn state is retained for tool
continuations and reset when a new user turn begins. Closed connections, initial send failures,
authorization expiry, and missing previous responses are reconnected or retried automatically
before falling back to HTTP/SSE.

The daemon writes timestamped request metadata to `~/.grok/codex-oauth/daemon.log`. Each valid
proxy request records the selected upstream transport, model and client request ID, upstream status
and request ID, connection-reuse and incremental-request decisions, response-header or
WebSocket-event latency, stream error type, and final outcome and duration. Stream error messages,
prompts, OAuth tokens, and response content are not included.
If Grok omits `x-request-id`, the proxy generates a `proxy-<pid>-<sequence>` correlation ID.
Cache diagnostics record only whether `prompt_cache_key` is present, a short SHA-256 fingerprint,
input and cached-input token counts, and the cache-hit percentage; the raw cache key is never logged.
When Grok omits `prompt_cache_key`, the proxy derives it from Grok's stable `x-grok-session-id`
and uses that Grok value as the Codex `session-id`. `thread-id` uses `x-grok-thread-id` when
available and otherwise falls back to the Grok session. A separate stable installation identifier
is sent only through WebSocket client metadata. An explicit request-body cache key always takes
precedence without changing either identity header.

```bash
grok-codex-oauth logs
grok-codex-oauth logs --follow
grok-codex-oauth logs --lines 500 --errors
grok-codex-oauth logs --request-id <request-id>
```

The final `outcome` distinguishes `completed`, `sse_error`, `http_error`,
`transport_error`, and `client_disconnected`. The daemon rotates the active log at 10 MiB and
keeps `daemon.log.1` through `daemon.log.3`.

## Test architecture

The project follows the same contract-first TDD structure as `device-hub`:

- unit tests lock down pure OAuth, storage, TOML, header, and JSON rules;
- service tests exercise refresh coordination;
- API integration tests start real Axum servers on random loopback ports and inject a mock
  upstream through the proxy composition seam.

Run focused tests while iterating:

```bash
cargo test -p codex-oauth
cargo test -p codex-models
cargo test -p codex-protocol
cargo test -p codex-proxy
cargo test -p grok-config
```
