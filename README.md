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
- Codex-native backend web search routed through the same loopback proxy and OAuth session
- Filtering of backend-only JSON `keepalive` events that Grok's Responses decoder cannot accept
- Expiry-window refresh plus one refresh/retry after an upstream `401`
- Usage-limit error normalization, per-session prompt caching, and model-aware reasoning clamps
- Grok system-prompt response guidelines rewritten to Codex-style communication at the proxy boundary
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

If outbound access requires an HTTP CONNECT proxy, set the standard proxy environment before
starting Grok or the daemon:

```bash
export HTTP_PROXY=http://127.0.0.1:7890
export HTTPS_PROXY=http://127.0.0.1:7890
export ALL_PROXY=http://127.0.0.1:7890
export NO_PROXY=127.0.0.1,localhost
```

The HTTP/SSE and WebSocket transports both honor these settings. WebSocket uses `HTTPS_PROXY`
first, then `ALL_PROXY`, and observes `NO_PROXY`. HTTP proxy URLs may include percent-encoded Basic
Auth credentials. WebSocket connection setup and individual sends are each limited to 10 seconds;
a failed send discards the cached connection and retries once before falling back to HTTP/SSE.
Waiting for HTTP/SSE response headers is limited to 30 seconds.

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
stop                   Stop the daemon without deleting credentials
restart [--port PORT]  Restart the daemon without changing credentials
status [--json]        Report login/config/daemon state
setup [--custom-instructions PATH] [--clear-instructions]
                       Install or update the built-in selectable model catalog
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

Pass `setup --custom-instructions rules.md` to append a Markdown file's contents to every request's
system prompt. The file is copied to `~/.grok/codex-oauth/additional-instructions.md` and read by
the proxy on startup. Re-running `setup` without the flag keeps the current copy; use
`setup --clear-instructions` to remove it. When the file is absent (the default), no extra
instructions are injected.

The proxy preserves Grok's rendered system prompt, including its tools, permissions, safety rules,
and agent loop, but replaces the `<response_guidelines>` section with the Codex-style policy stored
in `crates/codex-protocol/assets/codex-response-guidelines.md`. Grok's compact
`<output_efficiency>` variant is handled the same way. Prompts without either legacy section are
left unchanged. During compilation, `codex-protocol` fetches the current template from Grok Build's
`main` branch and generates the compatible template in Cargo's `OUT_DIR`. Set
`GROK_BUILD_PROMPT_REV` to a commit SHA for a reproducible build. Any custom instructions installed
with `setup --custom-instructions` are appended after the response guidelines.

`setup` marks the managed Codex model entries as backend-search capable and, when no explicit
non-Codex search model is configured, sets `[models].web_search` to `codex-sol`. Search requests
then use Codex's native Responses `web_search` tool through the loopback proxy instead of consuming
Grok/xAI search-model quota. The protocol bridge maps Grok's hosted `x_search` schema to Codex
`web_search` and fills the in-progress search action required by Grok's stream decoder. An
explicitly configured non-Codex search model is preserved.

`models sync` queries the authenticated Codex `/models` endpoint and adds every visible,
API-supported model to Grok's `/model` picker. A successful result is cached at
`~/.grok/codex-oauth/models-cache.json`. If the endpoint is temporarily unavailable, sync uses the
last cache; on a first-run offline machine it uses the built-in catalog.

The sync also installs each model's reasoning-effort menu and default. Grok 0.2.114 supports
`low`, `medium`, `high`, `xhigh`, and `max`; a backend-only `ultra` level is omitted until Grok can
represent it.

## DeepSeek (and other) OpenAI-compatible backends

Besides the default Codex OAuth backend, the proxy can drive any OpenAI-compatible upstream by
adding a `[backend.openai]` section to `~/.grok/codex-oauth/proxy.toml` (or a `backend.env`). See
`docs/plan-openai-backend.md` for the full multi-provider configuration, and
`docs/openai-provider-matrix.md` for the per-capability matrix (web search, reasoning, caching,
tool calls, cost).

Most providers (DeepSeek Chat, Groq, Together, opencode zen, ...) speak Chat
`/chat/completions`; in that mode web search is disabled and Grok's `x_search` tool is dropped.

Since DeepSeek-V4 now exposes a native **Responses API** with server-side `web_search` support, a
provider can opt into Responses mode with `responses_api = true`. The proxy then forwards the Grok
Responses body to `{base_url}/responses`, maps Grok's `x_search` to DeepSeek's `web_search` tool,
and bridges `web_search_call` events back through the same Codex SSE path. Web search defaults to
enabled in Responses mode (`web_search = true` follows `responses_api`) and can be disabled with
`web_search = false`.

The round trip is normalized both ways so neither decoder rejects prior searches: when Grok echoes
a `web_search_call` input item back, the proxy fills the `action` object and, for `search` actions,
the `queries` string list DeepSeek's deserializer requires; when DeepSeek returns
`web_search_call` actions that only carry `queries`/`url`, the bridge adds the `query` string
Grok's stream decoder requires.

```toml
[backend.openai]
base_url = "https://api.deepseek.com"     # or https://api.deepseek.com/v1
api_key = "${DEEPSEEK_API_KEY}"
model = "deepseek-v4-flash"
responses_api = true                      # Responses API mode (endpoint /responses)
web_search = true                         # optional; defaults to responses_api
[backend.openai.model_context_windows]
"deepseek-v4-flash" = 1048576
```

`.env` equivalents: `OPENAI_RESPONSES_API=1`, `OPENAI_WEB_SEARCH=1`. After changing the config,
restart the daemon to pick it up.

## Proxy diagnostics

The daemon prefers the Codex Responses WebSocket v2 transport and automatically falls back to
HTTP/SSE when the WebSocket handshake or initial send is unavailable. Grok still receives an SSE
response, so no Grok-side protocol change is required. Requests from the same Grok session reuse
one serialized WebSocket connection. A background connection pump continues reading between turns,
answers WebSocket Ping frames with Pong, and safely queues or filters private keepalive events.
When the complete next input is a safe extension of the previous turn and all other request
properties are unchanged, the proxy sends only the input delta with `previous_response_id`;
otherwise it conservatively sends the full request. Codex turn state is retained for tool
continuations and reset when a new user turn begins. Closed connections, initial send failures or
timeouts, authorization expiry, and missing previous responses are reconnected or retried
automatically before falling back to HTTP/SSE.

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

Only models in the built-in or synchronized Codex catalog are accepted. A native Grok model such
as `grok-4.5` is rejected locally instead of being forwarded with ChatGPT OAuth credentials.

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
