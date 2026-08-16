# Architecture

```text
Grok Responses client
        |
        | POST /v1/responses + random local bearer credential
        v
codex-proxy (127.0.0.1 only)
        |-- codex-protocol: stateless/streaming JSON, backend web-search events, and safe headers
        |-- codex-oauth: keyring/file credentials and refresh
        |
        | HTTPS, OAuth bearer, ChatGPT account ID
        v
https://chatgpt.com/backend-api/codex/responses
```

Crate boundaries:

- `codex-oauth` owns PKCE, state, token exchange/refresh, JWT claim extraction, and credential
  stores.
- `codex-models` owns authenticated model discovery, visibility filtering, stable Grok aliases,
  and the private on-disk catalog cache.
- `codex-protocol` owns all backend-specific JSON/header rules and incrementally bridges Codex
  output-item SSE events into the terminal `response.output` shape expected by Grok.
- `codex-proxy` owns HTTP routing, local authentication, limits, refresh/retry, and streaming.
- `grok-config` owns the managed `[model.codex*]` TOML namespaces while preserving unrelated
  user configuration.
- `grok-codex-oauth` is the composition root and lifecycle CLI.

The proxy configuration admits only the hard-coded production URL. Tests may use a separate
constructor that accepts only numeric loopback hosts; this is the injection seam for mock servers,
not a user-configurable general proxy.

Two upstream backends are supported, selected by whether an OpenAI-compatible config is present:

- `codex` (default): OAuth + ChatGPT/Codex Responses API, WebSocket incremental, explicit
  `prompt_cache_key`, web search via the OAuth backend.
- `openai`: any OpenAI-compatible provider via `base_url` + `api_key` (TOML or `.env`), in one of
  two wire formats (`wire_format = "chat-completions" | "responses"`; `responses_api = true` is
  the deprecated alias). Chat mode (default) talks to `{base_url}/chat/completions`: the Grok
  Responses body is translated to Chat messages and the SSE stream bridged back; implicit
  server-side prefix caching (no `prompt_cache_key`), web search dropped. Responses mode
  (e.g. DeepSeek) forwards the body nearly as-is to `{base_url}/responses`: `x_search` maps to
  the provider's `web_search` tool (server-side search) and the Codex backend's SSE bridge is
  reused. Per-provider compat flags (`stateless`, `supports_reasoning_effort`,
  `max_tokens_field`), a cache policy (`implicit` | `explicit` `prompt_cache_key`) and a USD
  cost table tune DeepSeek-specific assumptions; see `docs/plan-openai-compat-hardening.md`.

`codex-proxy/src/config.rs` owns backend parsing; `codex-proxy/src/backend/openai.rs` owns the
OpenAI request transforms (Chat and Responses) and the SSE bridge (reusing `CodexSseTransformer`
for the terminal `output` shape). Shared shell (auth, limits, diagnostics, cache accounting, SSE
framing) stays in `codex-proxy/src/lib.rs`. See `docs/plan-openai-backend.md`.
