# Architecture

```text
Grok Responses client
        |
        | POST /v1/responses + random local bearer credential
        v
codex-proxy (127.0.0.1 only)
        |-- codex-protocol: stateless/streaming JSON and safe headers
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
