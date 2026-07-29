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
- Expiry-window refresh plus one refresh/retry after an upstream `401`
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
```

`setup` creates `~/.grok/config.toml.bak` before changing an existing configuration. OAuth tokens
are never written into Grok configuration, environment variables, command arguments, or normal
logs.

`models sync` queries the authenticated Codex `/models` endpoint and adds every visible,
API-supported model to Grok's `/model` picker. A successful result is cached at
`~/.grok/codex-oauth/models-cache.json`. If the endpoint is temporarily unavailable, sync uses the
last cache; on a first-run offline machine it uses the built-in catalog.

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
