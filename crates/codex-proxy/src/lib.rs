//! Loopback-only Responses API proxy.

mod backend;
mod config;

use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use async_trait::async_trait;
use axum::{
    Json, Router,
    body::{Body, Bytes},
    extract::{DefaultBodyLimit, State},
    http::{HeaderMap, HeaderValue, StatusCode, header::CONTENT_TYPE},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use codex_oauth::{AccessCredential, CredentialStore, Credentials, OAuthClient};
use codex_protocol::{
    CodexSseTransformer, GROK_CODEX_SYSTEM_PROMPT_TEMPLATE, TransformOptions, UpstreamRequestIds,
};
use futures_util::StreamExt;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::{
    sync::{Mutex, Semaphore},
};
use url::Url;

pub use crate::config::{
    BackendKind, ConfigError, OpenAIBackendConfig, openai_backend_from_env_file,
    openai_backend_from_toml,
};

pub const CODEX_RESPONSES_URL: &str = "https://chatgpt.com/backend-api/codex/responses";
const DEFAULT_BODY_LIMIT: usize = 16 * 1024 * 1024;
const DEFAULT_MAX_CONCURRENCY: usize = 4;
const DEFAULT_WEBSOCKET_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_WEBSOCKET_SEND_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_HTTP_RESPONSE_HEADERS_TIMEOUT: Duration = Duration::from_secs(30);
static REQUEST_SEQUENCE: AtomicU64 = AtomicU64::new(1);

pub fn system_prompt_policy_fingerprint(additional_instructions: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(GROK_CODEX_SYSTEM_PROMPT_TEMPLATE.as_bytes());
    hasher.update([0]);
    hasher.update(additional_instructions.as_bytes());
    let digest = hasher.finalize();
    digest[..8]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

pub fn websocket_proxy_configured_from_env() -> Result<bool, ProxyError> {
    let upstream = Url::parse(CODEX_RESPONSES_URL)?;
    Ok(proxy_url_from_environment(&websocket_url(&upstream))?.is_some())
}


#[async_trait]
pub trait CredentialProvider: Send + Sync {
    async fn access_credential(&self) -> Result<AccessCredential, String>;
    async fn force_refresh(&self) -> Result<AccessCredential, String>;
}

pub struct StoredCredentialProvider {
    store: Arc<dyn CredentialStore>,
    oauth: OAuthClient,
    refresh_lock: Mutex<()>,
    refresh_window: Duration,
}

impl StoredCredentialProvider {
    pub fn new(store: Arc<dyn CredentialStore>, oauth: OAuthClient) -> Self {
        Self {
            store,
            oauth,
            refresh_lock: Mutex::new(()),
            refresh_window: Duration::from_secs(300),
        }
    }

    async fn load_required(&self) -> Result<Credentials, String> {
        self.store
            .load()
            .await
            .map_err(|_| "failed to read OAuth credentials".to_string())?
            .ok_or_else(|| "not logged in; run `grok-codex-oauth login`".to_string())
    }

    async fn refresh_current(&self, current: &Credentials) -> Result<Credentials, String> {
        let refreshed = self
            .oauth
            .refresh(current)
            .await
            .map_err(|error| format!("OAuth refresh failed: {error}"))?;
        self.store
            .save(&refreshed)
            .await
            .map_err(|_| "failed to persist refreshed credentials".to_string())?;
        Ok(refreshed)
    }
}

#[async_trait]
impl CredentialProvider for StoredCredentialProvider {
    async fn access_credential(&self) -> Result<AccessCredential, String> {
        let credentials = self.load_required().await?;
        if !credentials.needs_refresh(std::time::SystemTime::now(), self.refresh_window) {
            return Ok(credentials.access_credential());
        }

        let _guard = self.refresh_lock.lock().await;
        let latest = self.load_required().await?;
        if !latest.needs_refresh(std::time::SystemTime::now(), self.refresh_window) {
            return Ok(latest.access_credential());
        }
        Ok(self.refresh_current(&latest).await?.access_credential())
    }

    async fn force_refresh(&self) -> Result<AccessCredential, String> {
        let _guard = self.refresh_lock.lock().await;
        let current = self.load_required().await?;
        Ok(self.refresh_current(&current).await?.access_credential())
    }
}

#[derive(Clone, Debug)]
pub struct ProxyConfig {
    upstream_url: Url,
    websocket_url: Option<Url>,
    installation_id: String,
    local_secret: String,
    body_limit: usize,
    max_concurrency: usize,
    reasoning_options: HashMap<String, TransformOptions>,
    allowed_models: Option<HashSet<String>>,
    websocket_proxy: Option<Url>,
    websocket_connect_timeout: Duration,
    websocket_send_timeout: Duration,
    http_response_headers_timeout: Duration,
    allow_test_upstream: bool,
    additional_instructions: Option<String>,
    openai_backend: Option<OpenAIBackendConfig>,
}

impl ProxyConfig {
    /// The upstream backend this proxy drives.
    pub fn backend_kind(&self) -> BackendKind {
        if self.openai_backend.is_some() {
            BackendKind::Openai
        } else {
            BackendKind::Codex
        }
    }
    pub fn production(local_secret: impl Into<String>) -> Result<Self, ProxyError> {
        let local_secret = local_secret.into();
        let upstream_url = Url::parse(CODEX_RESPONSES_URL)?;
        Ok(Self {
            websocket_url: Some(websocket_url(&upstream_url)),
            upstream_url,
            installation_id: derive_installation_id(&local_secret),
            local_secret,
            body_limit: DEFAULT_BODY_LIMIT,
            max_concurrency: DEFAULT_MAX_CONCURRENCY,
            reasoning_options: HashMap::new(),
            allowed_models: Some(HashSet::new()),
            websocket_proxy: None,
            websocket_connect_timeout: DEFAULT_WEBSOCKET_CONNECT_TIMEOUT,
            websocket_send_timeout: DEFAULT_WEBSOCKET_SEND_TIMEOUT,
            http_response_headers_timeout: DEFAULT_HTTP_RESPONSE_HEADERS_TIMEOUT,
            allow_test_upstream: false,
            additional_instructions: None,
            openai_backend: None,
        })
    }

    pub fn for_test(upstream_url: impl AsRef<str>, local_secret: impl Into<String>) -> Self {
        let local_secret = local_secret.into();
        let upstream_url = Url::parse(upstream_url.as_ref()).unwrap_or_else(|_| {
            Url::parse("http://127.0.0.1/invalid").expect("static loopback fallback URL must parse")
        });
        Self {
            upstream_url,
            websocket_url: None,
            installation_id: derive_installation_id(&local_secret),
            local_secret,
            body_limit: DEFAULT_BODY_LIMIT,
            max_concurrency: DEFAULT_MAX_CONCURRENCY,
            reasoning_options: HashMap::new(),
            allowed_models: None,
            websocket_proxy: None,
            websocket_connect_timeout: DEFAULT_WEBSOCKET_CONNECT_TIMEOUT,
            websocket_send_timeout: DEFAULT_WEBSOCKET_SEND_TIMEOUT,
            http_response_headers_timeout: DEFAULT_HTTP_RESPONSE_HEADERS_TIMEOUT,
            allow_test_upstream: true,
            additional_instructions: None,
            openai_backend: None,
        }
    }

    /// Route traffic to an OpenAI Chat-compatible backend instead of Codex.
    pub fn with_openai_backend(mut self, backend: OpenAIBackendConfig) -> Self {
        self.openai_backend = Some(backend);
        self
    }

    pub fn with_limits(mut self, body_limit: usize, max_concurrency: usize) -> Self {
        self.body_limit = body_limit;
        self.max_concurrency = max_concurrency;
        self
    }

    pub fn with_installation_id(mut self, installation_id: impl Into<String>) -> Self {
        self.installation_id = installation_id.into();
        self
    }

    pub fn with_websocket_upstream(mut self, upstream_url: impl AsRef<str>) -> Self {
        self.websocket_url = Url::parse(upstream_url.as_ref()).ok();
        self
    }

    pub fn with_websocket_proxy(mut self, proxy_url: impl AsRef<str>) -> Result<Self, ProxyError> {
        self.websocket_proxy = Some(validate_websocket_proxy(proxy_url.as_ref())?);
        Ok(self)
    }

    pub fn with_websocket_proxy_from_env(mut self) -> Result<Self, ProxyError> {
        let Some(upstream) = self.websocket_url.as_ref() else {
            return Ok(self);
        };
        self.websocket_proxy = proxy_url_from_environment(upstream)?;
        Ok(self)
    }

    pub fn with_allowed_models(
        mut self,
        models: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.allowed_models = Some(models.into_iter().map(Into::into).collect());
        self
    }

    pub fn with_upstream_timeouts(
        mut self,
        websocket_connect: Duration,
        http_response_headers: Duration,
    ) -> Self {
        self.websocket_connect_timeout = websocket_connect;
        self.http_response_headers_timeout = http_response_headers;
        self
    }

    pub fn with_websocket_send_timeout(mut self, timeout: Duration) -> Self {
        self.websocket_send_timeout = timeout;
        self
    }

    pub fn with_model_reasoning(
        mut self,
        model: impl Into<String>,
        allowed_reasoning_efforts: Vec<String>,
        default_reasoning_effort: Option<String>,
    ) -> Self {
        if allowed_reasoning_efforts.is_empty() {
            return self;
        }
        let default_reasoning_effort = default_reasoning_effort
            .filter(|default| allowed_reasoning_efforts.contains(default))
            .unwrap_or_else(|| allowed_reasoning_efforts[0].clone());
        self.reasoning_options.insert(
            model.into(),
            TransformOptions {
                allowed_reasoning_efforts,
                default_reasoning_effort,
            },
        );
        self
    }

    pub fn with_additional_instructions(mut self, additional: impl Into<String>) -> Self {
        let additional = additional.into();
        self.additional_instructions = (!additional.trim().is_empty()).then_some(additional);
        self
    }

    fn transform_options(&self, request: &Value) -> TransformOptions {
        request
            .get("model")
            .and_then(Value::as_str)
            .and_then(|model| self.reasoning_options.get(model))
            .cloned()
            .unwrap_or_default()
    }

    fn validate(&self) -> Result<(), ProxyError> {
        if self.local_secret.is_empty() {
            return Err(ProxyError::EmptyLocalSecret);
        }
        if self.installation_id.is_empty() {
            return Err(ProxyError::InvalidInstallationId);
        }
        if self.body_limit == 0 || self.max_concurrency == 0 {
            return Err(ProxyError::InvalidLimit);
        }
        if self.websocket_connect_timeout.is_zero()
            || self.websocket_send_timeout.is_zero()
            || self.http_response_headers_timeout.is_zero()
        {
            return Err(ProxyError::InvalidTimeout);
        }
        if !self.allow_test_upstream && self.upstream_url.as_str() != CODEX_RESPONSES_URL {
            return Err(ProxyError::UpstreamNotAllowed);
        }
        if self.allow_test_upstream
            && (!is_loopback_url(&self.upstream_url)
                || self
                    .websocket_url
                    .as_ref()
                    .is_some_and(|url| !is_loopback_url(url)))
        {
            return Err(ProxyError::UpstreamNotAllowed);
        }
        Ok(())
    }
}

fn websocket_url(upstream_url: &Url) -> Url {
    let mut websocket_url = upstream_url.clone();
    let scheme = match upstream_url.scheme() {
        "http" => "ws",
        "https" => "wss",
        _ => return websocket_url,
    };
    let _ = websocket_url.set_scheme(scheme);
    websocket_url
}

fn is_loopback_url(url: &Url) -> bool {
    url.host_str()
        .and_then(|host| host.parse::<std::net::IpAddr>().ok())
        .is_some_and(|address| address.is_loopback())
}

fn validate_websocket_proxy(proxy_url: &str) -> Result<Url, ProxyError> {
    let proxy = Url::parse(proxy_url)?;
    if proxy.scheme() != "http" || proxy.host_str().is_none() {
        return Err(ProxyError::UnsupportedWebSocketProxy);
    }
    Ok(proxy)
}

fn proxy_url_from_environment(upstream: &Url) -> Result<Option<Url>, ProxyError> {
    proxy_url_from_values(upstream, |name| std::env::var(name).ok())
}

fn proxy_url_from_values(
    upstream: &Url,
    value: impl Fn(&str) -> Option<String>,
) -> Result<Option<Url>, ProxyError> {
    let no_proxy = value("NO_PROXY").or_else(|| value("no_proxy"));
    if no_proxy
        .as_deref()
        .is_some_and(|list| no_proxy_matches(upstream, list))
    {
        return Ok(None);
    }
    let proxy = value("HTTPS_PROXY")
        .or_else(|| value("https_proxy"))
        .or_else(|| value("ALL_PROXY"))
        .or_else(|| value("all_proxy"));
    proxy
        .filter(|proxy| !proxy.trim().is_empty())
        .map(|proxy| validate_websocket_proxy(proxy.trim()))
        .transpose()
}

fn no_proxy_matches(upstream: &Url, list: &str) -> bool {
    let Some(host) = upstream.host_str() else {
        return false;
    };
    let port = upstream.port_or_known_default();
    list.split(',').any(|entry| {
        let entry = entry.trim();
        if entry == "*" {
            return true;
        }
        if entry.is_empty() {
            return false;
        }
        let (pattern, expected_port) = match entry.rsplit_once(':') {
            Some((pattern, port)) if port.parse::<u16>().is_ok() => {
                (pattern, port.parse::<u16>().ok())
            }
            _ => (entry, None),
        };
        if expected_port.is_some() && expected_port != port {
            return false;
        }
        let pattern = pattern.trim_start_matches('.');
        host.eq_ignore_ascii_case(pattern)
            || host
                .to_ascii_lowercase()
                .ends_with(&format!(".{}", pattern.to_ascii_lowercase()))
    })
}

fn derive_installation_id(local_secret: &str) -> String {
    let digest = Sha256::digest(format!("grok-codex-oauth-installation:{local_secret}").as_bytes());
    let fingerprint = digest[..16]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    format!("grok-installation-{fingerprint}")
}

#[derive(Clone)]
pub(crate) struct AppState {
    pub(crate) config: ProxyConfig,
    pub(crate) credentials: Arc<dyn CredentialProvider>,
    pub(crate) client: reqwest::Client,
    pub(crate) concurrency: Arc<Semaphore>,
    pub(crate) websocket_sessions: Arc<backend::codex::WebSocketSessionPool>,
}

pub fn build_router(
    config: ProxyConfig,
    credentials: Arc<dyn CredentialProvider>,
) -> Result<Router, ProxyError> {
    config.validate()?;
    let body_limit = config.body_limit;
    let concurrency = config.max_concurrency;
    let state = AppState {
        config,
        credentials,
        client: reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(20))
            .build()
            .map_err(ProxyError::HttpClient)?,
        concurrency: Arc::new(Semaphore::new(concurrency)),
        websocket_sessions: Arc::new(backend::codex::WebSocketSessionPool::default()),
    };
    Ok(Router::new()
        .route("/health", get(health))
        .route("/v1/responses", post(proxy_responses))
        .layer(DefaultBodyLimit::max(body_limit))
        .with_state(state))
}

async fn health(State(state): State<AppState>) -> Json<Value> {
    Json(json!({
        "status": "ok",
        "backend": match state.config.backend_kind() {
            BackendKind::Codex => "codex",
            BackendKind::Openai => "openai",
        },
        "websocket_proxy": state.config.websocket_proxy.is_some(),
        "system_prompt_policy_fingerprint": system_prompt_policy_fingerprint(
            state.config.additional_instructions.as_deref().unwrap_or_default()
        )
    }))
}

async fn proxy_responses(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let request_started_at = Instant::now();
    if !valid_local_secret(&headers, &state.config.local_secret) {
        return error_response(StatusCode::UNAUTHORIZED, "invalid local proxy credential");
    }
    let _permit = match state.concurrency.clone().try_acquire_owned() {
        Ok(permit) => permit,
        Err(_) => {
            return error_response(
                StatusCode::TOO_MANY_REQUESTS,
                "local proxy concurrency limit reached",
            );
        }
    };
    let parsed = match serde_json::from_slice::<Value>(&body) {
        Ok(parsed) => parsed,
        Err(_) => {
            return error_response(StatusCode::BAD_REQUEST, "request body must be valid JSON");
        }
    };
    // The Codex model allowlist only applies to the Codex backend; the
    // OpenAI-compatible backend uses the provider's own model set.
    if state.config.openai_backend.is_none()
        && let Some(allowed_models) = &state.config.allowed_models
    {
        let model = parsed.get("model").and_then(Value::as_str);
        if !model.is_some_and(|model| allowed_models.contains(model)) {
            let model = model.unwrap_or("<missing>");
            tracing::warn!(model, "Rejected unsupported model at Codex proxy boundary");
            return error_response(
                StatusCode::BAD_REQUEST,
                &format!("unsupported Codex model `{model}`"),
            );
        }
    }
    if state.config.openai_backend.is_some() {
        let client_request_id = headers
            .get("x-request-id")
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned)
            .unwrap_or_else(|| format!("proxy-{}-{}", std::process::id(), REQUEST_SEQUENCE.fetch_add(1, Ordering::Relaxed)));
        return backend::openai::handle_openai(&state, &headers, &parsed, &client_request_id).await;
    }

    backend::codex::handle_codex(&state, &headers, parsed, request_started_at).await
}

fn valid_local_secret(headers: &HeaderMap, expected: &str) -> bool {
    let supplied = headers
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "));
    supplied.is_some_and(|value| constant_time_equal(value.as_bytes(), expected.as_bytes()))
}

fn constant_time_equal(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right)
        .fold(0_u8, |difference, (left, right)| {
            difference | (left ^ right)
        })
        == 0
}

async fn stream_response(upstream: reqwest::Response, diagnostics: RequestDiagnostics) -> Response {
    let status = upstream.status();
    let source_headers = upstream.headers().clone();
    if !status.is_success() {
        let body = upstream.bytes().await.unwrap_or_default();
        let response_status = normalize_upstream_error_status(status, &body);
        tracing::warn!(
            upstream_status = %status,
            upstream_error = %sanitize_error_body(&body),
            "Codex upstream rejected the request"
        );
        diagnostics.log_completed(
            "http_error",
            &header_value(&source_headers, "x-request-id"),
            None,
            CompletionMetrics {
                bytes_received: body.len(),
                chunks_received: 1,
                ..CompletionMetrics::default()
            },
        );
        let mut response = Response::new(Body::from(body));
        *response.status_mut() = response_status;
        copy_response_headers(&source_headers, &mut response);
        return response;
    }
    let upstream_request_id = header_value(&source_headers, "x-request-id");
    let stream = bridge_sse_stream(upstream, diagnostics, upstream_request_id);
    let mut response = Response::new(Body::from_stream(stream));
    *response.status_mut() = status;
    copy_response_headers(&source_headers, &mut response);
    response.headers_mut().insert(
        "x-codex-upstream-transport",
        HeaderValue::from_static("http-sse"),
    );
    response
}

pub(crate) fn normalize_upstream_error_status(status: reqwest::StatusCode, body: &[u8]) -> StatusCode {
    if status != reqwest::StatusCode::NOT_FOUND {
        return StatusCode::from_u16(status.as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
    }
    let usage_limited = std::str::from_utf8(body).ok().is_some_and(|body| {
        let normalized = body.to_ascii_lowercase();
        [
            "usage_limit_reached",
            "usage_not_included",
            "rate_limit_exceeded",
            "usage limit",
        ]
        .iter()
        .any(|marker| normalized.contains(marker))
    });
    if usage_limited {
        StatusCode::TOO_MANY_REQUESTS
    } else {
        StatusCode::NOT_FOUND
    }
}

fn bridge_sse_stream(
    upstream: reqwest::Response,
    diagnostics: RequestDiagnostics,
    upstream_request_id: String,
) -> impl futures_util::Stream<Item = Result<Bytes, std::io::Error>> {
    let source = upstream.bytes_stream();
    async_stream::stream! {
        let mut source = std::pin::pin!(source);
        let mut transformer = CodexSseTransformer::default();
        let mut stream_diagnostics =
            StreamDiagnostics::new(diagnostics, upstream_request_id);
        while let Some(chunk) = source.next().await {
            let chunk = match chunk {
                Ok(chunk) => chunk,
                Err(error) => {
                    stream_diagnostics.log_transport_error();
                    stream_diagnostics.finish("transport_error");
                    yield Err(std::io::Error::other(error));
                    return;
                }
            };
            stream_diagnostics.observe_chunk(&chunk);
            let events = transformer.push(&chunk);
            stream_diagnostics.observe_filtered_keepalives(
                transformer.take_filtered_keepalive_events(),
            );
            for event in events {
                stream_diagnostics.observe_event(&event);
                yield Ok(Bytes::from(event));
            }
        }
        let remainders = transformer.finish();
        stream_diagnostics.observe_filtered_keepalives(
            transformer.take_filtered_keepalive_events(),
        );
        for remainder in remainders {
            stream_diagnostics.observe_event(&remainder);
            yield Ok(Bytes::from(remainder));
        }
        let outcome = if stream_diagnostics.sse_error_type.is_some() {
            "sse_error"
        } else {
            "completed"
        };
        stream_diagnostics.finish(outcome);
    }
}

#[derive(Clone)]
pub(crate) struct RequestDiagnostics {
    started_at: Instant,
    client_request_id: String,
    model: String,
    session_id_fingerprint: String,
    thread_id_fingerprint: String,
    prompt_cache_key_fingerprint: Option<String>,
}

impl RequestDiagnostics {
    fn new(
        started_at: Instant,
        headers: &HeaderMap,
        request: &Value,
        ids: &UpstreamRequestIds,
    ) -> Self {
        Self {
            started_at,
            client_request_id: headers
                .get("x-request-id")
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned)
                .unwrap_or_else(|| {
                    format!(
                        "proxy-{}-{}",
                        std::process::id(),
                        REQUEST_SEQUENCE.fetch_add(1, Ordering::Relaxed)
                    )
                }),
            model: request
                .get("model")
                .and_then(Value::as_str)
                .unwrap_or("<missing>")
                .to_owned(),
            session_id_fingerprint: identifier_fingerprint(&ids.session_id),
            thread_id_fingerprint: identifier_fingerprint(&ids.thread_id),
            prompt_cache_key_fingerprint: request
                .get("prompt_cache_key")
                .and_then(Value::as_str)
                .filter(|key| !key.is_empty())
                .map(cache_key_fingerprint),
        }
    }

    /// Build diagnostics for a backend whose wire request never carries an
    /// explicit `prompt_cache_key` (the OpenAI-compatible backend).
    pub(crate) fn new_openai(started_at: Instant, headers: &HeaderMap, request: &Value) -> Self {
        let codex = Self::new(
            started_at,
            headers,
            request,
            &UpstreamRequestIds {
                installation_id: String::new(),
                session_id: String::new(),
                thread_id: String::new(),
                prompt_cache_key: None,
            },
        );
        Self {
            prompt_cache_key_fingerprint: None,
            ..codex
        }
    }

    fn log_received(&self) {
        tracing::info!(
            client_request_id = self.client_request_id.as_str(),
            model = self.model.as_str(),
            session_id_fingerprint = self.session_id_fingerprint.as_str(),
            thread_id_fingerprint = self.thread_id_fingerprint.as_str(),
            prompt_cache_key_present = self.prompt_cache_key_fingerprint.is_some(),
            prompt_cache_key_fingerprint = self
                .prompt_cache_key_fingerprint
                .as_deref()
                .unwrap_or("<none>"),
            "Codex proxy request received"
        );
    }

    fn log_response_headers(&self, response: &reqwest::Response, attempt: u64) {
        let upstream_request_id = header_value(response.headers(), "x-request-id");
        tracing::info!(
            client_request_id = self.client_request_id.as_str(),
            upstream_request_id = upstream_request_id.as_str(),
            model = self.model.as_str(),
            attempt,
            upstream_status = response.status().as_u16(),
            response_headers_ms = elapsed_ms(self.started_at),
            "Codex upstream response headers received"
        );
    }

    fn log_request_failed(&self, attempt: u64) {
        tracing::warn!(
            client_request_id = self.client_request_id.as_str(),
            model = self.model.as_str(),
            attempt,
            total_ms = elapsed_ms(self.started_at),
            outcome = "upstream_request_error",
            "Codex upstream request failed before response headers"
        );
    }

    fn log_completed(
        &self,
        outcome: &str,
        upstream_request_id: &str,
        sse_error_type: Option<&str>,
        metrics: CompletionMetrics,
    ) {
        let cache_usage_present = metrics.input_tokens.is_some();
        let input_tokens = metrics.input_tokens.unwrap_or(0);
        let cached_input_tokens = metrics.cached_input_tokens.unwrap_or(0);
        let cache_hit_ratio_pct = if input_tokens == 0 {
            0.0
        } else {
            cached_input_tokens as f64 * 100.0 / input_tokens as f64
        };
        tracing::info!(
            client_request_id = self.client_request_id.as_str(),
            upstream_request_id,
            model = self.model.as_str(),
            outcome,
            sse_error_type = sse_error_type.unwrap_or("<none>"),
            total_ms = elapsed_ms(self.started_at),
            bytes_received = metrics.bytes_received,
            chunks_received = metrics.chunks_received,
            filtered_keepalive_events = metrics.filtered_keepalive_events,
            cache_usage_present,
            input_tokens,
            cached_input_tokens,
            cache_hit_ratio_pct,
            "Codex proxy request completed"
        );
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct CompletionMetrics {
    bytes_received: usize,
    chunks_received: u64,
    filtered_keepalive_events: u64,
    input_tokens: Option<u64>,
    cached_input_tokens: Option<u64>,
}

fn identifier_fingerprint(key: &str) -> String {
    Sha256::digest(key.as_bytes())[..8]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn cache_key_fingerprint(key: &str) -> String {
    identifier_fingerprint(key)
}

struct StreamDiagnostics {
    request: RequestDiagnostics,
    upstream_request_id: String,
    first_bytes_logged: bool,
    sse_error_type: Option<String>,
    bytes_received: usize,
    chunks_received: u64,
    filtered_keepalive_events: u64,
    input_tokens: Option<u64>,
    cached_input_tokens: Option<u64>,
    finished: bool,
}

impl StreamDiagnostics {
    fn new(request: RequestDiagnostics, upstream_request_id: String) -> Self {
        Self {
            request,
            upstream_request_id,
            first_bytes_logged: false,
            sse_error_type: None,
            bytes_received: 0,
            chunks_received: 0,
            filtered_keepalive_events: 0,
            input_tokens: None,
            cached_input_tokens: None,
            finished: false,
        }
    }

    fn observe_chunk(&mut self, chunk: &[u8]) {
        self.bytes_received = self.bytes_received.saturating_add(chunk.len());
        self.chunks_received = self.chunks_received.saturating_add(1);
        if self.first_bytes_logged || chunk.is_empty() {
            return;
        }
        self.first_bytes_logged = true;
        tracing::info!(
            client_request_id = self.request.client_request_id.as_str(),
            upstream_request_id = self.upstream_request_id.as_str(),
            model = self.request.model.as_str(),
            ttfb_ms = elapsed_ms(self.request.started_at),
            first_chunk_bytes = chunk.len(),
            "Codex upstream first SSE bytes received"
        );
    }

    fn observe_event(&mut self, event: &[u8]) {
        if let Some((input_tokens, cached_input_tokens)) = extract_cache_usage(event) {
            self.input_tokens = Some(input_tokens);
            self.cached_input_tokens = Some(cached_input_tokens);
        }
        let Some(error_type) = extract_sse_error_type(event) else {
            return;
        };
        tracing::warn!(
            client_request_id = self.request.client_request_id.as_str(),
            upstream_request_id = self.upstream_request_id.as_str(),
            model = self.request.model.as_str(),
            sse_error_type = error_type.as_str(),
            elapsed_ms = elapsed_ms(self.request.started_at),
            "Codex upstream SSE error received"
        );
        self.sse_error_type = Some(error_type);
    }

    fn observe_filtered_keepalives(&mut self, count: u64) {
        self.filtered_keepalive_events = self.filtered_keepalive_events.saturating_add(count);
    }

    fn log_transport_error(&self) {
        tracing::warn!(
            client_request_id = self.request.client_request_id.as_str(),
            upstream_request_id = self.upstream_request_id.as_str(),
            model = self.request.model.as_str(),
            elapsed_ms = elapsed_ms(self.request.started_at),
            "Codex upstream SSE transport failed"
        );
    }

    fn finish(&mut self, outcome: &str) {
        if self.finished {
            return;
        }
        self.finished = true;
        self.request.log_completed(
            outcome,
            &self.upstream_request_id,
            self.sse_error_type.as_deref(),
            self.completion_metrics(),
        );
    }

    fn completion_metrics(&self) -> CompletionMetrics {
        CompletionMetrics {
            bytes_received: self.bytes_received,
            chunks_received: self.chunks_received,
            filtered_keepalive_events: self.filtered_keepalive_events,
            input_tokens: self.input_tokens,
            cached_input_tokens: self.cached_input_tokens,
        }
    }
}

impl Drop for StreamDiagnostics {
    fn drop(&mut self) {
        if !self.finished {
            self.request.log_completed(
                "client_disconnected",
                &self.upstream_request_id,
                self.sse_error_type.as_deref(),
                self.completion_metrics(),
            );
        }
    }
}

pub(crate) fn extract_cache_usage(event: &[u8]) -> Option<(u64, u64)> {
    let text = std::str::from_utf8(event).ok()?;
    let data = text.lines().find_map(|line| {
        line.strip_prefix("data:")
            .map(str::trim_start)
            .filter(|data| !data.is_empty() && *data != "[DONE]")
    })?;
    let value = serde_json::from_str::<Value>(data).ok()?;
    let input_tokens = [
        "/response/usage/input_tokens",
        "/usage/input_tokens",
        "/usage/prompt_tokens",
    ]
    .iter()
    .find_map(|pointer| value.pointer(pointer).and_then(Value::as_u64))?;
    let cached_input_tokens = [
        "/response/usage/input_tokens_details/cached_tokens",
        "/usage/input_tokens_details/cached_tokens",
        "/usage/prompt_tokens_details/cached_tokens",
    ]
    .iter()
    .find_map(|pointer| value.pointer(pointer).and_then(Value::as_u64))
    .unwrap_or(0);
    Some((input_tokens, cached_input_tokens))
}

pub(crate) fn extract_sse_error_type(event: &[u8]) -> Option<String> {
    let text = std::str::from_utf8(event).ok()?;
    let data = text.lines().find_map(|line| {
        line.strip_prefix("data:")
            .map(str::trim_start)
            .filter(|data| !data.is_empty() && *data != "[DONE]")
    })?;
    let value = serde_json::from_str::<Value>(data).ok()?;
    let event_type = value.get("type").and_then(Value::as_str)?;
    if event_type != "error" && !event_type.ends_with(".failed") {
        return None;
    }
    for pointer in [
        "/error/type",
        "/error/code",
        "/response/error/type",
        "/response/error/code",
        "/error_type",
        "/code",
    ] {
        if let Some(error_type) = value.pointer(pointer).and_then(Value::as_str) {
            return Some(error_type.to_owned());
        }
    }
    Some(event_type.to_owned())
}

pub(crate) fn header_value(headers: &HeaderMap, name: &'static str) -> String {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("<missing>")
        .to_owned()
}

pub(crate) fn elapsed_ms(started_at: Instant) -> u64 {
    started_at
        .elapsed()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

pub(crate) fn copy_response_headers(source_headers: &HeaderMap, response: &mut Response) {
    for name in [
        "content-type",
        "x-request-id",
        "retry-after",
        "x-ratelimit-limit-requests",
        "x-ratelimit-remaining-requests",
        "x-ratelimit-reset-requests",
    ] {
        if let Some(value) = source_headers.get(name)
            && let Ok(header_name) = http::header::HeaderName::from_bytes(name.as_bytes())
        {
            response.headers_mut().insert(header_name, value.clone());
        }
    }
}

pub(crate) fn sanitize_error_body(body: &[u8]) -> String {
    const MAX_DIAGNOSTIC_BYTES: usize = 4096;
    let truncated = &body[..body.len().min(MAX_DIAGNOSTIC_BYTES)];
    let Ok(mut value) = serde_json::from_slice::<Value>(truncated) else {
        return "non-JSON upstream error body".into();
    };
    redact_sensitive_fields(&mut value);
    value.to_string()
}

pub(crate) fn redact_sensitive_fields(value: &mut Value) {
    match value {
        Value::Object(object) => {
            for (key, value) in object {
                let normalized = key.to_ascii_lowercase();
                if normalized.contains("token")
                    || normalized.contains("authorization")
                    || normalized.contains("cookie")
                    || normalized.contains("account")
                {
                    *value = Value::String("<redacted>".into());
                } else {
                    redact_sensitive_fields(value);
                }
            }
        }
        Value::Array(array) => {
            for value in array {
                redact_sensitive_fields(value);
            }
        }
        _ => {}
    }
}

pub(crate) fn error_response(status: StatusCode, message: &str) -> Response {
    let mut response = Json(json!({"error": message})).into_response();
    *response.status_mut() = status;
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    response
}

#[derive(Debug, Error)]
pub enum ProxyError {
    #[error("local proxy credential must not be empty")]
    EmptyLocalSecret,
    #[error("proxy installation ID must not be empty")]
    InvalidInstallationId,
    #[error("proxy body and concurrency limits must be positive")]
    InvalidLimit,
    #[error("proxy upstream timeouts must be positive")]
    InvalidTimeout,
    #[error("WebSocket proxy must be an HTTP URL with a host")]
    UnsupportedWebSocketProxy,
    #[error("upstream URL is not allowlisted")]
    UpstreamNotAllowed,
    #[error(transparent)]
    Url(#[from] url::ParseError),
    #[error("failed to create HTTP client")]
    HttpClient(#[source] reqwest::Error),
}

#[cfg(test)]
mod tests {
    use super::{
        RequestDiagnostics, derive_installation_id, extract_sse_error_type,
        normalize_upstream_error_status, proxy_url_from_values, sanitize_error_body,
    };
    use crate::backend::codex::{
        CachedWebSocketResponse, MAX_CACHED_WEBSOCKET_SESSIONS, WebSocketSession,
        WebSocketSessionPool, prepare_websocket_request, proxy_authorization,
        websocket_terminal_response_has_empty_output,
    };
    use http::{HeaderMap, StatusCode};
    use serde_json::json;
    use std::{collections::HashMap, time::Instant};
    use url::Url;

    #[test]
    fn websocket_proxy_environment_honors_precedence_and_no_proxy() {
        let upstream =
            Url::parse("wss://chatgpt.com/backend-api/codex/responses").expect("upstream URL");
        let values = HashMap::from([
            ("HTTPS_PROXY", "http://127.0.0.1:7890"),
            ("ALL_PROXY", "http://127.0.0.1:9999"),
        ]);
        let selected =
            proxy_url_from_values(&upstream, |name| values.get(name).map(ToString::to_string))
                .expect("proxy selection")
                .expect("selected proxy");
        assert_eq!(selected.as_str(), "http://127.0.0.1:7890/");

        let values = HashMap::from([
            ("HTTPS_PROXY", "http://127.0.0.1:7890"),
            ("NO_PROXY", ".chatgpt.com,localhost"),
        ]);
        assert!(
            proxy_url_from_values(&upstream, |name| {
                values.get(name).map(ToString::to_string)
            })
            .expect("proxy selection")
            .is_none()
        );
    }

    #[test]
    fn websocket_proxy_basic_auth_decodes_url_credentials() {
        let proxy = Url::parse("http://user%40name:p%3Ass@127.0.0.1:7890").expect("proxy URL");
        assert_eq!(
            proxy_authorization(&proxy).expect("proxy authorization"),
            Some("dXNlckBuYW1lOnA6c3M=".to_string())
        );
    }

    #[test]
    fn upstream_diagnostic_redacts_sensitive_fields() {
        let body = br#"{
            "error": {
                "message": "Unsupported parameter",
                "access_token": "secret",
                "chatgpt_account_id": "account-secret"
            }
        }"#;

        let diagnostic = sanitize_error_body(body);

        assert!(diagnostic.contains("Unsupported parameter"));
        assert!(!diagnostic.contains("secret"));
        assert!(diagnostic.contains("<redacted>"));
    }

    #[test]
    fn sse_error_type_supports_top_level_and_nested_openai_shapes() {
        let top_level = br#"data: {"type":"error","code":"server_error","message":"do not log"}"#;
        let nested = br#"data: {"type":"response.failed","response":{"error":{"type":"service_unavailable_error","message":"do not log"}}}"#;

        assert_eq!(
            extract_sse_error_type(top_level).as_deref(),
            Some("server_error")
        );
        assert_eq!(
            extract_sse_error_type(nested).as_deref(),
            Some("service_unavailable_error")
        );
        assert_eq!(
            extract_sse_error_type(br#"data: {"type":"response.completed"}"#),
            None
        );
    }

    #[test]
    fn empty_terminal_response_detection_excludes_failures_and_nonempty_output() {
        for kind in ["response.completed", "response.incomplete"] {
            assert!(websocket_terminal_response_has_empty_output(
                &json!({"type": kind, "response": {"output": []}}).to_string()
            ));
            assert!(websocket_terminal_response_has_empty_output(
                &json!({"type": kind, "response": {}}).to_string()
            ));
            assert!(!websocket_terminal_response_has_empty_output(
                &json!({"type": kind, "response": {"output": [{}]}}).to_string()
            ));
        }
        assert!(!websocket_terminal_response_has_empty_output(
            r#"{"type":"response.failed","response":{"output":[]}}"#
        ));
    }

    #[test]
    fn request_diagnostics_generates_a_correlation_id_when_grok_omits_one() {
        let diagnostics = RequestDiagnostics::new(
            Instant::now(),
            &HeaderMap::new(),
            &json!({"model":"test"}),
            &codex_protocol::UpstreamRequestIds {
                installation_id: "installation".into(),
                session_id: "session".into(),
                thread_id: "thread".into(),
                prompt_cache_key: None,
            },
        );

        assert!(diagnostics.client_request_id.starts_with("proxy-"));
        assert_ne!(diagnostics.client_request_id, "<missing>");
    }

    #[test]
    fn installation_id_is_stable_without_exposing_the_proxy_secret() {
        let first = derive_installation_id("local-secret");
        let second = derive_installation_id("local-secret");
        let different = derive_installation_id("other-secret");

        assert_eq!(first, second);
        assert_ne!(first, different);
        assert!(!first.contains("local-secret"));
    }

    #[test]
    fn incremental_request_falls_back_to_full_when_non_input_properties_change() {
        let previous = json!({
            "model": "gpt-test",
            "tools": [{"type": "function", "name": "first"}],
            "input": [{"type": "message", "role": "user", "content": "one"}]
        });
        let current = json!({
            "model": "gpt-test",
            "tools": [{"type": "function", "name": "changed"}],
            "input": [
                {"type": "message", "role": "user", "content": "one"},
                {"type": "message", "role": "user", "content": "two"}
            ]
        });
        let session = WebSocketSession {
            last_request: Some(previous),
            last_response: Some(CachedWebSocketResponse {
                response_id: "resp-1".into(),
                output: Vec::new(),
            }),
            ..WebSocketSession::default()
        };

        let prepared = prepare_websocket_request(&current, &session);

        assert!(!prepared.incremental);
        assert!(prepared.payload.get("previous_response_id").is_none());
        assert_eq!(prepared.payload["input"], current["input"]);
    }

    #[tokio::test]
    async fn websocket_session_pool_stays_bounded() {
        let pool = WebSocketSessionPool::default();
        for index in 0..MAX_CACHED_WEBSOCKET_SESSIONS + 10 {
            let session = pool.get(format!("session-{index}")).await;
            drop(session);
        }

        assert_eq!(
            pool.sessions.lock().await.len(),
            MAX_CACHED_WEBSOCKET_SESSIONS
        );
    }

    #[test]
    fn only_usage_related_404_errors_are_normalized_to_rate_limits() {
        for code in [
            "usage_limit_reached",
            "usage_not_included",
            "rate_limit_exceeded",
        ] {
            let body = format!(r#"{{"error":{{"code":"{code}"}}}}"#);
            assert_eq!(
                normalize_upstream_error_status(reqwest::StatusCode::NOT_FOUND, body.as_bytes()),
                StatusCode::TOO_MANY_REQUESTS
            );
        }
        assert_eq!(
            normalize_upstream_error_status(
                reqwest::StatusCode::NOT_FOUND,
                br#"{"error":{"code":"model_not_found"}}"#
            ),
            StatusCode::NOT_FOUND
        );
    }
}
