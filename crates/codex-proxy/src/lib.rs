//! Loopback-only Responses API proxy.

use std::{
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
    CodexSseTransformer, TransformOptions, build_upstream_headers, transform_responses_request,
};
use futures_util::StreamExt;
use serde_json::{Value, json};
use thiserror::Error;
use tokio::sync::{Mutex, Semaphore};
use url::Url;

pub const CODEX_RESPONSES_URL: &str = "https://chatgpt.com/backend-api/codex/responses";
const DEFAULT_BODY_LIMIT: usize = 16 * 1024 * 1024;
const DEFAULT_MAX_CONCURRENCY: usize = 4;
static REQUEST_SEQUENCE: AtomicU64 = AtomicU64::new(1);

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
    local_secret: String,
    body_limit: usize,
    max_concurrency: usize,
    allow_test_upstream: bool,
}

impl ProxyConfig {
    pub fn production(local_secret: impl Into<String>) -> Result<Self, ProxyError> {
        Ok(Self {
            upstream_url: Url::parse(CODEX_RESPONSES_URL)?,
            local_secret: local_secret.into(),
            body_limit: DEFAULT_BODY_LIMIT,
            max_concurrency: DEFAULT_MAX_CONCURRENCY,
            allow_test_upstream: false,
        })
    }

    pub fn for_test(upstream_url: impl AsRef<str>, local_secret: impl Into<String>) -> Self {
        let upstream_url = Url::parse(upstream_url.as_ref()).unwrap_or_else(|_| {
            Url::parse("http://127.0.0.1/invalid").expect("static loopback fallback URL must parse")
        });
        Self {
            upstream_url,
            local_secret: local_secret.into(),
            body_limit: DEFAULT_BODY_LIMIT,
            max_concurrency: DEFAULT_MAX_CONCURRENCY,
            allow_test_upstream: true,
        }
    }

    pub fn with_limits(mut self, body_limit: usize, max_concurrency: usize) -> Self {
        self.body_limit = body_limit;
        self.max_concurrency = max_concurrency;
        self
    }

    fn validate(&self) -> Result<(), ProxyError> {
        if self.local_secret.is_empty() {
            return Err(ProxyError::EmptyLocalSecret);
        }
        if self.body_limit == 0 || self.max_concurrency == 0 {
            return Err(ProxyError::InvalidLimit);
        }
        if !self.allow_test_upstream && self.upstream_url.as_str() != CODEX_RESPONSES_URL {
            return Err(ProxyError::UpstreamNotAllowed);
        }
        if self.allow_test_upstream {
            let loopback = self
                .upstream_url
                .host_str()
                .and_then(|host| host.parse::<std::net::IpAddr>().ok())
                .is_some_and(|address| address.is_loopback());
            if !loopback {
                return Err(ProxyError::UpstreamNotAllowed);
            }
        }
        Ok(())
    }
}

#[derive(Clone)]
struct AppState {
    config: ProxyConfig,
    credentials: Arc<dyn CredentialProvider>,
    client: reqwest::Client,
    concurrency: Arc<Semaphore>,
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
    };
    Ok(Router::new()
        .route("/health", get(health))
        .route("/v1/responses", post(proxy_responses))
        .layer(DefaultBodyLimit::max(body_limit))
        .with_state(state))
}

async fn health() -> Json<Value> {
    Json(json!({"status": "ok"}))
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
    let transformed = match transform_responses_request(parsed, &TransformOptions::default()) {
        Ok(transformed) => transformed,
        Err(error) => return error_response(StatusCode::BAD_REQUEST, &error.to_string()),
    };
    let diagnostics = RequestDiagnostics::new(request_started_at, &headers, &transformed);
    diagnostics.log_received();
    let encoded = match serde_json::to_vec(&transformed) {
        Ok(encoded) => encoded,
        Err(_) => {
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to encode transformed request",
            );
        }
    };
    let credential = match state.credentials.access_credential().await {
        Ok(credential) => credential,
        Err(message) => return error_response(StatusCode::UNAUTHORIZED, &message),
    };

    let first = match send_upstream(
        &state,
        &headers,
        &encoded,
        &credential,
        &diagnostics.client_request_id,
    )
    .await
    {
        Ok(response) => response,
        Err(message) => {
            diagnostics.log_request_failed(1);
            return error_response(StatusCode::BAD_GATEWAY, &message);
        }
    };
    diagnostics.log_response_headers(&first, 1);
    let upstream = if first.status() == reqwest::StatusCode::UNAUTHORIZED {
        drop(first);
        let refreshed = match state.credentials.force_refresh().await {
            Ok(credential) => credential,
            Err(message) => return error_response(StatusCode::UNAUTHORIZED, &message),
        };
        match send_upstream(
            &state,
            &headers,
            &encoded,
            &refreshed,
            &diagnostics.client_request_id,
        )
        .await
        {
            Ok(response) => {
                diagnostics.log_response_headers(&response, 2);
                response
            }
            Err(message) => {
                diagnostics.log_request_failed(2);
                return error_response(StatusCode::BAD_GATEWAY, &message);
            }
        }
    } else {
        first
    };

    stream_response(upstream, diagnostics).await
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

async fn send_upstream(
    state: &AppState,
    incoming_headers: &HeaderMap,
    body: &[u8],
    credential: &AccessCredential,
    request_id: &str,
) -> Result<reqwest::Response, String> {
    let mut headers = build_upstream_headers(
        incoming_headers,
        &credential.access_token,
        &credential.account_id,
    )
    .map_err(|error| error.to_string())?;
    if !headers.contains_key("x-request-id") {
        let request_id = HeaderValue::from_str(request_id)
            .map_err(|_| "invalid proxy request ID".to_string())?;
        headers.insert("x-request-id", request_id);
    }
    state
        .client
        .post(state.config.upstream_url.clone())
        .headers(headers)
        .body(body.to_vec())
        .send()
        .await
        .map_err(|_| "Codex upstream request failed".to_string())
}

async fn stream_response(upstream: reqwest::Response, diagnostics: RequestDiagnostics) -> Response {
    let status = upstream.status();
    let source_headers = upstream.headers().clone();
    if !status.is_success() {
        let body = upstream.bytes().await.unwrap_or_default();
        tracing::warn!(
            upstream_status = %status,
            upstream_error = %sanitize_error_body(&body),
            "Codex upstream rejected the request"
        );
        diagnostics.log_completed(
            "http_error",
            &header_value(&source_headers, "x-request-id"),
            None,
            body.len(),
            1,
            0,
        );
        let mut response = Response::new(Body::from(body));
        *response.status_mut() = status;
        copy_response_headers(&source_headers, &mut response);
        return response;
    }
    let upstream_request_id = header_value(&source_headers, "x-request-id");
    let stream = bridge_sse_stream(upstream, diagnostics, upstream_request_id);
    let mut response = Response::new(Body::from_stream(stream));
    *response.status_mut() = status;
    copy_response_headers(&source_headers, &mut response);
    response
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
struct RequestDiagnostics {
    started_at: Instant,
    client_request_id: String,
    model: String,
}

impl RequestDiagnostics {
    fn new(started_at: Instant, headers: &HeaderMap, request: &Value) -> Self {
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
        }
    }

    fn log_received(&self) {
        tracing::info!(
            client_request_id = self.client_request_id.as_str(),
            model = self.model.as_str(),
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
        bytes_received: usize,
        chunks_received: u64,
        filtered_keepalive_events: u64,
    ) {
        tracing::info!(
            client_request_id = self.client_request_id.as_str(),
            upstream_request_id,
            model = self.model.as_str(),
            outcome,
            sse_error_type = sse_error_type.unwrap_or("<none>"),
            total_ms = elapsed_ms(self.started_at),
            bytes_received,
            chunks_received,
            filtered_keepalive_events,
            "Codex proxy request completed"
        );
    }
}

struct StreamDiagnostics {
    request: RequestDiagnostics,
    upstream_request_id: String,
    first_bytes_logged: bool,
    sse_error_type: Option<String>,
    bytes_received: usize,
    chunks_received: u64,
    filtered_keepalive_events: u64,
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
            self.bytes_received,
            self.chunks_received,
            self.filtered_keepalive_events,
        );
    }
}

impl Drop for StreamDiagnostics {
    fn drop(&mut self) {
        if !self.finished {
            self.request.log_completed(
                "client_disconnected",
                &self.upstream_request_id,
                self.sse_error_type.as_deref(),
                self.bytes_received,
                self.chunks_received,
                self.filtered_keepalive_events,
            );
        }
    }
}

fn extract_sse_error_type(event: &[u8]) -> Option<String> {
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

fn header_value(headers: &HeaderMap, name: &'static str) -> String {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("<missing>")
        .to_owned()
}

fn elapsed_ms(started_at: Instant) -> u64 {
    started_at
        .elapsed()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

fn copy_response_headers(source_headers: &HeaderMap, response: &mut Response) {
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

fn sanitize_error_body(body: &[u8]) -> String {
    const MAX_DIAGNOSTIC_BYTES: usize = 4096;
    let truncated = &body[..body.len().min(MAX_DIAGNOSTIC_BYTES)];
    let Ok(mut value) = serde_json::from_slice::<Value>(truncated) else {
        return "non-JSON upstream error body".into();
    };
    redact_sensitive_fields(&mut value);
    value.to_string()
}

fn redact_sensitive_fields(value: &mut Value) {
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

fn error_response(status: StatusCode, message: &str) -> Response {
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
    #[error("proxy body and concurrency limits must be positive")]
    InvalidLimit,
    #[error("upstream URL is not allowlisted")]
    UpstreamNotAllowed,
    #[error(transparent)]
    Url(#[from] url::ParseError),
    #[error("failed to create HTTP client")]
    HttpClient(#[source] reqwest::Error),
}

#[cfg(test)]
mod tests {
    use super::{RequestDiagnostics, extract_sse_error_type, sanitize_error_body};
    use http::HeaderMap;
    use serde_json::json;
    use std::time::Instant;

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
    fn request_diagnostics_generates_a_correlation_id_when_grok_omits_one() {
        let diagnostics =
            RequestDiagnostics::new(Instant::now(), &HeaderMap::new(), &json!({"model":"test"}));

        assert!(diagnostics.client_request_id.starts_with("proxy-"));
        assert_ne!(diagnostics.client_request_id, "<missing>");
    }
}
