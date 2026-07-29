//! Loopback-only Responses API proxy.

use std::{sync::Arc, time::Duration};

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

    let first = match send_upstream(&state, &headers, &encoded, &credential).await {
        Ok(response) => response,
        Err(message) => return error_response(StatusCode::BAD_GATEWAY, &message),
    };
    let upstream = if first.status() == reqwest::StatusCode::UNAUTHORIZED {
        drop(first);
        let refreshed = match state.credentials.force_refresh().await {
            Ok(credential) => credential,
            Err(message) => return error_response(StatusCode::UNAUTHORIZED, &message),
        };
        match send_upstream(&state, &headers, &encoded, &refreshed).await {
            Ok(response) => response,
            Err(message) => return error_response(StatusCode::BAD_GATEWAY, &message),
        }
    } else {
        first
    };

    stream_response(upstream).await
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
) -> Result<reqwest::Response, String> {
    let headers = build_upstream_headers(
        incoming_headers,
        &credential.access_token,
        &credential.account_id,
    )
    .map_err(|error| error.to_string())?;
    state
        .client
        .post(state.config.upstream_url.clone())
        .headers(headers)
        .body(body.to_vec())
        .send()
        .await
        .map_err(|_| "Codex upstream request failed".to_string())
}

async fn stream_response(upstream: reqwest::Response) -> Response {
    let status = upstream.status();
    let source_headers = upstream.headers().clone();
    if !status.is_success() {
        let body = upstream.bytes().await.unwrap_or_default();
        tracing::warn!(
            upstream_status = %status,
            upstream_error = %sanitize_error_body(&body),
            "Codex upstream rejected the request"
        );
        let mut response = Response::new(Body::from(body));
        *response.status_mut() = status;
        copy_response_headers(&source_headers, &mut response);
        return response;
    }
    let stream = bridge_sse_stream(upstream);
    let mut response = Response::new(Body::from_stream(stream));
    *response.status_mut() = status;
    copy_response_headers(&source_headers, &mut response);
    response
}

fn bridge_sse_stream(
    upstream: reqwest::Response,
) -> impl futures_util::Stream<Item = Result<Bytes, std::io::Error>> {
    let source = upstream.bytes_stream();
    async_stream::try_stream! {
        let mut source = std::pin::pin!(source);
        let mut transformer = CodexSseTransformer::default();
        while let Some(chunk) = source.next().await {
            let chunk = chunk.map_err(std::io::Error::other)?;
            for event in transformer.push(&chunk) {
                yield Bytes::from(event);
            }
        }
        for remainder in transformer.finish() {
            yield Bytes::from(remainder);
        }
    }
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
    use super::sanitize_error_body;

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
}
