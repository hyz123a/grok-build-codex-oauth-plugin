//! Loopback-only Responses API proxy.

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
use base64::{Engine, engine::general_purpose::STANDARD};
use codex_oauth::{AccessCredential, CredentialStore, Credentials, OAuthClient};
use codex_protocol::{
    CodexSseTransformer, GROK_CODEX_SYSTEM_PROMPT_TEMPLATE, TransformOptions, UpstreamRequestIds,
    apply_codex_response_guidelines, build_upstream_headers, transform_responses_request,
};
use futures_util::{SinkExt, StreamExt};
use percent_encoding::percent_decode_str;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::{Mutex, OwnedMutexGuard, Semaphore, mpsc, oneshot},
    task::JoinHandle,
};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, client_async_tls_with_config,
    tungstenite::{Error as WebSocketError, Message, client::IntoClientRequest},
};
use url::Url;

pub const CODEX_RESPONSES_URL: &str = "https://chatgpt.com/backend-api/codex/responses";
const DEFAULT_BODY_LIMIT: usize = 16 * 1024 * 1024;
const DEFAULT_MAX_CONCURRENCY: usize = 4;
const MAX_CACHED_WEBSOCKET_SESSIONS: usize = 64;
const DEFAULT_WEBSOCKET_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_WEBSOCKET_SEND_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_HTTP_RESPONSE_HEADERS_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_PROXY_RESPONSE_HEADERS: usize = 16 * 1024;
static REQUEST_SEQUENCE: AtomicU64 = AtomicU64::new(1);
type RawCodexWebSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;

pub fn system_prompt_policy_fingerprint() -> String {
    let digest = Sha256::digest(GROK_CODEX_SYSTEM_PROMPT_TEMPLATE.as_bytes());
    digest[..8]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

pub fn websocket_proxy_configured_from_env() -> Result<bool, ProxyError> {
    let upstream = Url::parse(CODEX_RESPONSES_URL)?;
    Ok(proxy_url_from_environment(&websocket_url(&upstream))?.is_some())
}

struct CodexWebSocketPump {
    tx_command: mpsc::Sender<WebSocketCommand>,
    rx_message: mpsc::UnboundedReceiver<Result<Message, WebSocketError>>,
    pump_task: JoinHandle<()>,
}

enum WebSocketCommand {
    Send {
        message: Message,
        tx_result: oneshot::Sender<Result<(), WebSocketError>>,
    },
}

impl CodexWebSocketPump {
    fn new(websocket: RawCodexWebSocket) -> Self {
        let (tx_command, mut rx_command) = mpsc::channel::<WebSocketCommand>(32);
        let (tx_message, rx_message) = mpsc::unbounded_channel::<Result<Message, WebSocketError>>();
        let pump_task = tokio::spawn(async move {
            let mut websocket = websocket;
            loop {
                tokio::select! {
                    command = rx_command.recv() => {
                        let Some(command) = command else {
                            break;
                        };
                        match command {
                            WebSocketCommand::Send { message, tx_result } => {
                                let result = websocket.send(message).await;
                                let failed = result.is_err();
                                let _ = tx_result.send(result);
                                if failed {
                                    break;
                                }
                            }
                        }
                    }
                    message = websocket.next() => {
                        let Some(message) = message else {
                            break;
                        };
                        match message {
                            Ok(Message::Ping(payload)) => {
                                if let Err(error) = websocket.send(Message::Pong(payload)).await {
                                    let _ = tx_message.send(Err(error));
                                    break;
                                }
                            }
                            Ok(Message::Pong(_)) => {}
                            Ok(message @ (
                                Message::Text(_)
                                | Message::Binary(_)
                                | Message::Close(_)
                                | Message::Frame(_)
                            )) => {
                                let is_close = matches!(message, Message::Close(_));
                                if tx_message.send(Ok(message)).is_err() || is_close {
                                    break;
                                }
                            }
                            Err(error) => {
                                let _ = tx_message.send(Err(error));
                                break;
                            }
                        }
                    }
                }
            }
        });
        Self {
            tx_command,
            rx_message,
            pump_task,
        }
    }

    async fn send(&self, message: Message) -> Result<(), WebSocketError> {
        let (tx_result, rx_result) = oneshot::channel();
        if self
            .tx_command
            .send(WebSocketCommand::Send { message, tx_result })
            .await
            .is_err()
        {
            return Err(WebSocketError::ConnectionClosed);
        }
        rx_result
            .await
            .unwrap_or(Err(WebSocketError::ConnectionClosed))
    }

    async fn next(&mut self) -> Option<Result<Message, WebSocketError>> {
        self.rx_message.recv().await
    }

    fn is_closed(&self) -> bool {
        self.pump_task.is_finished() || self.tx_command.is_closed()
    }
}

impl Drop for CodexWebSocketPump {
    fn drop(&mut self) {
        self.pump_task.abort();
    }
}

#[derive(Debug, Error)]
enum WebSocketSendError {
    #[error("timed out after {0:?}")]
    Timeout(Duration),
    #[error("transport failed: {0}")]
    Transport(#[source] WebSocketError),
}

async fn send_websocket_message(
    connection: &CodexWebSocketPump,
    message: Message,
    timeout: Duration,
) -> Result<(), WebSocketSendError> {
    tokio::time::timeout(timeout, connection.send(message))
        .await
        .map_err(|_| WebSocketSendError::Timeout(timeout))?
        .map_err(WebSocketSendError::Transport)
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
}

impl ProxyConfig {
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
        }
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
struct AppState {
    config: ProxyConfig,
    credentials: Arc<dyn CredentialProvider>,
    client: reqwest::Client,
    concurrency: Arc<Semaphore>,
    websocket_sessions: Arc<WebSocketSessionPool>,
}

#[derive(Default)]
struct WebSocketSessionPool {
    sessions: Mutex<HashMap<String, Arc<Mutex<WebSocketSession>>>>,
}

#[derive(Default)]
struct WebSocketSession {
    connection: Option<CodexWebSocketPump>,
    last_request: Option<Value>,
    last_response: Option<CachedWebSocketResponse>,
    turn_state: Option<String>,
}

#[derive(Clone)]
struct CachedWebSocketResponse {
    response_id: String,
    output: Vec<Value>,
}

impl WebSocketSessionPool {
    async fn get(&self, key: String) -> Arc<Mutex<WebSocketSession>> {
        let mut sessions = self.sessions.lock().await;
        if let Some(session) = sessions.get(&key) {
            return Arc::clone(session);
        }
        if sessions.len() >= MAX_CACHED_WEBSOCKET_SESSIONS
            && let Some(eviction_key) = sessions
                .iter()
                .find(|(_, session)| Arc::strong_count(session) == 1)
                .map(|(key, _)| key.clone())
        {
            sessions.remove(&eviction_key);
        }
        let session = Arc::new(Mutex::new(WebSocketSession::default()));
        sessions.insert(key, Arc::clone(&session));
        session
    }
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
        websocket_sessions: Arc::new(WebSocketSessionPool::default()),
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
        "websocket_proxy": state.config.websocket_proxy.is_some(),
        "system_prompt_policy_fingerprint": system_prompt_policy_fingerprint()
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
    if let Some(allowed_models) = &state.config.allowed_models {
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
    let transform_options = state.config.transform_options(&parsed);
    let mut transformed = match transform_responses_request(parsed, &transform_options) {
        Ok(transformed) => transformed,
        Err(error) => return error_response(StatusCode::BAD_REQUEST, &error.to_string()),
    };
    if let Err(error) = apply_codex_response_guidelines(&mut transformed) {
        return error_response(StatusCode::BAD_REQUEST, &error.to_string());
    }
    let ids =
        resolve_upstream_request_ids(&mut transformed, &headers, &state.config.installation_id);
    let diagnostics = RequestDiagnostics::new(request_started_at, &headers, &transformed, &ids);
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

    if state.config.websocket_url.is_some() {
        match send_websocket_upstream(
            &state,
            &headers,
            &transformed,
            &credential,
            &diagnostics.client_request_id,
            &ids,
            diagnostics.clone(),
        )
        .await
        {
            Ok(response) => return response,
            Err(reason) => {
                tracing::warn!(
                    client_request_id = diagnostics.client_request_id.as_str(),
                    model = diagnostics.model.as_str(),
                    reason,
                    fallback_transport = "http-sse",
                    "Codex upstream WebSocket unavailable; falling back"
                );
            }
        }
    }

    let first = match send_upstream(
        &state,
        &headers,
        &encoded,
        &credential,
        &diagnostics.client_request_id,
        &ids,
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
            &ids,
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

fn resolve_upstream_request_ids(
    request: &mut Value,
    headers: &HeaderMap,
    installation_id: &str,
) -> UpstreamRequestIds {
    let grok_session_id = headers
        .get("x-grok-session-id")
        .and_then(|value| value.to_str().ok())
        .filter(|key| !key.is_empty())
        .unwrap_or(installation_id)
        .to_owned();
    let grok_thread_id = headers
        .get("x-grok-thread-id")
        .and_then(|value| value.to_str().ok())
        .filter(|key| !key.is_empty())
        .unwrap_or(&grok_session_id)
        .to_owned();
    let prompt_cache_key = if let Some(key) = request
        .get("prompt_cache_key")
        .and_then(Value::as_str)
        .filter(|key| !key.is_empty())
    {
        Some(key.to_owned())
    } else {
        request.as_object_mut().map(|object| {
            object.insert(
                "prompt_cache_key".to_owned(),
                Value::String(grok_session_id.clone()),
            );
            grok_session_id.clone()
        })
    };
    UpstreamRequestIds {
        installation_id: installation_id.to_owned(),
        session_id: grok_session_id,
        thread_id: grok_thread_id,
        prompt_cache_key,
    }
}

async fn send_upstream(
    state: &AppState,
    incoming_headers: &HeaderMap,
    body: &[u8],
    credential: &AccessCredential,
    request_id: &str,
    ids: &UpstreamRequestIds,
) -> Result<reqwest::Response, String> {
    let mut headers = build_upstream_headers(
        incoming_headers,
        &credential.access_token,
        &credential.account_id,
        ids,
    )
    .map_err(|error| error.to_string())?;
    if !headers.contains_key("x-request-id") {
        let request_id = HeaderValue::from_str(request_id)
            .map_err(|_| "invalid proxy request ID".to_string())?;
        headers.insert("x-request-id", request_id);
    }
    tokio::time::timeout(
        state.config.http_response_headers_timeout,
        state
            .client
            .post(state.config.upstream_url.clone())
            .headers(headers)
            .body(body.to_vec())
            .send(),
    )
    .await
    .map_err(|_| "Codex upstream response headers timed out".to_string())?
    .map_err(|_| "Codex upstream request failed".to_string())
}

async fn send_websocket_upstream(
    state: &AppState,
    incoming_headers: &HeaderMap,
    request: &Value,
    credential: &AccessCredential,
    request_id: &str,
    ids: &UpstreamRequestIds,
    diagnostics: RequestDiagnostics,
) -> Result<Response, String> {
    let session_key = websocket_session_key(&credential.account_id, ids);
    let session = state.websocket_sessions.get(session_key).await;
    let mut session = session.lock_owned().await;
    if session
        .connection
        .as_ref()
        .is_some_and(CodexWebSocketPump::is_closed)
    {
        reset_websocket_session(&mut session);
    }
    let mut prepared = prepare_websocket_request(request, &session);
    if prepared.starts_new_turn {
        session.turn_state = None;
    }
    let mut connection_reused = session.connection.is_some();
    let mut payload = websocket_request_payload(
        &prepared.payload,
        ids,
        request_id,
        session.turn_state.as_deref(),
    )?;
    let full_retry_payload = websocket_request_payload(
        &PreparedWebSocketRequest::full(request).payload,
        ids,
        request_id,
        session.turn_state.as_deref(),
    )?;

    if session.connection.is_none() {
        let (connection, handshake_turn_state) = connect_websocket_with_refresh(
            state,
            incoming_headers,
            credential,
            request_id,
            ids,
            &diagnostics,
        )
        .await?;
        session.connection = Some(connection);
        if session.turn_state.is_none() {
            session.turn_state = handshake_turn_state;
        }
    }

    let send_result = send_websocket_message(
        session
            .connection
            .as_ref()
            .ok_or_else(|| "websocket connection is unavailable".to_string())?,
        Message::Text(payload.clone().into()),
        state.config.websocket_send_timeout,
    )
    .await;
    if let Err(error) = send_result {
        tracing::warn!(
            client_request_id = diagnostics.client_request_id.as_str(),
            model = diagnostics.model.as_str(),
            error = %error,
            "Codex upstream WebSocket send failed; reconnecting"
        );
        reset_websocket_session(&mut session);
        connection_reused = false;
        prepared = PreparedWebSocketRequest::full(request);
        payload = websocket_request_payload(&prepared.payload, ids, request_id, None)?;
        let (connection, handshake_turn_state) = connect_websocket_with_refresh(
            state,
            incoming_headers,
            credential,
            request_id,
            ids,
            &diagnostics,
        )
        .await?;
        session.connection = Some(connection);
        session.turn_state = handshake_turn_state;
        let retry_result = send_websocket_message(
            session
                .connection
                .as_ref()
                .ok_or_else(|| "websocket connection is unavailable".to_string())?,
            Message::Text(payload.into()),
            state.config.websocket_send_timeout,
        )
        .await;
        if let Err(error) = retry_result {
            reset_websocket_session(&mut session);
            return Err(format!(
                "failed to send Codex websocket request after reconnect: {error}"
            ));
        }
    }

    session.last_request = Some(request.clone());
    session.last_response = None;
    tracing::info!(
        client_request_id = diagnostics.client_request_id.as_str(),
        model = diagnostics.model.as_str(),
        transport = "websocket",
        connection_reused,
        incremental_request = prepared.incremental,
        previous_response_id_present = prepared.incremental,
        "Codex upstream WebSocket request sent"
    );

    let stream = bridge_websocket_stream(
        WebSocketLease::new(session),
        diagnostics,
        WebSocketRequestMode {
            connection_reused,
            incremental: prepared.incremental,
        },
        full_retry_payload,
    );
    let mut response = Response::new(Body::from_stream(stream));
    *response.status_mut() = StatusCode::OK;
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("text/event-stream"));
    response.headers_mut().insert(
        "x-codex-upstream-transport",
        HeaderValue::from_static("websocket"),
    );
    Ok(response)
}

async fn connect_websocket_with_refresh(
    state: &AppState,
    incoming_headers: &HeaderMap,
    credential: &AccessCredential,
    request_id: &str,
    ids: &UpstreamRequestIds,
    diagnostics: &RequestDiagnostics,
) -> Result<(CodexWebSocketPump, Option<String>), String> {
    match connect_websocket_upstream(
        state,
        incoming_headers,
        credential,
        request_id,
        ids,
        diagnostics,
    )
    .await
    {
        Ok(connection) => Ok(connection),
        Err(WebSocketConnectError::Unauthorized) => {
            let refreshed = state.credentials.force_refresh().await?;
            connect_websocket_upstream(
                state,
                incoming_headers,
                &refreshed,
                request_id,
                ids,
                diagnostics,
            )
            .await
            .map_err(WebSocketConnectError::into_message)
        }
        Err(error) => Err(error.into_message()),
    }
}

fn reset_websocket_session(session: &mut WebSocketSession) {
    session.connection = None;
    session.last_request = None;
    session.last_response = None;
    session.turn_state = None;
}

async fn connect_websocket_upstream(
    state: &AppState,
    incoming_headers: &HeaderMap,
    credential: &AccessCredential,
    request_id: &str,
    ids: &UpstreamRequestIds,
    diagnostics: &RequestDiagnostics,
) -> Result<(CodexWebSocketPump, Option<String>), WebSocketConnectError> {
    let websocket_url = state.config.websocket_url.as_ref().ok_or_else(|| {
        WebSocketConnectError::Other("websocket upstream is disabled".to_string())
    })?;
    let mut headers = build_upstream_headers(
        incoming_headers,
        &credential.access_token,
        &credential.account_id,
        ids,
    )
    .map_err(|error| WebSocketConnectError::Other(error.to_string()))?;
    headers.remove("accept");
    headers.remove("content-type");
    headers.insert(
        "openai-beta",
        HeaderValue::from_static("responses_websockets=2026-02-06"),
    );
    let thread_id = HeaderValue::from_str(&ids.thread_id)
        .map_err(|_| WebSocketConnectError::Other("invalid websocket thread ID".to_string()))?;
    headers.insert("x-client-request-id", thread_id);
    let window_id = HeaderValue::from_str(&ids.session_id)
        .map_err(|_| WebSocketConnectError::Other("invalid websocket session ID".to_string()))?;
    headers.insert("x-codex-window-id", window_id);
    if !headers.contains_key("x-request-id") {
        let request_id = HeaderValue::from_str(request_id)
            .map_err(|_| WebSocketConnectError::Other("invalid proxy request ID".to_string()))?;
        headers.insert("x-request-id", request_id);
    }

    let mut websocket_request = websocket_url.as_str().into_client_request().map_err(|_| {
        WebSocketConnectError::Other("failed to build Codex websocket request".to_string())
    })?;
    websocket_request.headers_mut().extend(headers);
    let connect = async {
        let socket = connect_websocket_socket(websocket_url, state.config.websocket_proxy.as_ref())
            .await
            .map_err(WebSocketConnectError::Other)?;
        client_async_tls_with_config(websocket_request, socket, None, None)
            .await
            .map_err(|error| {
                if matches!(
                    &error,
                    tokio_tungstenite::tungstenite::Error::Http(response)
                        if response.status() == StatusCode::UNAUTHORIZED
                ) {
                    WebSocketConnectError::Unauthorized
                } else {
                    WebSocketConnectError::Other(format!(
                        "websocket handshake failed: {}",
                        websocket_error_kind(&error)
                    ))
                }
            })
    };
    let (websocket, response) =
        tokio::time::timeout(state.config.websocket_connect_timeout, connect)
            .await
            .map_err(|_| {
                WebSocketConnectError::Other("websocket connection timed out".to_string())
            })??;
    tracing::info!(
        client_request_id = diagnostics.client_request_id.as_str(),
        model = diagnostics.model.as_str(),
        upstream_status = response.status().as_u16(),
        transport = "websocket",
        proxy_used = state.config.websocket_proxy.is_some(),
        "Codex upstream WebSocket connected"
    );
    let turn_state = response
        .headers()
        .get("x-codex-turn-state")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    Ok((CodexWebSocketPump::new(websocket), turn_state))
}

async fn connect_websocket_socket(
    websocket_url: &Url,
    proxy: Option<&Url>,
) -> Result<TcpStream, String> {
    let host = websocket_url
        .host_str()
        .ok_or_else(|| "websocket upstream has no host".to_string())?;
    let port = websocket_url
        .port_or_known_default()
        .ok_or_else(|| "websocket upstream has no port".to_string())?;
    let authority = format_authority(host, port);
    let Some(proxy) = proxy else {
        return TcpStream::connect(&authority)
            .await
            .map_err(|_| "failed to connect to websocket upstream".to_string());
    };

    let proxy_host = proxy
        .host_str()
        .ok_or_else(|| "websocket proxy has no host".to_string())?;
    let proxy_port = proxy.port_or_known_default().unwrap_or(80);
    let mut socket = TcpStream::connect(format_authority(proxy_host, proxy_port))
        .await
        .map_err(|_| "failed to connect to websocket proxy".to_string())?;
    let authorization = proxy_authorization(proxy)?;
    let mut request = format!(
        "CONNECT {authority} HTTP/1.1\r\nHost: {authority}\r\nProxy-Connection: Keep-Alive\r\n"
    );
    if let Some(authorization) = authorization {
        request.push_str("Proxy-Authorization: Basic ");
        request.push_str(&authorization);
        request.push_str("\r\n");
    }
    request.push_str("\r\n");
    socket
        .write_all(request.as_bytes())
        .await
        .map_err(|_| "failed to write websocket proxy CONNECT request".to_string())?;

    let mut response = Vec::with_capacity(512);
    let mut buffer = [0_u8; 512];
    loop {
        let read = socket
            .read(&mut buffer)
            .await
            .map_err(|_| "failed to read websocket proxy CONNECT response".to_string())?;
        if read == 0 {
            return Err("websocket proxy closed during CONNECT".to_string());
        }
        response.extend_from_slice(&buffer[..read]);
        if response.windows(4).any(|window| window == b"\r\n\r\n") {
            break;
        }
        if response.len() > MAX_PROXY_RESPONSE_HEADERS {
            return Err("websocket proxy CONNECT response headers were too large".to_string());
        }
    }
    let status_line = response
        .split(|byte| *byte == b'\n')
        .next()
        .and_then(|line| std::str::from_utf8(line).ok())
        .unwrap_or_default()
        .trim();
    let status = status_line.split_whitespace().nth(1);
    if status != Some("200") {
        return Err(format!(
            "websocket proxy CONNECT failed with status {}",
            status.unwrap_or("unknown")
        ));
    }
    Ok(socket)
}

fn format_authority(host: &str, port: u16) -> String {
    if host.contains(':') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

fn proxy_authorization(proxy: &Url) -> Result<Option<String>, String> {
    if proxy.username().is_empty() && proxy.password().is_none() {
        return Ok(None);
    }
    let username = percent_decode_str(proxy.username())
        .decode_utf8()
        .map_err(|_| "websocket proxy username is not valid UTF-8".to_string())?;
    let password = percent_decode_str(proxy.password().unwrap_or_default())
        .decode_utf8()
        .map_err(|_| "websocket proxy password is not valid UTF-8".to_string())?;
    Ok(Some(
        STANDARD.encode(format!("{username}:{password}").as_bytes()),
    ))
}

enum WebSocketConnectError {
    Unauthorized,
    Other(String),
}

impl WebSocketConnectError {
    fn into_message(self) -> String {
        match self {
            Self::Unauthorized => "websocket handshake unauthorized".to_string(),
            Self::Other(message) => message,
        }
    }
}

fn websocket_request_payload(
    request: &Value,
    ids: &UpstreamRequestIds,
    request_id: &str,
    turn_state: Option<&str>,
) -> Result<String, String> {
    let mut request = request.clone();
    let object = request
        .as_object_mut()
        .ok_or_else(|| "Codex websocket request must be a JSON object".to_string())?;
    object.insert("type".into(), Value::String("response.create".into()));
    let client_metadata = object
        .entry("client_metadata")
        .or_insert_with(|| Value::Object(serde_json::Map::new()));
    if !client_metadata.is_object() {
        *client_metadata = Value::Object(serde_json::Map::new());
    }
    let metadata = client_metadata
        .as_object_mut()
        .ok_or_else(|| "failed to build websocket client metadata".to_string())?;
    metadata.insert("session_id".into(), Value::String(ids.session_id.clone()));
    metadata.insert("thread_id".into(), Value::String(ids.thread_id.clone()));
    metadata.insert(
        "x-codex-installation-id".into(),
        Value::String(ids.installation_id.clone()),
    );
    metadata.insert(
        "x-codex-window-id".into(),
        Value::String(ids.session_id.clone()),
    );
    metadata.insert("turn_id".into(), Value::String(request_id.to_owned()));
    metadata.insert(
        "x-codex-ws-stream-request-start-ms".into(),
        Value::String(unix_timestamp_ms().to_string()),
    );
    metadata.remove("x-codex-turn-state");
    if let Some(turn_state) = turn_state {
        metadata.insert(
            "x-codex-turn-state".into(),
            Value::String(turn_state.to_owned()),
        );
    }
    serde_json::to_string(&request)
        .map_err(|_| "failed to encode Codex websocket request".to_string())
}

fn unix_timestamp_ms() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

fn websocket_session_key(account_id: &str, ids: &UpstreamRequestIds) -> String {
    let mut digest = Sha256::new();
    digest.update(account_id.as_bytes());
    digest.update([0]);
    digest.update(ids.session_id.as_bytes());
    digest.update([0]);
    digest.update(ids.thread_id.as_bytes());
    digest
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

struct PreparedWebSocketRequest {
    payload: Value,
    incremental: bool,
    starts_new_turn: bool,
}

impl PreparedWebSocketRequest {
    fn full(request: &Value) -> Self {
        let mut payload = request.clone();
        if let Some(object) = payload.as_object_mut() {
            object.remove("previous_response_id");
        }
        Self {
            payload,
            incremental: false,
            starts_new_turn: request_input(request)
                .is_some_and(|input| input.iter().any(is_user_message)),
        }
    }
}

fn prepare_websocket_request(
    request: &Value,
    session: &WebSocketSession,
) -> PreparedWebSocketRequest {
    let Some(previous_request) = session.last_request.as_ref() else {
        return PreparedWebSocketRequest::full(request);
    };
    let Some(previous_response) = session.last_response.as_ref() else {
        return PreparedWebSocketRequest::full(request);
    };
    if !websocket_request_properties_match(previous_request, request) {
        return PreparedWebSocketRequest::full(request);
    }
    let Some(previous_input) = request_input(previous_request) else {
        return PreparedWebSocketRequest::full(request);
    };
    let Some(current_input) = request_input(request) else {
        return PreparedWebSocketRequest::full(request);
    };
    let baseline = previous_input
        .iter()
        .chain(previous_response.output.iter())
        .collect::<Vec<_>>();
    if current_input.len() < baseline.len()
        || !baseline
            .iter()
            .zip(current_input)
            .all(|(previous, current)| *previous == current)
    {
        return PreparedWebSocketRequest::full(request);
    }
    let delta = current_input[baseline.len()..].to_vec();
    let mut payload = request.clone();
    let Some(object) = payload.as_object_mut() else {
        return PreparedWebSocketRequest::full(request);
    };
    object.insert("input".into(), Value::Array(delta.clone()));
    object.insert(
        "previous_response_id".into(),
        Value::String(previous_response.response_id.clone()),
    );
    PreparedWebSocketRequest {
        payload,
        incremental: true,
        starts_new_turn: delta.iter().any(is_user_message),
    }
}

fn websocket_request_properties_match(previous: &Value, current: &Value) -> bool {
    fn properties(request: &Value) -> Option<serde_json::Map<String, Value>> {
        let mut properties = request.as_object()?.clone();
        for field in [
            "input",
            "client_metadata",
            "previous_response_id",
            "stream_options",
            "type",
        ] {
            properties.remove(field);
        }
        Some(properties)
    }
    properties(previous) == properties(current)
}

fn request_input(request: &Value) -> Option<&[Value]> {
    request.get("input")?.as_array().map(Vec::as_slice)
}

fn is_user_message(item: &Value) -> bool {
    item.get("type").and_then(Value::as_str) == Some("message")
        && item.get("role").and_then(Value::as_str) == Some("user")
}

fn websocket_error_kind(error: &tokio_tungstenite::tungstenite::Error) -> &'static str {
    use tokio_tungstenite::tungstenite::Error;
    match error {
        Error::Http(_) => "http_upgrade_rejected",
        Error::Io(_) => "network_error",
        Error::Tls(_) => "tls_error",
        Error::ConnectionClosed | Error::AlreadyClosed => "connection_closed",
        Error::Protocol(_) => "protocol_error",
        _ => "websocket_error",
    }
}

struct WebSocketLease {
    session: OwnedMutexGuard<WebSocketSession>,
    finished: bool,
}

impl WebSocketLease {
    fn new(session: OwnedMutexGuard<WebSocketSession>) -> Self {
        Self {
            session,
            finished: false,
        }
    }

    fn mark_finished(&mut self) {
        self.finished = true;
    }
}

impl Drop for WebSocketLease {
    fn drop(&mut self) {
        if !self.finished {
            self.session.connection = None;
            self.session.last_request = None;
            self.session.last_response = None;
            self.session.turn_state = None;
        }
    }
}

#[derive(Clone, Copy)]
struct WebSocketRequestMode {
    connection_reused: bool,
    incremental: bool,
}

fn bridge_websocket_stream(
    mut lease: WebSocketLease,
    diagnostics: RequestDiagnostics,
    mode: WebSocketRequestMode,
    full_retry_payload: String,
) -> impl futures_util::Stream<Item = Result<Bytes, std::io::Error>> {
    async_stream::stream! {
        let mut transformer = CodexSseTransformer::default();
        let mut stream_diagnostics =
            WebSocketStreamDiagnostics::new(diagnostics, mode);
        let mut incremental_retry_used = false;
        let outcome;
        loop {
            let message = match lease.session.connection.as_mut() {
                Some(websocket) => websocket.next().await,
                None => None,
            };
            let Some(message) = message else {
                stream_diagnostics.finish("transport_error");
                yield Err(std::io::Error::other(
                    "Codex websocket closed before a terminal response",
                ));
                return;
            };
            let message = match message {
                Ok(message) => message,
                Err(_) => {
                    tracing::warn!(
                        client_request_id = stream_diagnostics.request.client_request_id.as_str(),
                        model = stream_diagnostics.request.model.as_str(),
                        "Codex upstream WebSocket transport failed"
                    );
                    stream_diagnostics.finish("transport_error");
                    yield Err(std::io::Error::other("Codex websocket transport failed"));
                    return;
                }
            };
            match message {
                Message::Text(text) => {
                    let text = text.as_str();
                    if let Some(turn_state) = websocket_event_turn_state(text) {
                        lease.session.turn_state = Some(turn_state);
                    }
                    if is_internal_websocket_event(text) {
                        continue;
                    }
                    if mode.incremental
                        && !incremental_retry_used
                        && !stream_diagnostics.first_event_logged
                        && websocket_previous_response_not_found(text)
                    {
                        incremental_retry_used = true;
                        lease.session.last_response = None;
                        let send_result = match lease.session.connection.as_mut() {
                            Some(websocket) => websocket
                                .send(Message::Text(full_retry_payload.clone().into()))
                                .await,
                            None => return,
                        };
                        if send_result.is_err() {
                            stream_diagnostics.finish("transport_error");
                            yield Err(std::io::Error::other(
                                "failed to retry full websocket request",
                            ));
                            return;
                        }
                        tracing::warn!(
                            client_request_id =
                                stream_diagnostics.request.client_request_id.as_str(),
                            model = stream_diagnostics.request.model.as_str(),
                            retry_reason = "previous_response_not_found",
                            "Codex WebSocket incremental request retried with full input"
                        );
                        continue;
                    }
                    if mode.incremental
                        && !incremental_retry_used
                        && !stream_diagnostics.first_event_logged
                        && websocket_terminal_response_has_empty_output(text)
                    {
                        incremental_retry_used = true;
                        lease.session.last_response = None;
                        let send_result = match lease.session.connection.as_mut() {
                            Some(websocket) => websocket
                                .send(Message::Text(full_retry_payload.clone().into()))
                                .await,
                            None => return,
                        };
                        if send_result.is_err() {
                            stream_diagnostics.finish("transport_error");
                            yield Err(std::io::Error::other(
                                "failed to retry full websocket request",
                            ));
                            return;
                        }
                        tracing::warn!(
                            client_request_id =
                                stream_diagnostics.request.client_request_id.as_str(),
                            model = stream_diagnostics.request.model.as_str(),
                            retry_reason = "empty_terminal_response",
                            "Codex WebSocket incremental request retried with full input"
                        );
                        continue;
                    }
                    let framed = format!("data: {text}\n\n");
                    stream_diagnostics.observe_event(framed.as_bytes());
                    let terminal = websocket_event_is_terminal(text);
                    if let Some(response) = completed_websocket_response(text) {
                        lease.session.last_response = Some(response);
                    }
                    for event in transformer.push(framed.as_bytes()) {
                        yield Ok(Bytes::from(event));
                    }
                    stream_diagnostics.observe_filtered_keepalives(
                        transformer.take_filtered_keepalive_events(),
                    );
                    if terminal {
                        outcome = if stream_diagnostics.sse_error_type.is_some() {
                            "sse_error"
                        } else {
                            "completed"
                        };
                        if outcome == "sse_error" {
                            lease.session.connection = None;
                            lease.session.last_request = None;
                            lease.session.last_response = None;
                            lease.session.turn_state = None;
                        }
                        break;
                    }
                }
                Message::Ping(payload) => {
                    let send_result = match lease.session.connection.as_mut() {
                        Some(websocket) => websocket.send(Message::Pong(payload)).await,
                        None => return,
                    };
                    if send_result.is_err() {
                        stream_diagnostics.finish("transport_error");
                        yield Err(std::io::Error::other("failed to answer websocket ping"));
                        return;
                    }
                }
                Message::Close(_) => {
                    stream_diagnostics.finish("transport_error");
                    yield Err(std::io::Error::other(
                        "Codex websocket closed before a terminal response",
                    ));
                    return;
                }
                Message::Binary(_) | Message::Pong(_) | Message::Frame(_) => {}
            }
        }
        for event in transformer.finish() {
            yield Ok(Bytes::from(event));
        }
        stream_diagnostics.observe_filtered_keepalives(
            transformer.take_filtered_keepalive_events(),
        );
        stream_diagnostics.finish(outcome);
        lease.mark_finished();
    }
}

fn websocket_previous_response_not_found(payload: &str) -> bool {
    let Ok(event) = serde_json::from_str::<Value>(payload) else {
        return false;
    };
    event.get("type").and_then(Value::as_str) == Some("error")
        && event
            .get("error")
            .and_then(|error| error.get("code"))
            .and_then(Value::as_str)
            == Some("previous_response_not_found")
}

fn websocket_terminal_response_has_empty_output(payload: &str) -> bool {
    let Ok(event) = serde_json::from_str::<Value>(payload) else {
        return false;
    };
    if !matches!(
        event.get("type").and_then(Value::as_str),
        Some("response.completed" | "response.incomplete")
    ) {
        return false;
    }
    let Some(response) = event.get("response").and_then(Value::as_object) else {
        return false;
    };
    match response.get("output") {
        None => true,
        Some(Value::Array(output)) => output.is_empty(),
        Some(_) => false,
    }
}

fn websocket_event_turn_state(payload: &str) -> Option<String> {
    let event = serde_json::from_str::<Value>(payload).ok()?;
    event
        .get("headers")?
        .get("x-codex-turn-state")?
        .as_str()
        .map(str::to_owned)
}

fn completed_websocket_response(payload: &str) -> Option<CachedWebSocketResponse> {
    let event = serde_json::from_str::<Value>(payload).ok()?;
    if event.get("type").and_then(Value::as_str) != Some("response.completed") {
        return None;
    }
    let response = event.get("response")?;
    Some(CachedWebSocketResponse {
        response_id: response.get("id")?.as_str()?.to_owned(),
        output: response
            .get("output")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default(),
    })
}

fn is_internal_websocket_event(payload: &str) -> bool {
    serde_json::from_str::<Value>(payload)
        .ok()
        .and_then(|event| event.get("type").and_then(Value::as_str).map(str::to_owned))
        .is_some_and(|kind| kind.starts_with("responsesapi.") || kind.starts_with("codex."))
}

fn websocket_event_is_terminal(payload: &str) -> bool {
    serde_json::from_str::<Value>(payload)
        .ok()
        .and_then(|event| event.get("type").and_then(Value::as_str).map(str::to_owned))
        .is_some_and(|kind| {
            matches!(
                kind.as_str(),
                "response.completed" | "response.failed" | "response.incomplete" | "error"
            )
        })
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

fn normalize_upstream_error_status(status: reqwest::StatusCode, body: &[u8]) -> StatusCode {
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
struct RequestDiagnostics {
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
struct CompletionMetrics {
    bytes_received: usize,
    chunks_received: u64,
    filtered_keepalive_events: u64,
    input_tokens: Option<u64>,
    cached_input_tokens: Option<u64>,
}

struct WebSocketStreamDiagnostics {
    request: RequestDiagnostics,
    mode: WebSocketRequestMode,
    first_event_logged: bool,
    sse_error_type: Option<String>,
    metrics: CompletionMetrics,
    finished: bool,
}

impl WebSocketStreamDiagnostics {
    fn new(request: RequestDiagnostics, mode: WebSocketRequestMode) -> Self {
        Self {
            request,
            mode,
            first_event_logged: false,
            sse_error_type: None,
            metrics: CompletionMetrics::default(),
            finished: false,
        }
    }

    fn observe_event(&mut self, event: &[u8]) {
        self.metrics.bytes_received = self.metrics.bytes_received.saturating_add(event.len());
        self.metrics.chunks_received = self.metrics.chunks_received.saturating_add(1);
        if !self.first_event_logged {
            self.first_event_logged = true;
            tracing::info!(
                client_request_id = self.request.client_request_id.as_str(),
                model = self.request.model.as_str(),
                ttfb_ms = elapsed_ms(self.request.started_at),
                first_event_bytes = event.len(),
                transport = "websocket",
                connection_reused = self.mode.connection_reused,
                incremental_request = self.mode.incremental,
                "Codex upstream first WebSocket event received"
            );
        }
        if let Some((input_tokens, cached_input_tokens)) = extract_cache_usage(event) {
            self.metrics.input_tokens = Some(input_tokens);
            self.metrics.cached_input_tokens = Some(cached_input_tokens);
        }
        if self.sse_error_type.is_none()
            && let Some(error_type) = extract_sse_error_type(event)
        {
            tracing::warn!(
                client_request_id = self.request.client_request_id.as_str(),
                model = self.request.model.as_str(),
                sse_error_type = error_type.as_str(),
                elapsed_ms = elapsed_ms(self.request.started_at),
                transport = "websocket",
                "Codex upstream WebSocket error received"
            );
            self.sse_error_type = Some(error_type);
        }
    }

    fn observe_filtered_keepalives(&mut self, count: u64) {
        self.metrics.filtered_keepalive_events =
            self.metrics.filtered_keepalive_events.saturating_add(count);
    }

    fn finish(&mut self, outcome: &str) {
        if self.finished {
            return;
        }
        self.finished = true;
        self.request.log_completed(
            outcome,
            "<missing>",
            self.sse_error_type.as_deref(),
            self.metrics,
        );
    }
}

impl Drop for WebSocketStreamDiagnostics {
    fn drop(&mut self) {
        if !self.finished {
            self.request.log_completed(
                "client_disconnected",
                "<missing>",
                self.sse_error_type.as_deref(),
                self.metrics,
            );
        }
    }
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

fn extract_cache_usage(event: &[u8]) -> Option<(u64, u64)> {
    let text = std::str::from_utf8(event).ok()?;
    let data = text.lines().find_map(|line| {
        line.strip_prefix("data:")
            .map(str::trim_start)
            .filter(|data| !data.is_empty() && *data != "[DONE]")
    })?;
    let value = serde_json::from_str::<Value>(data).ok()?;
    let input_tokens = ["/response/usage/input_tokens", "/usage/input_tokens"]
        .iter()
        .find_map(|pointer| value.pointer(pointer).and_then(Value::as_u64))?;
    let cached_input_tokens = [
        "/response/usage/input_tokens_details/cached_tokens",
        "/usage/input_tokens_details/cached_tokens",
    ]
    .iter()
    .find_map(|pointer| value.pointer(pointer).and_then(Value::as_u64))
    .unwrap_or(0);
    Some((input_tokens, cached_input_tokens))
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
        CachedWebSocketResponse, MAX_CACHED_WEBSOCKET_SESSIONS, RequestDiagnostics,
        WebSocketSession, WebSocketSessionPool, derive_installation_id, extract_sse_error_type,
        normalize_upstream_error_status, prepare_websocket_request, proxy_authorization,
        proxy_url_from_values, sanitize_error_body, websocket_terminal_response_has_empty_output,
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
