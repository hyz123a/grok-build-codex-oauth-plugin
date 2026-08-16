//! Codex (ChatGPT/Codex OAuth) upstream backend.
//!
//! Owns the Codex-specific request transform, the explicit `prompt_cache_key`
//! session key, the reusable WebSocket incremental transport, OAuth credential
//! refresh/retry, and the fallback HTTP/SSE path. The shared shell in
//! `crate` (auth, concurrency, diagnostics, SSE framing) is reused.

use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use axum::{
    body::{Body, Bytes},
    http::{HeaderMap, HeaderValue, StatusCode, header::CONTENT_TYPE},
    response::Response,
};
use base64::{Engine, engine::general_purpose::STANDARD};
use codex_oauth::AccessCredential;
use codex_protocol::{
    CodexSseTransformer, append_additional_instructions, apply_codex_response_guidelines,
    build_upstream_headers, transform_responses_request,
};
use futures_util::{SinkExt, StreamExt};
use percent_encoding::percent_decode_str;
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::{Mutex, OwnedMutexGuard, mpsc, oneshot},
    task::JoinHandle,
};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, client_async_tls_with_config,
    tungstenite::{Error as WebSocketError, Message, client::IntoClientRequest},
};
use url::Url;

use crate::{
    AppState, CompletionMetrics, RequestDiagnostics, error_response, extract_cache_usage,
    extract_sse_error_type, stream_response,
};

pub(crate) const MAX_CACHED_WEBSOCKET_SESSIONS: usize = 64;
const MAX_PROXY_RESPONSE_HEADERS: usize = 16 * 1024;
type RawCodexWebSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;

pub(crate) struct CodexWebSocketPump {
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

#[derive(Debug)]
enum WebSocketSendError {
    Timeout(Duration),
    Transport(WebSocketError),
}

impl std::fmt::Display for WebSocketSendError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Timeout(duration) => write!(formatter, "timed out after {duration:?}"),
            Self::Transport(error) => write!(formatter, "transport failed: {error}"),
        }
    }
}

impl std::error::Error for WebSocketSendError {}

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

#[derive(Default)]
pub(crate) struct WebSocketSessionPool {
    pub(crate) sessions: Mutex<HashMap<String, Arc<Mutex<WebSocketSession>>>>,
}

#[derive(Default)]
pub(crate) struct WebSocketSession {
    pub(crate) connection: Option<CodexWebSocketPump>,
    pub(crate) last_request: Option<Value>,
    pub(crate) last_response: Option<CachedWebSocketResponse>,
    pub(crate) turn_state: Option<String>,
}

#[derive(Clone)]
pub(crate) struct CachedWebSocketResponse {
    pub(crate) response_id: String,
    pub(crate) output: Vec<Value>,
}

impl WebSocketSessionPool {
    pub(crate) async fn get(&self, key: String) -> Arc<Mutex<WebSocketSession>> {
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

/// Handle a Codex-backend request after the shared shell has validated the
/// local credential, concurrency, and model allowlist.
pub(crate) async fn handle_codex(
    state: &AppState,
    headers: &HeaderMap,
    parsed: Value,
    request_started_at: Instant,
) -> Response {
    let transform_options = state.config.transform_options(&parsed);
    let mut transformed = match transform_responses_request(parsed, &transform_options) {
        Ok(transformed) => transformed,
        Err(error) => return error_response(StatusCode::BAD_REQUEST, &error.to_string()),
    };
    if let Err(error) = apply_codex_response_guidelines(&mut transformed) {
        return error_response(StatusCode::BAD_REQUEST, &error.to_string());
    }
    if let Some(additional) = state.config.additional_instructions.as_deref()
        && let Err(error) = append_additional_instructions(&mut transformed, additional)
    {
        return error_response(StatusCode::BAD_REQUEST, &error.to_string());
    }
    let ids = resolve_upstream_request_ids(&mut transformed, headers, &state.config.installation_id);
    let diagnostics = RequestDiagnostics::new(request_started_at, headers, &transformed, &ids);
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
            state,
            headers,
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
        state,
        headers,
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
            state,
            headers,
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

    stream_response(upstream, diagnostics, None).await
}

fn resolve_upstream_request_ids(
    request: &mut Value,
    headers: &HeaderMap,
    installation_id: &str,
) -> codex_protocol::UpstreamRequestIds {
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
    codex_protocol::UpstreamRequestIds {
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
    ids: &codex_protocol::UpstreamRequestIds,
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
    ids: &codex_protocol::UpstreamRequestIds,
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
    ids: &codex_protocol::UpstreamRequestIds,
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
    ids: &codex_protocol::UpstreamRequestIds,
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

pub(crate) fn proxy_authorization(proxy: &Url) -> Result<Option<String>, String> {
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
    ids: &codex_protocol::UpstreamRequestIds,
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

fn websocket_session_key(account_id: &str, ids: &codex_protocol::UpstreamRequestIds) -> String {
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

pub(crate) struct PreparedWebSocketRequest {
    pub(crate) payload: Value,
    pub(crate) incremental: bool,
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

pub(crate) fn prepare_websocket_request(
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
        let mut stream_diagnostics = WebSocketStreamDiagnostics::new(diagnostics, mode);
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

pub(crate) fn websocket_terminal_response_has_empty_output(payload: &str) -> bool {
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
                ttfb_ms = crate::elapsed_ms(self.request.started_at),
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
                elapsed_ms = crate::elapsed_ms(self.request.started_at),
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
