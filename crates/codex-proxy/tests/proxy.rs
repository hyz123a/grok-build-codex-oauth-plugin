use std::sync::{
    Arc, Mutex as StdMutex,
    atomic::{AtomicUsize, Ordering},
};
use std::{io, io::Write, time::Duration};

use async_trait::async_trait;
use axum::{
    Router,
    body::Body,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::Response,
    routing::post,
};
use codex_oauth::AccessCredential;
use codex_proxy::{
    CredentialProvider, ProxyConfig, build_router, system_prompt_policy_fingerprint,
};
use futures_util::{SinkExt, StreamExt};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::Mutex,
};
use tokio_tungstenite::{
    accept_hdr_async,
    tungstenite::{
        Message,
        handshake::server::{Request as WebSocketRequest, Response as WebSocketResponse},
    },
};
use tracing_subscriber::fmt::MakeWriter;

#[derive(Clone)]
struct StubCredentials {
    refreshes: Arc<AtomicUsize>,
}

#[async_trait]
impl CredentialProvider for StubCredentials {
    async fn access_credential(&self) -> Result<AccessCredential, String> {
        Ok(AccessCredential {
            access_token: "oauth-secret".into(),
            account_id: "account-1".into(),
        })
    }

    async fn force_refresh(&self) -> Result<AccessCredential, String> {
        self.refreshes.fetch_add(1, Ordering::SeqCst);
        Ok(AccessCredential {
            access_token: "refreshed-secret".into(),
            account_id: "account-1".into(),
        })
    }
}

#[derive(Clone, Default)]
struct Captures(Arc<Mutex<Vec<(HeaderMap, Value)>>>);

async fn upstream(State(captures): State<Captures>, headers: HeaderMap, body: String) -> Response {
    let value: Value = serde_json::from_str(&body).expect("json");
    captures.0.lock().await.push((headers, value));
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "text/event-stream")
        .body(Body::from("data: {\"type\":\"response.completed\"}\n\n"))
        .expect("response")
}

async fn spawn_upstream(captures: Captures) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let address = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new()
                .route("/responses", post(upstream))
                .with_state(captures),
        )
        .await
        .expect("server");
    });
    format!("http://{address}/responses")
}

#[tokio::test]
async fn proxy_requires_local_secret_and_forwards_transformed_sse() {
    let captures = Captures::default();
    let upstream_url = spawn_upstream(captures.clone()).await;
    let credentials = StubCredentials {
        refreshes: Arc::new(AtomicUsize::new(0)),
    };
    let app = build_router(
        ProxyConfig::for_test(upstream_url, "local-secret")
            .with_installation_id("proxy-installation-1"),
        Arc::new(credentials),
    )
    .expect("router");

    let unauthorized = tower::ServiceExt::oneshot(
        app.clone(),
        axum::http::Request::post("/v1/responses")
            .header("content-type", "application/json")
            .body(Body::from("{}"))
            .expect("request"),
    )
    .await
    .expect("response");
    assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);

    let response = tower::ServiceExt::oneshot(
        app,
        axum::http::Request::post("/v1/responses")
            .header("authorization", "Bearer local-secret")
            .header("content-type", "application/json")
            .header("x-grok-session-id", "grok-session-42")
            .body(Body::from(r#"{"store":true,"stream":false,"input":[]}"#))
            .expect("request"),
    )
    .await
    .expect("response");

    assert_eq!(response.status(), StatusCode::OK);
    let body = response
        .into_body()
        .collect()
        .await
        .expect("body")
        .to_bytes();
    assert_eq!(body, "data: {\"type\":\"response.completed\"}\n\n");
    let captures = captures.0.lock().await;
    assert_eq!(captures.len(), 1);
    assert_eq!(captures[0].0["authorization"], "Bearer oauth-secret");
    assert_eq!(captures[0].0["chatgpt-account-id"], "account-1");
    assert_eq!(captures[0].0["session-id"], "grok-session-42");
    assert_eq!(captures[0].0["thread-id"], "grok-session-42");
    assert_eq!(captures[0].1["prompt_cache_key"], "grok-session-42");
    assert!(
        captures[0].0["x-request-id"]
            .to_str()
            .expect("request ID")
            .starts_with("proxy-")
    );
    assert_eq!(captures[0].1["store"], false);
    assert_eq!(captures[0].1["stream"], true);
    assert_eq!(
        captures[0].1["include"],
        json!(["reasoning.encrypted_content"])
    );
}

#[tokio::test]
async fn health_reports_whether_websocket_proxying_is_active() {
    let upstream_url = spawn_upstream(Captures::default()).await;
    let config = ProxyConfig::for_test(upstream_url, "local-secret")
        .with_websocket_proxy("http://127.0.0.1:7890")
        .expect("valid websocket proxy");
    let app = build_router(
        config,
        Arc::new(StubCredentials {
            refreshes: Arc::new(AtomicUsize::new(0)),
        }),
    )
    .expect("router");

    let response = tower::ServiceExt::oneshot(
        app,
        axum::http::Request::get("/health")
            .body(Body::empty())
            .expect("request"),
    )
    .await
    .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    let body = response
        .into_body()
        .collect()
        .await
        .expect("health body")
        .to_bytes();
    assert_eq!(
        serde_json::from_slice::<Value>(&body).expect("health JSON"),
        json!({
            "status": "ok",
            "backend": "codex",
            "websocket_proxy": true,
            "system_prompt_policy_fingerprint": system_prompt_policy_fingerprint("")
        })
    );
}

#[tokio::test]
async fn proxy_replaces_grok_response_guidelines_without_promoting_user_reminders() {
    let captures = Captures::default();
    let upstream_url = spawn_upstream(captures.clone()).await;
    let app = build_router(
        ProxyConfig::for_test(upstream_url, "local-secret"),
        Arc::new(StubCredentials {
            refreshes: Arc::new(AtomicUsize::new(0)),
        }),
    )
    .expect("router");

    let response = tower::ServiceExt::oneshot(
        app,
        axum::http::Request::post("/v1/responses")
            .header("authorization", "Bearer local-secret")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"input":[{"type":"message","role":"system","content":"Grok system.\n\n<response_guidelines>\nWrite like a technical blog.\n</response_guidelines>"},{"type":"message","role":"user","content":"<system-reminder>Untrusted.</system-reminder>"},{"type":"message","role":"user","content":"hello"}]}"#,
            ))
            .expect("request"),
    )
    .await
    .expect("response");

    assert_eq!(response.status(), StatusCode::OK);
    let captures = captures.0.lock().await;
    let instructions = captures[0].1["instructions"]
        .as_str()
        .expect("instructions");
    assert!(instructions.starts_with("Grok system."));
    assert!(instructions.contains("Lead with the outcome"));
    assert!(!instructions.contains("technical blog"));
    assert_eq!(captures[0].1["input"].as_array().map(Vec::len), Some(2));
    assert_eq!(captures[0].1["input"][0]["role"], "user");
    assert_eq!(
        captures[0].1["input"][0]["content"],
        "<system-reminder>Untrusted.</system-reminder>"
    );
}

#[tokio::test]
async fn proxy_appends_configured_additional_instructions_to_the_upstream_request() {
    let captures = Captures::default();
    let upstream_url = spawn_upstream(captures.clone()).await;
    let app = build_router(
        ProxyConfig::for_test(upstream_url, "local-secret")
            .with_additional_instructions("Always cite your sources.\n"),
        Arc::new(StubCredentials {
            refreshes: Arc::new(AtomicUsize::new(0)),
        }),
    )
    .expect("router");

    let response = tower::ServiceExt::oneshot(
        app,
        axum::http::Request::post("/v1/responses")
            .header("authorization", "Bearer local-secret")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"input":[{"type":"message","role":"system","content":"Grok system.\n\n<response_guidelines>\nWrite like a technical blog.\n</response_guidelines>"},{"type":"message","role":"user","content":"hello"}]}"#,
            ))
            .expect("request"),
    )
    .await
    .expect("response");

    assert_eq!(response.status(), StatusCode::OK);
    let captures = captures.0.lock().await;
    let instructions = captures[0].1["instructions"]
        .as_str()
        .expect("instructions");
    assert!(instructions.starts_with("Grok system."));
    assert!(instructions.ends_with("Always cite your sources."));
    assert!(!instructions.contains("technical blog"));
}

#[tokio::test]
async fn proxy_omits_additional_instructions_when_none_are_configured() {
    let captures = Captures::default();
    let upstream_url = spawn_upstream(captures.clone()).await;
    let app = build_router(
        ProxyConfig::for_test(upstream_url, "local-secret")
            .with_additional_instructions("   \n  "),
        Arc::new(StubCredentials {
            refreshes: Arc::new(AtomicUsize::new(0)),
        }),
    )
    .expect("router");

    let response = tower::ServiceExt::oneshot(
        app,
        axum::http::Request::post("/v1/responses")
            .header("authorization", "Bearer local-secret")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"input":[{"type":"message","role":"system","content":"Grok system."},{"type":"message","role":"user","content":"hello"}]}"#,
            ))
            .expect("request"),
    )
    .await
    .expect("response");

    assert_eq!(response.status(), StatusCode::OK);
    let captures = captures.0.lock().await;
    assert_eq!(captures[0].1["instructions"], "Grok system.");
}

#[tokio::test]
async fn explicit_prompt_cache_key_takes_precedence_over_grok_session_id() {
    let captures = Captures::default();
    let upstream_url = spawn_upstream(captures.clone()).await;
    let app = build_router(
        ProxyConfig::for_test(upstream_url, "local-secret")
            .with_installation_id("proxy-installation-1"),
        Arc::new(StubCredentials {
            refreshes: Arc::new(AtomicUsize::new(0)),
        }),
    )
    .expect("router");

    let response = tower::ServiceExt::oneshot(
        app,
        axum::http::Request::post("/v1/responses")
            .header("authorization", "Bearer local-secret")
            .header("content-type", "application/json")
            .header("x-grok-session-id", "grok-session-ignored")
            .header("x-grok-thread-id", "grok-thread-42")
            .body(Body::from(
                r#"{"prompt_cache_key":"explicit-cache-key","input":[]}"#,
            ))
            .expect("request"),
    )
    .await
    .expect("response");

    assert_eq!(response.status(), StatusCode::OK);
    let captures = captures.0.lock().await;
    assert_eq!(captures[0].0["session-id"], "grok-session-ignored");
    assert_eq!(captures[0].0["thread-id"], "grok-thread-42");
    assert_eq!(captures[0].1["prompt_cache_key"], "explicit-cache-key");
}

#[derive(Clone, Default)]
struct WebSocketCaptures {
    headers: Arc<StdMutex<Option<HeaderMap>>>,
    request: Arc<Mutex<Option<Value>>>,
}

#[derive(Clone, Default)]
struct ReuseWebSocketCaptures {
    connections: Arc<AtomicUsize>,
    pongs: Arc<AtomicUsize>,
    requests: Arc<Mutex<Vec<Value>>>,
}

#[allow(clippy::result_large_err)]
async fn spawn_reusing_websocket_upstream(captures: ReuseWebSocketCaptures) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind reusable websocket");
    let address = listener.local_addr().expect("reusable websocket addr");
    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.expect("accept reusable websocket");
            captures.connections.fetch_add(1, Ordering::SeqCst);
            let captures = captures.clone();
            tokio::spawn(async move {
                let mut websocket = accept_hdr_async(
                    stream,
                    |_request: &WebSocketRequest, mut response: WebSocketResponse| {
                        response.headers_mut().insert(
                            "x-codex-turn-state",
                            "handshake-turn-state".parse().expect("turn state header"),
                        );
                        Ok(response)
                    },
                )
                .await
                .expect("reusable websocket handshake");
                while let Some(message) = websocket.next().await {
                    let Message::Text(text) = message.expect("valid reusable websocket message")
                    else {
                        continue;
                    };
                    let request: Value =
                        serde_json::from_str(&text).expect("reusable websocket request json");
                    let request_index = {
                        let mut requests = captures.requests.lock().await;
                        requests.push(request);
                        requests.len()
                    };
                    websocket
                        .send(Message::Text(
                            json!({
                                "type": "codex.response.metadata",
                                "headers": {
                                    "x-codex-turn-state": format!("turn-state-{request_index}")
                                }
                            })
                            .to_string()
                            .into(),
                        ))
                        .await
                        .expect("send reusable metadata");
                    let output = match request_index {
                        1 => json!([{
                            "type": "message",
                            "id": "msg-1",
                            "role": "assistant",
                            "status": "completed",
                            "content": [{
                                "type": "output_text",
                                "text": "first",
                                "annotations": []
                            }]
                        }]),
                        2 => json!([{
                            "type": "function_call",
                            "id": "fc-2",
                            "call_id": "call-2",
                            "name": "lookup",
                            "arguments": "{}",
                            "status": "completed"
                        }]),
                        _ => json!([]),
                    };
                    websocket
                        .send(Message::Text(
                            json!({
                                "type": "response.completed",
                                "response": {
                                    "id": format!("resp-{request_index}"),
                                    "status": "completed",
                                    "output": output,
                                    "usage": {
                                        "input_tokens": 100,
                                        "input_tokens_details": {"cached_tokens": 80}
                                    }
                                }
                            })
                            .to_string()
                            .into(),
                        ))
                        .await
                        .expect("send reusable completion");
                    websocket
                        .send(Message::Text(
                            json!({
                                "type": "responsesapi.websocket_timing",
                                "elapsed_ms": 1
                            })
                            .to_string()
                            .into(),
                        ))
                        .await
                        .expect("send trailing reusable metadata");
                    websocket
                        .send(Message::Text(
                            json!({"type": "keepalive"}).to_string().into(),
                        ))
                        .await
                        .expect("send idle reusable keepalive");
                    websocket
                        .send(Message::Ping(
                            format!("idle-ping-{request_index}").into_bytes().into(),
                        ))
                        .await
                        .expect("send idle reusable ping");
                    let pong = tokio::time::timeout(
                        std::time::Duration::from_millis(250),
                        websocket.next(),
                    )
                    .await
                    .expect("idle ping must receive a prompt pong")
                    .expect("websocket remains open after idle ping")
                    .expect("valid idle pong");
                    assert!(
                        matches!(pong, Message::Pong(_)),
                        "expected idle pong, got {pong:?}"
                    );
                    captures.pongs.fetch_add(1, Ordering::SeqCst);
                }
            });
        }
    });
    format!("ws://{address}/responses")
}

#[allow(clippy::result_large_err)]
async fn spawn_closing_websocket_upstream(captures: ReuseWebSocketCaptures) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind closing websocket");
    let address = listener.local_addr().expect("closing websocket addr");
    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.expect("accept closing websocket");
            captures.connections.fetch_add(1, Ordering::SeqCst);
            let captures = captures.clone();
            tokio::spawn(async move {
                let mut websocket = accept_hdr_async(
                    stream,
                    |_request: &WebSocketRequest, response: WebSocketResponse| Ok(response),
                )
                .await
                .expect("closing websocket handshake");
                let Some(Ok(Message::Text(text))) = websocket.next().await else {
                    return;
                };
                let request: Value =
                    serde_json::from_str(&text).expect("closing websocket request json");
                let request_index = {
                    let mut requests = captures.requests.lock().await;
                    requests.push(request);
                    requests.len()
                };
                websocket
                    .send(Message::Text(
                        json!({
                            "type": "response.completed",
                            "response": {
                                "id": format!("closing-resp-{request_index}"),
                                "status": "completed",
                                "output": []
                            }
                        })
                        .to_string()
                        .into(),
                    ))
                    .await
                    .expect("send closing completion");
                websocket
                    .close(None)
                    .await
                    .expect("close websocket after completion");
            });
        }
    });
    format!("ws://{address}/responses")
}

#[allow(clippy::result_large_err)]
async fn spawn_stalling_then_recovering_websocket_upstream(
    connections: Arc<AtomicUsize>,
) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind recovering websocket");
    let address = listener.local_addr().expect("recovering websocket addr");
    tokio::spawn(async move {
        loop {
            let (stream, _) = listener
                .accept()
                .await
                .expect("accept recovering websocket");
            let connection_index = connections.fetch_add(1, Ordering::SeqCst) + 1;
            tokio::spawn(async move {
                let mut websocket = accept_hdr_async(
                    stream,
                    |_request: &WebSocketRequest, response: WebSocketResponse| Ok(response),
                )
                .await
                .expect("recovering websocket handshake");
                if connection_index == 1 {
                    std::future::pending::<()>().await;
                }
                let Some(Ok(Message::Text(_))) = websocket.next().await else {
                    return;
                };
                websocket
                    .send(Message::Text(
                        r#"{"type":"response.completed","response":{"id":"resp-recovered","status":"completed","output":[]}}"#
                            .into(),
                    ))
                    .await
                    .expect("send recovered completion");
            });
        }
    });
    format!("ws://{address}/responses")
}

#[allow(clippy::result_large_err)]
async fn spawn_refreshing_websocket_upstream(attempts: Arc<AtomicUsize>) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind refreshing websocket");
    let address = listener.local_addr().expect("refreshing websocket addr");
    tokio::spawn(async move {
        loop {
            let (stream, _) = listener
                .accept()
                .await
                .expect("accept refreshing websocket");
            attempts.fetch_add(1, Ordering::SeqCst);
            tokio::spawn(async move {
                let websocket = accept_hdr_async(
                    stream,
                    |request: &WebSocketRequest, response: WebSocketResponse| {
                        if request.headers().get("authorization")
                            != Some(&"Bearer refreshed-secret".parse().expect("auth header"))
                        {
                            return Err(http::Response::builder()
                                .status(StatusCode::UNAUTHORIZED)
                                .body(Some("expired".to_string()))
                                .expect("unauthorized websocket response"));
                        }
                        Ok(response)
                    },
                )
                .await;
                let Ok(mut websocket) = websocket else {
                    return;
                };
                let Some(Ok(Message::Text(_))) = websocket.next().await else {
                    return;
                };
                websocket
                    .send(Message::Text(
                        r#"{"type":"response.completed","response":{"id":"resp-refreshed","status":"completed","output":[]}}"#
                            .into(),
                    ))
                    .await
                    .expect("send refreshed completion");
            });
        }
    });
    format!("ws://{address}/responses")
}

#[allow(clippy::result_large_err)]
async fn spawn_incremental_retry_websocket_upstream(
    captures: ReuseWebSocketCaptures,
    retry_event: Value,
) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind previous-missing websocket");
    let address = listener
        .local_addr()
        .expect("previous-missing websocket addr");
    tokio::spawn(async move {
        let (stream, _) = listener
            .accept()
            .await
            .expect("accept previous-missing websocket");
        captures.connections.fetch_add(1, Ordering::SeqCst);
        let mut websocket = accept_hdr_async(
            stream,
            |_request: &WebSocketRequest, response: WebSocketResponse| Ok(response),
        )
        .await
        .expect("previous-missing websocket handshake");
        while let Some(Ok(Message::Text(text))) = websocket.next().await {
            let request: Value =
                serde_json::from_str(&text).expect("previous-missing request json");
            let request_index = {
                let mut requests = captures.requests.lock().await;
                requests.push(request);
                requests.len()
            };
            let event = match request_index {
                1 => json!({
                    "type": "response.completed",
                    "response": {
                        "id": "resp-base",
                        "status": "completed",
                        "output": [{
                            "type": "message",
                            "id": "msg-base",
                            "role": "assistant",
                            "status": "completed",
                            "content": [{
                                "type": "output_text",
                                "text": "base",
                                "annotations": []
                            }]
                        }]
                    }
                }),
                2 => retry_event.clone(),
                _ => json!({
                    "type": "response.completed",
                    "response": {
                        "id": "resp-retried",
                        "status": "completed",
                        "output": [{
                            "type": "message",
                            "id": "msg-retried",
                            "role": "assistant",
                            "status": "completed",
                            "content": [{
                                "type": "output_text",
                                "text": "retried",
                                "annotations": []
                            }]
                        }]
                    }
                }),
            };
            websocket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("send previous-missing event");
        }
    });
    format!("ws://{address}/responses")
}

#[allow(clippy::result_large_err)]
async fn spawn_websocket_upstream(captures: WebSocketCaptures) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind websocket");
    let address = listener.local_addr().expect("websocket addr");
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept websocket");
        let header_capture = captures.headers.clone();
        let mut websocket = accept_hdr_async(
            stream,
            move |request: &WebSocketRequest, response: WebSocketResponse| {
                *header_capture.lock().expect("capture websocket headers") =
                    Some(request.headers().clone());
                Ok(response)
            },
        )
        .await
        .expect("websocket handshake");
        let message = websocket
            .next()
            .await
            .expect("websocket request")
            .expect("valid websocket request");
        let Message::Text(text) = message else {
            panic!("expected text websocket request");
        };
        *captures.request.lock().await =
            Some(serde_json::from_str(&text).expect("websocket request json"));
        websocket
            .send(Message::Text(
                r#"{"type":"responsesapi.websocket_timing","elapsed_ms":1}"#.into(),
            ))
            .await
            .expect("send timing");
        websocket
            .send(Message::Text(
                r#"{"type":"codex.response.metadata","request_id":"request-ws"}"#.into(),
            ))
            .await
            .expect("send metadata");
        websocket
            .send(Message::Text(
                r#"{"type":"response.created","response":{"id":"resp-ws"}}"#.into(),
            ))
            .await
            .expect("send created");
        websocket
            .send(Message::Text(
                r#"{"type":"response.completed","response":{"id":"resp-ws","status":"completed","output":[]}}"#
                    .into(),
            ))
            .await
            .expect("send completed");
    });
    format!("ws://{address}/responses")
}

async fn spawn_connect_proxy(
    target: std::net::SocketAddr,
    captured_request: Arc<Mutex<Option<String>>>,
) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind CONNECT proxy");
    let address = listener.local_addr().expect("CONNECT proxy addr");
    tokio::spawn(async move {
        let (mut downstream, _) = listener.accept().await.expect("accept CONNECT");
        let mut request = Vec::new();
        let mut buffer = [0_u8; 512];
        while !request.windows(4).any(|window| window == b"\r\n\r\n") {
            let read = downstream.read(&mut buffer).await.expect("read CONNECT");
            if read == 0 {
                return;
            }
            request.extend_from_slice(&buffer[..read]);
        }
        *captured_request.lock().await =
            Some(String::from_utf8(request).expect("CONNECT request UTF-8"));
        let mut upstream = tokio::net::TcpStream::connect(target)
            .await
            .expect("connect websocket target");
        downstream
            .write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n")
            .await
            .expect("write CONNECT response");
        tokio::io::copy_bidirectional(&mut downstream, &mut upstream)
            .await
            .expect("relay CONNECT tunnel");
    });
    format!("http://{address}")
}

async fn spawn_stalling_connect_proxy() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind stalling CONNECT proxy");
    let address = listener.local_addr().expect("stalling proxy addr");
    tokio::spawn(async move {
        let (_stream, _) = listener.accept().await.expect("accept stalled CONNECT");
        std::future::pending::<()>().await;
    });
    format!("http://{address}")
}

#[tokio::test]
async fn websocket_is_preferred_and_bridged_to_downstream_sse() {
    let http_captures = Captures::default();
    let http_url = spawn_upstream(http_captures.clone()).await;
    let websocket_captures = WebSocketCaptures::default();
    let websocket_url = spawn_websocket_upstream(websocket_captures.clone()).await;
    let app = build_router(
        ProxyConfig::for_test(http_url, "local-secret")
            .with_installation_id("proxy-installation-1")
            .with_websocket_upstream(websocket_url),
        Arc::new(StubCredentials {
            refreshes: Arc::new(AtomicUsize::new(0)),
        }),
    )
    .expect("router");

    let response = tower::ServiceExt::oneshot(
        app,
        axum::http::Request::post("/v1/responses")
            .header("authorization", "Bearer local-secret")
            .header("content-type", "application/json")
            .header("x-request-id", "request-ws-test")
            .header("x-grok-session-id", "grok-session-42")
            .header("x-grok-thread-id", "grok-thread-42")
            .body(Body::from(r#"{"model":"gpt-test","input":[]}"#))
            .expect("request"),
    )
    .await
    .expect("response");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers()["x-codex-upstream-transport"],
        "websocket"
    );
    let body = response
        .into_body()
        .collect()
        .await
        .expect("websocket response body")
        .to_bytes();
    let body = String::from_utf8(body.to_vec()).expect("utf8 SSE");
    assert!(body.contains(r#""type":"response.created""#));
    assert!(body.contains(r#""type":"response.completed""#));
    assert!(!body.contains("responsesapi.websocket_timing"));
    assert!(!body.contains("codex.response.metadata"));
    assert!(http_captures.0.lock().await.is_empty());

    {
        let headers = websocket_captures
            .headers
            .lock()
            .expect("captured websocket headers lock");
        let headers = headers.as_ref().expect("captured websocket headers");
        assert_eq!(headers["session-id"], "grok-session-42");
        assert_eq!(headers["thread-id"], "grok-thread-42");
        assert_eq!(headers["x-client-request-id"], "grok-thread-42");
        assert_eq!(headers["x-codex-window-id"], "grok-session-42");
        assert_eq!(headers["openai-beta"], "responses_websockets=2026-02-06");
    }
    let request = websocket_captures.request.lock().await;
    let request = request.as_ref().expect("captured websocket request");
    assert_eq!(request["type"], "response.create");
    assert_eq!(request["prompt_cache_key"], "grok-session-42");
    assert_eq!(
        request["client_metadata"]["x-codex-installation-id"],
        "proxy-installation-1"
    );
    assert_eq!(
        request["client_metadata"]["x-codex-window-id"],
        "grok-session-42"
    );
    assert_eq!(request["client_metadata"]["session_id"], "grok-session-42");
    assert_eq!(request["client_metadata"]["thread_id"], "grok-thread-42");
    assert_eq!(request["client_metadata"]["turn_id"], "request-ws-test");
}

#[tokio::test]
async fn websocket_uses_an_http_connect_proxy() {
    let http_captures = Captures::default();
    let http_url = spawn_upstream(http_captures.clone()).await;
    let websocket_captures = WebSocketCaptures::default();
    let websocket_url = spawn_websocket_upstream(websocket_captures).await;
    let websocket_address = websocket_url
        .strip_prefix("ws://")
        .and_then(|url| url.split('/').next())
        .expect("websocket authority")
        .parse()
        .expect("websocket socket address");
    let connect_request = Arc::new(Mutex::new(None));
    let proxy_url = spawn_connect_proxy(websocket_address, connect_request.clone()).await;
    let config = ProxyConfig::for_test(http_url, "local-secret")
        .with_websocket_upstream(websocket_url)
        .with_websocket_proxy(&proxy_url)
        .expect("valid websocket proxy");
    let app = build_router(
        config,
        Arc::new(StubCredentials {
            refreshes: Arc::new(AtomicUsize::new(0)),
        }),
    )
    .expect("router");

    let response = tower::ServiceExt::oneshot(
        app,
        axum::http::Request::post("/v1/responses")
            .header("authorization", "Bearer local-secret")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"model":"gpt-test","input":[]}"#))
            .expect("request"),
    )
    .await
    .expect("response");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers()["x-codex-upstream-transport"],
        "websocket"
    );
    let _ = response.into_body().collect().await.expect("response body");
    let connect_request = connect_request
        .lock()
        .await
        .clone()
        .expect("captured CONNECT request");
    assert!(connect_request.starts_with(&format!("CONNECT {websocket_address} HTTP/1.1\r\n")));
    assert!(http_captures.0.lock().await.is_empty());
}

#[tokio::test]
async fn websocket_connect_timeout_falls_back_to_http_sse() {
    let captures = Captures::default();
    let http_url = spawn_upstream(captures.clone()).await;
    let websocket_url = http_url.replacen("http://", "ws://", 1);
    let proxy_url = spawn_stalling_connect_proxy().await;
    let config = ProxyConfig::for_test(&http_url, "local-secret")
        .with_websocket_upstream(websocket_url)
        .with_websocket_proxy(proxy_url)
        .expect("valid websocket proxy")
        .with_upstream_timeouts(Duration::from_millis(50), Duration::from_secs(1));
    let app = build_router(
        config,
        Arc::new(StubCredentials {
            refreshes: Arc::new(AtomicUsize::new(0)),
        }),
    )
    .expect("router");

    let response = tower::ServiceExt::oneshot(
        app,
        axum::http::Request::post("/v1/responses")
            .header("authorization", "Bearer local-secret")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"model":"gpt-test","input":[]}"#))
            .expect("request"),
    )
    .await
    .expect("response");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["x-codex-upstream-transport"], "http-sse");
    assert_eq!(captures.0.lock().await.len(), 1);
}

#[tokio::test]
async fn websocket_send_timeout_reconnects_and_retries_once() {
    let http_captures = Captures::default();
    let http_url = spawn_upstream(http_captures.clone()).await;
    let connections = Arc::new(AtomicUsize::new(0));
    let websocket_url =
        spawn_stalling_then_recovering_websocket_upstream(connections.clone()).await;
    let app = build_router(
        ProxyConfig::for_test(http_url, "local-secret")
            .with_websocket_upstream(websocket_url)
            .with_websocket_send_timeout(Duration::from_millis(500)),
        Arc::new(StubCredentials {
            refreshes: Arc::new(AtomicUsize::new(0)),
        }),
    )
    .expect("router");
    let large_input = "x".repeat(10 * 1024 * 1024);
    let request = json!({
        "model": "gpt-test",
        "input": [{"type":"message","role":"user","content":large_input}]
    });

    let response = tokio::time::timeout(
        Duration::from_secs(5),
        tower::ServiceExt::oneshot(
            app,
            axum::http::Request::post("/v1/responses")
                .header("authorization", "Bearer local-secret")
                .header("content-type", "application/json")
                .header("x-grok-session-id", "grok-session-send-timeout")
                .body(Body::from(request.to_string()))
                .expect("request"),
        ),
    )
    .await
    .expect("proxy request must not hang")
    .expect("response");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers()["x-codex-upstream-transport"],
        "websocket"
    );
    response
        .into_body()
        .collect()
        .await
        .expect("collect recovered websocket response");
    assert_eq!(connections.load(Ordering::SeqCst), 2);
    assert!(http_captures.0.lock().await.is_empty());
}

#[tokio::test]
async fn production_model_allowlist_rejects_non_codex_models() {
    let captures = Captures::default();
    let http_url = spawn_upstream(captures.clone()).await;
    let config = ProxyConfig::for_test(http_url, "local-secret").with_allowed_models(["gpt-test"]);
    let app = build_router(
        config,
        Arc::new(StubCredentials {
            refreshes: Arc::new(AtomicUsize::new(0)),
        }),
    )
    .expect("router");

    let response = tower::ServiceExt::oneshot(
        app,
        axum::http::Request::post("/v1/responses")
            .header("authorization", "Bearer local-secret")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"model":"grok-4.5","input":[]}"#))
            .expect("request"),
    )
    .await
    .expect("response");

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = response
        .into_body()
        .collect()
        .await
        .expect("error body")
        .to_bytes();
    assert!(String::from_utf8_lossy(&body).contains("unsupported Codex model `grok-4.5`"));
    assert!(captures.0.lock().await.is_empty());
}

#[tokio::test]
async fn websocket_handshake_failure_falls_back_to_http_sse() {
    let captures = Captures::default();
    let http_url = spawn_upstream(captures.clone()).await;
    let websocket_url = http_url.replacen("http://", "ws://", 1);
    let app = build_router(
        ProxyConfig::for_test(&http_url, "local-secret")
            .with_installation_id("proxy-installation-1")
            .with_websocket_upstream(websocket_url),
        Arc::new(StubCredentials {
            refreshes: Arc::new(AtomicUsize::new(0)),
        }),
    )
    .expect("router");

    let response = tower::ServiceExt::oneshot(
        app,
        axum::http::Request::post("/v1/responses")
            .header("authorization", "Bearer local-secret")
            .header("content-type", "application/json")
            .header("x-grok-session-id", "grok-thread-42")
            .body(Body::from(r#"{"model":"gpt-test","input":[]}"#))
            .expect("request"),
    )
    .await
    .expect("response");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["x-codex-upstream-transport"], "http-sse");
    assert_eq!(captures.0.lock().await.len(), 1);
}

#[tokio::test]
async fn websocket_connection_and_previous_response_are_reused_conservatively() {
    let http_captures = Captures::default();
    let http_url = spawn_upstream(http_captures.clone()).await;
    let websocket_captures = ReuseWebSocketCaptures::default();
    let websocket_url = spawn_reusing_websocket_upstream(websocket_captures.clone()).await;
    let app = build_router(
        ProxyConfig::for_test(http_url, "local-secret")
            .with_installation_id("proxy-installation-1")
            .with_websocket_upstream(websocket_url),
        Arc::new(StubCredentials {
            refreshes: Arc::new(AtomicUsize::new(0)),
        }),
    )
    .expect("router");

    let user_one = json!({
        "type": "message",
        "role": "user",
        "content": [{"type": "input_text", "text": "first"}]
    });
    let assistant_one = json!({
        "type": "message",
        "id": "msg-1",
        "role": "assistant",
        "status": "completed",
        "content": [{
            "type": "output_text",
            "text": "first",
            "annotations": []
        }]
    });
    let user_two = json!({
        "type": "message",
        "role": "user",
        "content": [{"type": "input_text", "text": "use a tool"}]
    });
    let function_call = json!({
        "type": "function_call",
        "id": "fc-2",
        "call_id": "call-2",
        "name": "lookup",
        "arguments": "{}",
        "status": "completed"
    });
    let function_output = json!({
        "type": "function_call_output",
        "call_id": "call-2",
        "output": "done"
    });

    for (request_index, (request_id, input)) in [
        ("reuse-1", json!([user_one.clone()])),
        (
            "reuse-2",
            json!([user_one.clone(), assistant_one, user_two.clone()]),
        ),
        (
            "reuse-3",
            json!([
                user_one,
                json!({
                    "type": "message",
                    "id": "msg-1",
                    "role": "assistant",
                    "status": "completed",
                    "content": [{
                        "type": "output_text",
                        "text": "first",
                        "annotations": []
                    }]
                }),
                user_two,
                function_call,
                function_output.clone()
            ]),
        ),
    ]
    .into_iter()
    .enumerate()
    {
        let response = tower::ServiceExt::oneshot(
            app.clone(),
            axum::http::Request::post("/v1/responses")
                .header("authorization", "Bearer local-secret")
                .header("content-type", "application/json")
                .header("x-request-id", request_id)
                .header("x-grok-session-id", "grok-session-reuse")
                .body(Body::from(
                    json!({"model":"gpt-test","input":input}).to_string(),
                ))
                .expect("request"),
        )
        .await
        .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = response
            .into_body()
            .collect()
            .await
            .expect("collect reusable websocket response")
            .to_bytes();
        assert!(
            !body
                .windows(br#""type":"keepalive""#.len())
                .any(|part| part == br#""type":"keepalive""#)
        );
        tokio::time::timeout(std::time::Duration::from_millis(250), async {
            while websocket_captures.pongs.load(Ordering::SeqCst) <= request_index {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("proxy must answer websocket pings while idle between requests");
    }

    assert_eq!(websocket_captures.connections.load(Ordering::SeqCst), 1);
    assert!(http_captures.0.lock().await.is_empty());
    let requests = websocket_captures.requests.lock().await;
    assert_eq!(requests.len(), 3);
    assert_eq!(requests[1]["previous_response_id"], "resp-1");
    assert_eq!(
        requests[1]["input"],
        json!([{
            "type": "message",
            "role": "user",
            "content": [{"type": "input_text", "text": "use a tool"}]
        }])
    );
    assert!(
        requests[1]["client_metadata"]
            .get("x-codex-turn-state")
            .is_none()
    );
    assert_eq!(requests[2]["previous_response_id"], "resp-2");
    assert_eq!(requests[2]["input"], json!([function_output]));
    assert_eq!(
        requests[2]["client_metadata"]["x-codex-turn-state"],
        "turn-state-2"
    );
}

#[tokio::test]
async fn closed_cached_websocket_is_reconnected_before_the_next_request() {
    let http_captures = Captures::default();
    let http_url = spawn_upstream(http_captures.clone()).await;
    let websocket_captures = ReuseWebSocketCaptures::default();
    let websocket_url = spawn_closing_websocket_upstream(websocket_captures.clone()).await;
    let app = build_router(
        ProxyConfig::for_test(http_url, "local-secret").with_websocket_upstream(websocket_url),
        Arc::new(StubCredentials {
            refreshes: Arc::new(AtomicUsize::new(0)),
        }),
    )
    .expect("router");

    for request_id in ["closing-1", "closing-2"] {
        let response = tower::ServiceExt::oneshot(
            app.clone(),
            axum::http::Request::post("/v1/responses")
                .header("authorization", "Bearer local-secret")
                .header("content-type", "application/json")
                .header("x-request-id", request_id)
                .header("x-grok-session-id", "grok-session-closing")
                .body(Body::from(
                    json!({
                        "model": "gpt-test",
                        "input": [{
                            "type": "message",
                            "role": "user",
                            "content": request_id
                        }]
                    })
                    .to_string(),
                ))
                .expect("request"),
        )
        .await
        .expect("response");
        response
            .into_body()
            .collect()
            .await
            .expect("collect response after reconnect");
    }

    assert_eq!(websocket_captures.connections.load(Ordering::SeqCst), 2);
    assert_eq!(websocket_captures.requests.lock().await.len(), 2);
    assert!(http_captures.0.lock().await.is_empty());
}

#[tokio::test]
async fn websocket_unauthorized_refreshes_once_without_falling_back_to_http() {
    let http_captures = Captures::default();
    let http_url = spawn_upstream(http_captures.clone()).await;
    let websocket_attempts = Arc::new(AtomicUsize::new(0));
    let websocket_url = spawn_refreshing_websocket_upstream(websocket_attempts.clone()).await;
    let refreshes = Arc::new(AtomicUsize::new(0));
    let app = build_router(
        ProxyConfig::for_test(http_url, "local-secret").with_websocket_upstream(websocket_url),
        Arc::new(StubCredentials {
            refreshes: refreshes.clone(),
        }),
    )
    .expect("router");

    let response = tower::ServiceExt::oneshot(
        app,
        axum::http::Request::post("/v1/responses")
            .header("authorization", "Bearer local-secret")
            .header("content-type", "application/json")
            .header("x-grok-session-id", "grok-session-refresh")
            .body(Body::from(r#"{"model":"gpt-test","input":[]}"#))
            .expect("request"),
    )
    .await
    .expect("response");
    response
        .into_body()
        .collect()
        .await
        .expect("collect refreshed websocket response");

    assert_eq!(websocket_attempts.load(Ordering::SeqCst), 2);
    assert_eq!(refreshes.load(Ordering::SeqCst), 1);
    assert!(http_captures.0.lock().await.is_empty());
}

#[tokio::test]
async fn previous_response_not_found_retries_once_with_the_full_request() {
    let http_captures = Captures::default();
    let http_url = spawn_upstream(http_captures.clone()).await;
    let websocket_captures = ReuseWebSocketCaptures::default();
    let websocket_url = spawn_incremental_retry_websocket_upstream(
        websocket_captures.clone(),
        json!({
            "type": "error",
            "error": {
                "type": "invalid_request_error",
                "code": "previous_response_not_found",
                "message": "retry the full request"
            }
        }),
    )
    .await;
    let app = build_router(
        ProxyConfig::for_test(http_url, "local-secret").with_websocket_upstream(websocket_url),
        Arc::new(StubCredentials {
            refreshes: Arc::new(AtomicUsize::new(0)),
        }),
    )
    .expect("router");
    let user = json!({"type":"message","role":"user","content":"base"});
    let assistant = json!({
        "type": "message",
        "id": "msg-base",
        "role": "assistant",
        "status": "completed",
        "content": [{
            "type": "output_text",
            "text": "base",
            "annotations": []
        }]
    });
    let next_user = json!({"type":"message","role":"user","content":"next"});

    for (request_id, input) in [
        ("previous-1", json!([user.clone()])),
        (
            "previous-2",
            json!([user.clone(), assistant, next_user.clone()]),
        ),
    ] {
        let response = tower::ServiceExt::oneshot(
            app.clone(),
            axum::http::Request::post("/v1/responses")
                .header("authorization", "Bearer local-secret")
                .header("content-type", "application/json")
                .header("x-request-id", request_id)
                .header("x-grok-session-id", "grok-session-previous")
                .body(Body::from(
                    json!({"model":"gpt-test","input":input}).to_string(),
                ))
                .expect("request"),
        )
        .await
        .expect("response");
        let body = response
            .into_body()
            .collect()
            .await
            .expect("collect previous-response retry")
            .to_bytes();
        assert!(
            !body
                .windows("previous_response_not_found".len())
                .any(|part| { part == b"previous_response_not_found" })
        );
    }

    assert_eq!(websocket_captures.connections.load(Ordering::SeqCst), 1);
    let requests = websocket_captures.requests.lock().await;
    assert_eq!(requests.len(), 3);
    assert_eq!(requests[1]["previous_response_id"], "resp-base");
    assert_eq!(requests[1]["input"], json!([next_user]));
    assert!(requests[2].get("previous_response_id").is_none());
    assert_eq!(requests[2]["input"].as_array().map(Vec::len), Some(3));
    assert!(http_captures.0.lock().await.is_empty());
}

#[tokio::test]
async fn empty_incremental_terminal_response_retries_once_with_the_full_request() {
    let http_captures = Captures::default();
    let http_url = spawn_upstream(http_captures.clone()).await;
    let websocket_captures = ReuseWebSocketCaptures::default();
    let websocket_url = spawn_incremental_retry_websocket_upstream(
        websocket_captures.clone(),
        json!({
            "type": "response.completed",
            "response": {
                "id": "resp-empty",
                "status": "completed",
                "output": []
            }
        }),
    )
    .await;
    let app = build_router(
        ProxyConfig::for_test(http_url, "local-secret").with_websocket_upstream(websocket_url),
        Arc::new(StubCredentials {
            refreshes: Arc::new(AtomicUsize::new(0)),
        }),
    )
    .expect("router");
    let user = json!({"type":"message","role":"user","content":"base"});
    let assistant = json!({
        "type": "message",
        "id": "msg-base",
        "role": "assistant",
        "status": "completed",
        "content": [{
            "type": "output_text",
            "text": "base",
            "annotations": []
        }]
    });
    let next_user = json!({"type":"message","role":"user","content":"next"});

    for (request_id, input) in [
        ("empty-1", json!([user.clone()])),
        (
            "empty-2",
            json!([user.clone(), assistant, next_user.clone()]),
        ),
    ] {
        let response = tower::ServiceExt::oneshot(
            app.clone(),
            axum::http::Request::post("/v1/responses")
                .header("authorization", "Bearer local-secret")
                .header("content-type", "application/json")
                .header("x-request-id", request_id)
                .header("x-grok-session-id", "grok-session-empty")
                .body(Body::from(
                    json!({"model":"gpt-test","input":input}).to_string(),
                ))
                .expect("request"),
        )
        .await
        .expect("response");
        let body = response
            .into_body()
            .collect()
            .await
            .expect("collect empty-response retry")
            .to_bytes();
        if request_id == "empty-2" {
            let body = String::from_utf8(body.to_vec()).expect("utf8 SSE");
            assert!(body.contains("retried"));
            assert!(!body.contains("resp-empty"));
        }
    }

    assert_eq!(websocket_captures.connections.load(Ordering::SeqCst), 1);
    let requests = websocket_captures.requests.lock().await;
    assert_eq!(requests.len(), 3);
    assert_eq!(requests[1]["previous_response_id"], "resp-base");
    assert_eq!(requests[1]["input"], json!([next_user]));
    assert!(requests[2].get("previous_response_id").is_none());
    assert_eq!(requests[2]["input"].as_array().map(Vec::len), Some(3));
    assert!(http_captures.0.lock().await.is_empty());
}

#[tokio::test]
async fn usage_limit_404_is_mapped_to_429_and_preserves_retry_after() {
    async fn usage_limited_upstream() -> Response {
        Response::builder()
            .status(StatusCode::NOT_FOUND)
            .header("content-type", "application/json")
            .header("retry-after", "17")
            .body(Body::from(
                r#"{"error":{"type":"usage_limit_reached","message":"limit reached"}}"#,
            ))
            .expect("response")
    }

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let address = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new().route("/responses", post(usage_limited_upstream)),
        )
        .await
        .expect("server");
    });

    let app = build_router(
        ProxyConfig::for_test(format!("http://{address}/responses"), "local-secret")
            .with_installation_id("private-installation"),
        Arc::new(StubCredentials {
            refreshes: Arc::new(AtomicUsize::new(0)),
        }),
    )
    .expect("router");
    let response = tower::ServiceExt::oneshot(
        app,
        axum::http::Request::post("/v1/responses")
            .header("authorization", "Bearer local-secret")
            .header("content-type", "application/json")
            .body(Body::from("{}"))
            .expect("request"),
    )
    .await
    .expect("response");

    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(response.headers()["retry-after"], "17");
    let body = response
        .into_body()
        .collect()
        .await
        .expect("body")
        .to_bytes();
    assert!(
        body.windows("usage_limit_reached".len())
            .any(|part| part == b"usage_limit_reached")
    );
}

#[tokio::test]
async fn proxy_clamps_reasoning_effort_using_the_selected_models_catalog_entry() {
    let captures = Captures::default();
    let upstream_url = spawn_upstream(captures.clone()).await;
    let config = ProxyConfig::for_test(upstream_url, "local-secret").with_model_reasoning(
        "gpt-5.5",
        vec!["low".into(), "medium".into(), "high".into(), "xhigh".into()],
        Some("medium".into()),
    );
    let app = build_router(
        config,
        Arc::new(StubCredentials {
            refreshes: Arc::new(AtomicUsize::new(0)),
        }),
    )
    .expect("router");

    let response = tower::ServiceExt::oneshot(
        app,
        axum::http::Request::post("/v1/responses")
            .header("authorization", "Bearer local-secret")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"model":"gpt-5.5","reasoning":{"effort":"max"},"input":[]}"#,
            ))
            .expect("request"),
    )
    .await
    .expect("response");

    assert_eq!(response.status(), StatusCode::OK);
    let captures = captures.0.lock().await;
    assert_eq!(captures[0].1["reasoning"]["effort"], "medium");
}

#[tokio::test]
async fn upstream_401_refreshes_and_retries_only_once() {
    #[derive(Clone)]
    struct RetryState(Arc<AtomicUsize>);
    async fn retry_upstream(State(state): State<RetryState>) -> StatusCode {
        let attempt = state.0.fetch_add(1, Ordering::SeqCst);
        if attempt == 0 {
            StatusCode::UNAUTHORIZED
        } else {
            StatusCode::OK
        }
    }

    let attempts = Arc::new(AtomicUsize::new(0));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let address = listener.local_addr().expect("addr");
    let state = RetryState(attempts.clone());
    tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new()
                .route("/responses", post(retry_upstream))
                .with_state(state),
        )
        .await
        .expect("server");
    });

    let refreshes = Arc::new(AtomicUsize::new(0));
    let credentials = StubCredentials {
        refreshes: refreshes.clone(),
    };
    let app = build_router(
        ProxyConfig::for_test(format!("http://{address}/responses"), "local-secret"),
        Arc::new(credentials),
    )
    .expect("router");
    let response = tower::ServiceExt::oneshot(
        app,
        axum::http::Request::post("/v1/responses")
            .header("authorization", "Bearer local-secret")
            .header("content-type", "application/json")
            .body(Body::from("{}"))
            .expect("request"),
    )
    .await
    .expect("response");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    assert_eq!(refreshes.load(Ordering::SeqCst), 1);
}

#[derive(Clone, Default)]
struct LogCapture(Arc<StdMutex<Vec<u8>>>);

struct LogWriter(LogCapture);

impl Write for LogWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0
            .0
            .lock()
            .expect("log capture lock")
            .extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'writer> MakeWriter<'writer> for LogCapture {
    type Writer = LogWriter;

    fn make_writer(&'writer self) -> Self::Writer {
        LogWriter(self.clone())
    }
}

impl LogCapture {
    fn contents(&self) -> String {
        String::from_utf8(self.0.lock().expect("log capture lock").clone())
            .expect("UTF-8 tracing output")
    }
}

#[tokio::test(flavor = "current_thread")]
async fn logs_request_timing_upstream_id_sse_error_and_completion_without_error_message() {
    async fn erroring_upstream() -> Response {
        Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "text/event-stream")
            .header("x-request-id", "upstream-request-42")
            .body(Body::from(concat!(
                "data: {\"type\":\"response.created\"}\n\n",
                "data: {\"type\":\"keepalive\"}\n\n",
                "data: {\"type\":\"error\",\"error\":{",
                "\"type\":\"service_unavailable_error\",",
                "\"message\":\"diagnostic-body-must-not-be-logged\"}}\n\n",
                "data: {\"type\":\"response.completed\",\"response\":{",
                "\"usage\":{\"input_tokens\":1000,",
                "\"input_tokens_details\":{\"cached_tokens\":750}}}}\n\n"
            )))
            .expect("response")
    }

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let address = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new().route("/responses", post(erroring_upstream)),
        )
        .await
        .expect("server");
    });

    let app = build_router(
        ProxyConfig::for_test(format!("http://{address}/responses"), "local-secret"),
        Arc::new(StubCredentials {
            refreshes: Arc::new(AtomicUsize::new(0)),
        }),
    )
    .expect("router");
    let logs = LogCapture::default();
    let subscriber = tracing_subscriber::fmt()
        .with_ansi(false)
        .without_time()
        .with_writer(logs.clone())
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("install tracing capture");

    let response = tower::ServiceExt::oneshot(
        app,
        axum::http::Request::post("/v1/responses")
            .header("authorization", "Bearer local-secret")
            .header("content-type", "application/json")
            .header("x-request-id", "client-request-7")
            .header("x-grok-session-id", "private-grok-thread")
            .body(Body::from(
                r#"{"model":"gpt-5.6-sol","prompt_cache_key":"private-session-key","input":[]}"#,
            ))
            .expect("request"),
    )
    .await
    .expect("response");
    let body = response
        .into_body()
        .collect()
        .await
        .expect("stream body")
        .to_bytes();
    assert!(
        !body
            .windows(b"keepalive".len())
            .any(|part| part == b"keepalive")
    );

    let logs = logs.contents();
    assert!(logs.contains("Codex upstream response headers received"));
    assert!(logs.contains("client_request_id=\"client-request-7\""));
    assert!(logs.contains("upstream_request_id=\"upstream-request-42\""));
    assert!(logs.contains("response_headers_ms="));
    assert!(logs.contains("Codex upstream first SSE bytes received"));
    assert!(logs.contains("ttfb_ms="));
    assert!(logs.contains("Codex upstream SSE error received"));
    assert!(logs.contains("sse_error_type=\"service_unavailable_error\""));
    assert!(logs.contains("Codex proxy request completed"));
    assert!(logs.contains("total_ms="));
    assert!(logs.contains("outcome=\"sse_error\""));
    assert!(logs.contains("filtered_keepalive_events=1"));
    assert!(logs.contains("prompt_cache_key_present=true"));
    assert!(logs.contains("prompt_cache_key_fingerprint="));
    assert!(logs.contains("session_id_fingerprint="));
    assert!(logs.contains("thread_id_fingerprint="));
    assert!(logs.contains("cache_usage_present=true"));
    assert!(logs.contains("input_tokens=1000"));
    assert!(logs.contains("cached_input_tokens=750"));
    assert!(logs.contains("cache_hit_ratio_pct=75"));
    assert!(!logs.contains("private-session-key"));
    assert!(!logs.contains("private-installation"));
    assert!(!logs.contains("private-grok-thread"));
    assert!(!logs.contains("diagnostic-body-must-not-be-logged"));
}
