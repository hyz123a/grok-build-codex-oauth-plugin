use std::sync::{
    Arc, Mutex as StdMutex,
    atomic::{AtomicUsize, Ordering},
};
use std::{io, io::Write};

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
use codex_proxy::{CredentialProvider, ProxyConfig, build_router};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tokio::sync::Mutex;
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
        ProxyConfig::for_test(upstream_url, "local-secret"),
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
async fn explicit_prompt_cache_key_takes_precedence_over_grok_session_id() {
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
            .header("x-grok-session-id", "grok-session-ignored")
            .body(Body::from(
                r#"{"prompt_cache_key":"explicit-cache-key","input":[]}"#,
            ))
            .expect("request"),
    )
    .await
    .expect("response");

    assert_eq!(response.status(), StatusCode::OK);
    let captures = captures.0.lock().await;
    assert_eq!(captures[0].0["session-id"], "explicit-cache-key");
    assert_eq!(captures[0].0["thread-id"], "explicit-cache-key");
    assert_eq!(captures[0].1["prompt_cache_key"], "explicit-cache-key");
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
        ProxyConfig::for_test(format!("http://{address}/responses"), "local-secret"),
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
    assert!(logs.contains("cache_usage_present=true"));
    assert!(logs.contains("input_tokens=1000"));
    assert!(logs.contains("cached_input_tokens=750"));
    assert!(logs.contains("cache_hit_ratio_pct=75"));
    assert!(!logs.contains("private-session-key"));
    assert!(!logs.contains("diagnostic-body-must-not-be-logged"));
}
