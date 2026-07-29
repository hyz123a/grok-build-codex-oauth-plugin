use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

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
    assert_eq!(captures[0].1["store"], false);
    assert_eq!(captures[0].1["stream"], true);
    assert_eq!(
        captures[0].1["include"],
        json!(["reasoning.encrypted_content"])
    );
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
