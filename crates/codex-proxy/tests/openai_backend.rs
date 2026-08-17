use std::io::{self, Write};
use std::sync::{Arc, Mutex as StdMutex, atomic::AtomicUsize};

use async_trait::async_trait;
use axum::{
    Router,
    body::Body,
    extract::State,
    http::StatusCode,
    response::Response,
    routing::post,
};
use codex_oauth::AccessCredential;
use codex_proxy::{
    CredentialProvider, OpenAIBackendConfig, OpenAIBackends, ProxyConfig, build_router,
};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tracing_subscriber::fmt::MakeWriter;

#[derive(Clone, Default)]
struct Captures(Arc<StdMutex<Vec<Value>>>);

#[derive(Clone, Default)]
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
        self.refreshes.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(AccessCredential {
            access_token: "refreshed-secret".into(),
            account_id: "account-1".into(),
        })
    }
}

async fn openai_upstream(State(captures): State<Captures>, body: String) -> Response {
    let value: Value = serde_json::from_str(&body).expect("json");
    captures.0.lock().expect("captures").push(value);
    let stream = "data: {\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"Hi\"},\"finish_reason\":null}]}\n\n\
data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\" there\"},\"finish_reason\":null}]}\n\n\
data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":10,\"prompt_tokens_details\":{\"cached_tokens\":8},\"completion_tokens\":3}}\n\n\
data: [DONE]\n\n";
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "text/event-stream")
        .body(Body::from(stream))
        .expect("response")
}

async fn spawn_openai_upstream(captures: Captures) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let address = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new()
                .route("/chat/completions", post(openai_upstream))
                .with_state(captures),
        )
        .await
        .expect("server");
    });
    format!("http://{address}")
}

fn openai_config(base_url: String) -> OpenAIBackendConfig {
    OpenAIBackendConfig {
        name: None,
        model_prefix: None,
        base_url,
        api_key: "sk-test".into(),
        default_model: None,
        model_context_windows: std::collections::HashMap::new(),
        thinking: None,
        reasoning_efforts: Vec::new(),
        default_reasoning_effort: None,
        wire_format: codex_proxy::WireFormat::ChatCompletions,
        compat: codex_proxy::CompatFlags::default(),
        cache_policy: codex_proxy::CachePolicy::Implicit,
        cost: None,
        web_search: false,
        extra_headers: std::collections::HashMap::new(),
    }
}

async fn collect_body(response: axum::response::Response) -> String {
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("body")
        .to_bytes();
    String::from_utf8_lossy(&bytes).into_owned()
}

#[tokio::test]
async fn openai_backend_transforms_request_and_bridges_text_sse() {
    let captures = Captures::default();
    let base_url = spawn_openai_upstream(captures.clone()).await;
    let app = build_router(
        ProxyConfig::for_test(
            "http://127.0.0.1/invalid", // unused in openai mode
            "local-secret",
        )
        .with_openai_backend(openai_config(base_url)),
        Arc::new(StubCredentials::default()),
    )
    .expect("router");

    let response = tower::ServiceExt::oneshot(
        app,
        axum::http::Request::post("/v1/responses")
            .header("authorization", "Bearer local-secret")
            .header("content-type", "application/json")
            .header("x-grok-session-id", "grok-session-42")
            .body(Body::from(
                json!({
                    "model": "deepseek-chat",
                    "instructions": "You are a helpful assistant.",
                    "input": [
                        {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "hello"}]}
                    ],
                    "tools": [
                        {"type": "function", "name": "get_weather", "description": "weather", "parameters": {"type": "object"}},
                        {"type": "x_search"}
                    ]
                })
                .to_string(),
            ))
            .expect("request"),
    )
    .await
    .expect("response");

    assert_eq!(response.status(), StatusCode::OK);
    let body = collect_body(response).await;
    assert!(body.contains("\"type\":\"response.output_text.delta\""));
    assert!(body.contains("\"delta\":\"Hi\""));
    assert!(body.contains("\"delta\":\" there\""));
    assert!(body.contains("\"type\":\"response.completed\""));
    assert!(body.contains("Hi there"), "terminal output should accumulate deltas");

    let captured = captures.0.lock().expect("captures");
    assert_eq!(captured.len(), 1);
    let request = &captured[0];
    assert_eq!(request["model"], "deepseek-chat");
    assert_eq!(request["stream"], true);
    assert!(request.get("prompt_cache_key").is_none(), "openai mode must not emit an explicit cache key");
    assert_eq!(
        request["messages"],
        json!([
            {"role": "system", "content": "You are a helpful assistant."},
            {"role": "user", "content": "hello"}
        ])
    );
    assert_eq!(
        request["tools"],
        json!([
            {"type": "function", "function": {
                "name": "get_weather",
                "description": "weather",
                "parameters": {"type": "object"}
            }}
        ]),
        "x_search tool must be dropped in openai mode"
    );
}

#[tokio::test]
async fn openai_backend_bridges_function_calls_into_responses_output() {
    let captures = Captures::default();
    let base_url = spawn_openai_upstream(captures.clone()).await;
    let app = build_router(
        ProxyConfig::for_test("http://127.0.0.1/invalid", "local-secret")
            .with_openai_backend(openai_config(base_url)),
        Arc::new(StubCredentials::default()),
    )
    .expect("router");

    let response = tower::ServiceExt::oneshot(
        app,
        axum::http::Request::post("/v1/responses")
            .header("authorization", "Bearer local-secret")
            .header("content-type", "application/json")
            .body(Body::from(
                json!({
                    "model": "deepseek-chat",
                    "input": [{"type": "message", "role": "user", "content": [{"type": "input_text", "text": "weather?"}]}]
                })
                .to_string(),
            ))
            .expect("request"),
    )
    .await
    .expect("response");

    assert_eq!(response.status(), StatusCode::OK);
    let body = collect_body(response).await;
    // The tool-call test upstream is the same text stream, so this just confirms
    // the text bridge path again; the tool-call bridge is covered by the
    // dedicated unit test in the module.
    assert!(body.contains("\"type\":\"response.output_text.delta\""));
}

#[tokio::test]
async fn openai_backend_default_model_is_used_when_request_omits_it() {
    let captures = Captures::default();
    let base_url = spawn_openai_upstream(captures.clone()).await;
    let mut config = openai_config(base_url);
    config.default_model = Some("deepseek-chat".into());
    let app = build_router(
        ProxyConfig::for_test("http://127.0.0.1/invalid", "local-secret")
            .with_openai_backend(config),
        Arc::new(StubCredentials::default()),
    )
    .expect("router");

    let response = tower::ServiceExt::oneshot(
        app,
        axum::http::Request::post("/v1/responses")
            .header("authorization", "Bearer local-secret")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"input":[]}"#))
            .expect("request"),
    )
    .await
    .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    let captured = captures.0.lock().expect("captures");
    assert_eq!(captured[0]["model"], "deepseek-chat");
}

#[tokio::test]
async fn openai_backend_routes_named_provider_by_model_prefix() {
    // Default provider (DeepSeek) + a named provider (opencode zen) routed by
    // the `opencode/` model prefix.
    let deepseek_captures = Captures::default();
    let deepseek_url = spawn_openai_upstream(deepseek_captures.clone()).await;
    let zen_captures = Captures::default();
    let zen_url = spawn_openai_upstream(zen_captures.clone()).await;

    let mut deepseek = openai_config(deepseek_url);
    deepseek.default_model = Some("deepseek-v4-flash".into());
    deepseek.model_context_windows = std::collections::HashMap::from([
        ("deepseek-v4-flash".to_string(), 1_048_576),
        ("deepseek-v4-pro".to_string(), 1_048_576),
    ]);
    let mut zen = openai_config(zen_url);
    zen.name = Some("opencode-zen".into());
    zen.model_prefix = Some("opencode/".into());
    zen.default_model = Some("opencode/deepseek-v4-flash".into());
    zen.model_context_windows = std::collections::HashMap::from([
        ("opencode/deepseek-v4-flash".to_string(), 1_048_576),
    ]);

    let app = build_router(
        ProxyConfig::for_test("http://127.0.0.1/invalid", "local-secret").with_openai_backends(
            OpenAIBackends {
                default: Some(deepseek),
                named: vec![zen],
            },
        ),
        Arc::new(StubCredentials::default()),
    )
    .expect("router");

    // A model with the `opencode/` prefix routes to the named provider, and the
    // prefix is stripped before it reaches the upstream.
    let response = tower::ServiceExt::oneshot(
        app.clone(),
        axum::http::Request::post("/v1/responses")
            .header("authorization", "Bearer local-secret")
            .header("content-type", "application/json")
            .body(Body::from(
                json!({
                    "model": "opencode/deepseek-v4-flash",
                    "input": [{"type": "message", "role": "user", "content": [{"type": "input_text", "text": "hi"}]}]
                })
                .to_string(),
            ))
            .expect("request"),
    )
    .await
    .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    drop(collect_body(response).await);
    {
        let zen_captured = zen_captures.0.lock().expect("captures");
        assert_eq!(zen_captured.len(), 1);
        assert_eq!(zen_captured[0]["model"], "deepseek-v4-flash");
        assert!(deepseek_captures.0.lock().expect("captures").is_empty());
    }

    // A plain model (no matching prefix) routes to the default provider.
    let response = tower::ServiceExt::oneshot(
        app,
        axum::http::Request::post("/v1/responses")
            .header("authorization", "Bearer local-secret")
            .header("content-type", "application/json")
            .body(Body::from(
                json!({
                    "model": "deepseek-v4-pro",
                    "input": [{"type": "message", "role": "user", "content": [{"type": "input_text", "text": "hi"}]}]
                })
                .to_string(),
            ))
            .expect("request"),
    )
    .await
    .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    drop(collect_body(response).await);
    let deepseek_captured = deepseek_captures.0.lock().expect("captures");
    assert_eq!(deepseek_captured.len(), 1);
    assert_eq!(deepseek_captured[0]["model"], "deepseek-v4-pro");
    assert_eq!(zen_captures.0.lock().expect("captures").len(), 1);
}

/// Mock DeepSeek Responses API upstream: serves `/responses`, captures the
/// request, and returns a Responses-format SSE stream with a `web_search_call`
/// output item and a terminal `response.completed` carrying usage.
async fn openai_responses_upstream(State(captures): State<Captures>, body: String) -> Response {
    let value: Value = serde_json::from_str(&body).expect("json");
    captures.0.lock().expect("captures").push(value);
    let stream = "data: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"type\":\"web_search_call\",\"id\":\"ws-1\",\"status\":\"in_progress\"}}\n\n\
data: {\"type\":\"response.web_search_call.searching\",\"item_id\":\"ws-1\"}\n\n\
data: {\"type\":\"response.output_text.delta\",\"item_id\":\"msg-1\",\"output_index\":1,\"content_index\":0,\"delta\":\"Answer\"}\n\n\
data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp-1\",\"object\":\"response\",\"created_at\":1,\"model\":\"deepseek-v4-flash\",\"status\":\"completed\",\"output\":[],\"usage\":{\"input_tokens\":10,\"input_tokens_details\":{\"cached_tokens\":8},\"output_tokens\":3}}}\n\n";
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "text/event-stream")
        .body(Body::from(stream))
        .expect("response")
}

async fn spawn_openai_responses_upstream(captures: Captures) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let address = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new()
                .route("/responses", post(openai_responses_upstream))
                .with_state(captures),
        )
        .await
        .expect("server");
    });
    format!("http://{address}")
}

#[tokio::test]
async fn openai_responses_mode_forwards_web_search_and_bridges_back() {
    let captures = Captures::default();
    let base_url = spawn_openai_responses_upstream(captures.clone()).await;
    let mut config = openai_config(base_url);
    config.wire_format = codex_proxy::WireFormat::Responses;
    config.web_search = true;
    config.default_model = Some("deepseek-v4-flash".into());
    let app = build_router(
        ProxyConfig::for_test("http://127.0.0.1/invalid", "local-secret")
            .with_openai_backend(config),
        Arc::new(StubCredentials::default()),
    )
    .expect("router");

    let response = tower::ServiceExt::oneshot(
        app,
        axum::http::Request::post("/v1/responses")
            .header("authorization", "Bearer local-secret")
            .header("content-type", "application/json")
            .body(Body::from(
                json!({
                    "model": "deepseek-v4-flash",
                    "input": [{"type": "message", "role": "user", "content": [{"type": "input_text", "text": "search"}]}],
                    "tools": [{"type": "x_search"}]
                })
                .to_string(),
            ))
            .expect("request"),
    )
    .await
    .expect("response");

    assert_eq!(response.status(), StatusCode::OK);
    let body = collect_body(response).await;
    // The web_search_call item must be bridged back with its required `action`
    // (the CodexSseTransformer fills the placeholder for the in-progress item).
    assert!(body.contains("\"type\":\"web_search_call\""), "web_search_call must be bridged");
    assert!(body.contains("\"action\""), "web_search_call must carry an action for Grok's decoder");
    assert!(body.contains("\"delta\":\"Answer\""));
    assert!(body.contains("\"type\":\"response.completed\""));
    assert!(body.contains("\"input_tokens\":10"), "terminal usage should surface");

    let captured = captures.0.lock().expect("captures");
    assert_eq!(captured.len(), 1);
    let request = &captured[0];
    // Must hit the Responses endpoint with a Responses-format body, not a
    // Chat Completions translation.
    assert_eq!(request["model"], "deepseek-v4-flash");
    assert_eq!(request["stream"], true);
    assert!(request.get("prompt_cache_key").is_none());
    let tools = request["tools"].as_array().expect("tools");
    assert_eq!(tools[0], json!({"type": "web_search"}), "x_search should map to web_search");
}

#[tokio::test]
async fn openai_responses_mode_drops_web_search_when_disabled() {
    let captures = Captures::default();
    let base_url = spawn_openai_responses_upstream(captures.clone()).await;
    let mut config = openai_config(base_url);
    config.wire_format = codex_proxy::WireFormat::Responses;
    config.web_search = false;
    config.default_model = Some("deepseek-v4-flash".into());
    let app = build_router(
        ProxyConfig::for_test("http://127.0.0.1/invalid", "local-secret")
            .with_openai_backend(config),
        Arc::new(StubCredentials::default()),
    )
    .expect("router");

    let response = tower::ServiceExt::oneshot(
        app,
        axum::http::Request::post("/v1/responses")
            .header("authorization", "Bearer local-secret")
            .header("content-type", "application/json")
            .body(Body::from(
                json!({
                    "model": "deepseek-v4-flash",
                    "input": [{"type": "message", "role": "user", "content": "hi"}],
                    "tools": [{"type": "x_search"}]
                })
                .to_string(),
            ))
            .expect("request"),
    )
    .await
    .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    drop(collect_body(response).await);

    let captured = captures.0.lock().expect("captures");
    let tools = captured[0]["tools"].as_array().expect("tools");
    assert!(tools.is_empty(), "web_search must be dropped when disabled");
}

/// Mock upstream that returns two parallel `tool_calls` in a single chunk.
async fn openai_upstream_tool_calls(State(captures): State<Captures>, body: String) -> Response {
    let value: Value = serde_json::from_str(&body).expect("json");
    captures.0.lock().expect("captures").push(value);
    let stream = "data: {\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_1\",\"type\":\"function\",\"function\":{\"name\":\"read\",\"arguments\":\"{}\"}},{\"index\":1,\"id\":\"call_2\",\"type\":\"function\",\"function\":{\"name\":\"ls\",\"arguments\":\"{}\"}}]},\"finish_reason\":null}]}\n\n\
data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"tool_calls\"}],\"usage\":{\"prompt_tokens\":20,\"prompt_tokens_details\":{\"cached_tokens\":10},\"completion_tokens\":4}}\n\n\
data: [DONE]\n\n";
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "text/event-stream")
        .body(Body::from(stream))
        .expect("response")
}

/// Mock upstream that never reports usage (some Chat providers omit it).
async fn openai_upstream_no_usage(State(captures): State<Captures>, body: String) -> Response {
    let value: Value = serde_json::from_str(&body).expect("json");
    captures.0.lock().expect("captures").push(value);
    let stream = "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Hi\"},\"finish_reason\":null}]}\n\n\
data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n\
data: [DONE]\n\n";
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "text/event-stream")
        .body(Body::from(stream))
        .expect("response")
}

async fn spawn_openai_upstream_with(
    captures: Captures,
    route: axum::routing::MethodRouter<Captures>,
) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let address = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new().route("/chat/completions", route).with_state(captures),
        )
        .await
        .expect("server");
    });
    format!("http://{address}")
}

#[tokio::test]
async fn openai_backend_bridges_parallel_tool_calls_into_responses_output() {
    let captures = Captures::default();
    let base_url =
        spawn_openai_upstream_with(captures.clone(), post(openai_upstream_tool_calls)).await;
    let app = build_router(
        ProxyConfig::for_test("http://127.0.0.1/invalid", "local-secret")
            .with_openai_backend(openai_config(base_url)),
        Arc::new(StubCredentials::default()),
    )
    .expect("router");

    let response = tower::ServiceExt::oneshot(
        app,
        axum::http::Request::post("/v1/responses")
            .header("authorization", "Bearer local-secret")
            .header("content-type", "application/json")
            .body(Body::from(
                json!({
                    "model": "deepseek-chat",
                    "input": [{"type": "message", "role": "user", "content": [{"type": "input_text", "text": "list and read"}]}]
                })
                .to_string(),
            ))
            .expect("request"),
    )
    .await
    .expect("response");

    assert_eq!(response.status(), StatusCode::OK);
    let body = collect_body(response).await;
    assert_eq!(
        body.matches("\"type\":\"response.output_item.added\"").count(),
        2,
        "both parallel tool calls must be bridged as added items"
    );
    assert!(body.contains("\"name\":\"read\""));
    assert!(body.contains("\"name\":\"ls\""));
    assert_eq!(
        body.matches("\"type\":\"response.function_call_arguments.done\"").count(),
        2,
        "each tool call must complete its arguments"
    );
    assert!(body.contains("\"type\":\"response.completed\""));
}

#[tokio::test]
async fn openai_backend_forwards_max_output_tokens_as_max_tokens() {
    let captures = Captures::default();
    let base_url = spawn_openai_upstream(captures.clone()).await;
    let app = build_router(
        ProxyConfig::for_test("http://127.0.0.1/invalid", "local-secret")
            .with_openai_backend(openai_config(base_url)),
        Arc::new(StubCredentials::default()),
    )
    .expect("router");

    let response = tower::ServiceExt::oneshot(
        app,
        axum::http::Request::post("/v1/responses")
            .header("authorization", "Bearer local-secret")
            .header("content-type", "application/json")
            .body(Body::from(
                json!({
                    "model": "deepseek-chat",
                    "max_output_tokens": 2048,
                    "input": [{"type": "message", "role": "user", "content": "hi"}]
                })
                .to_string(),
            ))
            .expect("request"),
    )
    .await
    .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    drop(collect_body(response).await);

    let captured = captures.0.lock().expect("captures");
    assert_eq!(captured[0]["max_tokens"], 2048);
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
async fn openai_backend_logs_usage_estimated_when_upstream_omits_usage() {
    let captures = Captures::default();
    let base_url = spawn_openai_upstream_with(captures.clone(), post(openai_upstream_no_usage)).await;
    let app = build_router(
        ProxyConfig::for_test("http://127.0.0.1/invalid", "local-secret")
            .with_openai_backend(openai_config(base_url)),
        Arc::new(StubCredentials::default()),
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
            .body(Body::from(json!({"model": "deepseek-chat", "input": []}).to_string()))
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
    assert!(
        body.windows(b"response.completed".len())
            .any(|window| window == b"response.completed")
    );

    let logs = logs.contents();
    assert!(logs.contains("upstream stream omitted usage"));
    assert!(logs.contains("usage_estimated=true"));
    assert!(logs.contains("estimated_cost_usd=\"<none>\""));
}

#[tokio::test]
async fn health_reports_openai_providers_with_cost() {
    let captures = Captures::default();
    let base_url = spawn_openai_upstream(captures.clone()).await;
    let mut config = openai_config(base_url);
    config.cost = Some(codex_proxy::Cost {
        input: 0.27,
        output: 1.10,
        cache_read: 0.07,
        cache_write: 0.27,
    });
    let app = build_router(
        ProxyConfig::for_test("http://127.0.0.1/invalid", "local-secret")
            .with_openai_backend(config),
        Arc::new(StubCredentials::default()),
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
        .expect("body")
        .to_bytes();
    let health: Value = serde_json::from_slice(&body).expect("health JSON");
    assert_eq!(health["backend"], "openai");
    let providers = health["providers"].as_array().expect("providers array");
    assert_eq!(providers.len(), 1);
    assert_eq!(providers[0]["name"], "<default>");
    assert_eq!(providers[0]["wire_format"], "chat-completions");
    assert_eq!(providers[0]["cache_policy"], "implicit");
    assert_eq!(providers[0]["cost"]["input"], 0.27);
    assert_eq!(providers[0]["cost"]["output"], 1.10);
}

async fn codex_upstream(State(captures): State<Captures>, body: String) -> Response {
    let value: Value = serde_json::from_str(&body).expect("json");
    captures.0.lock().expect("captures").push(value);
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "text/event-stream")
        .body(Body::from("data: {\"type\":\"response.completed\"}\n\n"))
        .expect("response")
}

async fn spawn_codex_upstream(captures: Captures) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let address = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new()
                .route("/responses", post(codex_upstream))
                .with_state(captures),
        )
        .await
        .expect("server");
    });
    format!("http://{address}/responses")
}

#[tokio::test]
async fn codex_route_prefix_serves_codex_backend_alongside_openai() {
    let codex_captures = Captures::default();
    let codex_url = spawn_codex_upstream(codex_captures.clone()).await;
    let openai_captures = Captures::default();
    let openai_url = spawn_openai_upstream(openai_captures.clone()).await;

    let mut deepseek = openai_config(openai_url.clone());
    deepseek.default_model = Some("deepseek-v4-flash".into());
    let mut zen = openai_config(openai_url.clone());
    zen.name = Some("opencode-zen".into());
    zen.model_prefix = Some("opencode/".into());
    zen.default_model = Some("opencode/deepseek-v4-flash".into());

    let app = build_router(
        ProxyConfig::for_test(codex_url, "local-secret").with_openai_backends(OpenAIBackends {
            default: Some(deepseek),
            named: vec![zen],
        }),
        Arc::new(StubCredentials::default()),
    )
    .expect("router");

    // A Codex-server (account) model routes to the built-in Codex backend with
    // the reserved `codex/` prefix stripped before going upstream.
    let _ = tower::ServiceExt::oneshot(
        app.clone(),
        axum::http::Request::post("/v1/responses")
            .header("authorization", "Bearer local-secret")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"model":"codex/gpt-5.6-sol","input":[]}"#))
            .expect("request"),
    )
    .await
    .expect("response");

    let codex = codex_captures.0.lock().expect("codex captures");
    assert_eq!(
        codex.len(),
        1,
        "codex-prefixed model must reach the Codex backend"
    );
    assert_eq!(
        codex[0]["model"], "gpt-5.6-sol",
        "codex route prefix must be stripped before upstream"
    );
    assert!(
        openai_captures.0.lock().expect("openai captures").is_empty(),
        "codex-prefixed model must not reach an OpenAI provider"
    );
    drop(codex);

    // An un-prefixed model (e.g. the default DeepSeek provider) still routes to
    // OpenAI even while the account Codex backend is available.
    let _ = tower::ServiceExt::oneshot(
        app.clone(),
        axum::http::Request::post("/v1/responses")
            .header("authorization", "Bearer local-secret")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"model":"deepseek-v4-flash","input":[]}"#))
            .expect("request"),
    )
    .await
    .expect("response");

    let openai = openai_captures.0.lock().expect("openai captures");
    assert!(
        !openai.is_empty(),
        "un-prefixed model must route to an OpenAI provider"
    );
    assert_eq!(
        codex_captures.0.lock().expect("codex captures").len(),
        1,
        "the Codex backend must not receive un-prefixed OpenAI models"
    );
}
