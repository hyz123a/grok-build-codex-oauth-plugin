use codex_protocol::{
    CodexSseTransformer, TransformOptions, build_upstream_headers, transform_responses_request,
};
use http::{HeaderMap, HeaderValue, header::AUTHORIZATION};
use serde_json::json;

#[test]
fn transform_enforces_stateless_streaming_without_losing_grok_fields() {
    let request = json!({
        "model": "gpt-5.4",
        "store": true,
        "stream": false,
        "include": ["file_search_call.results"],
        "max_output_tokens": 4096,
        "max_completion_tokens": 2048,
        "temperature": 0.3,
        "input": [{"type": "message", "role": "user", "content": "hello"}],
        "tools": [{"type": "function", "name": "read_file"}],
        "tool_choice": "auto",
        "prompt_cache_key": "cache-1",
        "reasoning": {"effort": "extreme", "summary": "auto"}
    });

    let transformed =
        transform_responses_request(request, &TransformOptions::default()).expect("transform");

    assert_eq!(transformed["store"], false);
    assert_eq!(transformed["stream"], true);
    assert_eq!(
        transformed["include"],
        json!(["file_search_call.results", "reasoning.encrypted_content"])
    );
    assert_eq!(transformed["reasoning"]["effort"], "high");
    assert!(transformed.get("max_output_tokens").is_none());
    assert!(transformed.get("max_completion_tokens").is_none());
    assert!(transformed.get("temperature").is_none());
    assert_eq!(transformed["input"][0]["content"], "hello");
    assert_eq!(transformed["tools"][0]["name"], "read_file");
    assert_eq!(transformed["tool_choice"], "auto");
    assert_eq!(transformed["prompt_cache_key"], "cache-1");
}

#[test]
fn transform_preserves_function_call_pairing_and_deduplicates_include() {
    let request = json!({
        "include": ["reasoning.encrypted_content"],
        "input": [
            {"type": "function_call", "call_id": "call-1", "name": "shell", "arguments": "{}"},
            {"type": "function_call_output", "call_id": "call-1", "output": "ok"}
        ]
    });

    let transformed = transform_responses_request(request.clone(), &TransformOptions::default())
        .expect("transform");

    assert_eq!(
        transformed["include"],
        json!(["reasoning.encrypted_content"])
    );
    assert_eq!(transformed["input"], request["input"]);
}

#[test]
fn transform_preserves_xhigh_and_max_reasoning_efforts() {
    for effort in ["xhigh", "max"] {
        let transformed = transform_responses_request(
            json!({"reasoning": {"effort": effort}}),
            &TransformOptions::default(),
        )
        .expect("transform");
        assert_eq!(transformed["reasoning"]["effort"], effort);
    }
}

#[test]
fn transform_moves_system_messages_to_top_level_instructions() {
    let request = json!({
        "instructions": "Existing bridge instruction.",
        "input": [
            {
                "type": "message",
                "role": "system",
                "content": [
                    {"type": "input_text", "text": "Follow the project rules."}
                ]
            },
            {
                "type": "message",
                "role": "user",
                "content": "hello"
            }
        ]
    });

    let transformed =
        transform_responses_request(request, &TransformOptions::default()).expect("transform");

    assert_eq!(
        transformed["instructions"],
        "Existing bridge instruction.\n\nFollow the project rules."
    );
    assert_eq!(transformed["input"].as_array().map(Vec::len), Some(1));
    assert_eq!(transformed["input"][0]["role"], "user");
}

#[test]
fn upstream_headers_replace_local_authorization_and_sensitive_headers() {
    let mut incoming = HeaderMap::new();
    incoming.insert(
        AUTHORIZATION,
        HeaderValue::from_static("Bearer local-proxy"),
    );
    incoming.insert("cookie", HeaderValue::from_static("private=value"));
    incoming.insert("x-request-id", HeaderValue::from_static("request-1"));

    let headers =
        build_upstream_headers(&incoming, "oauth-token", "account-1").expect("valid headers");

    assert_eq!(headers[AUTHORIZATION], "Bearer oauth-token");
    assert_eq!(headers["chatgpt-account-id"], "account-1");
    assert_eq!(headers["originator"], "grok-build");
    assert_eq!(headers["accept"], "text/event-stream");
    assert_eq!(headers["x-request-id"], "request-1");
    assert!(!headers.contains_key("cookie"));
}

#[test]
fn sse_bridge_injects_streamed_message_into_empty_completed_output() {
    let mut bridge = CodexSseTransformer::default();
    let first = br#"data: {"type":"response.output_item.added","output_index":0,"item":{"type":"message","id":"msg-1","role":"assistant","status":"in_progress","content":[]}}

data: {"type":"response.output_text.delta","output_index":0,"content_index":0,"item_id":"msg-1","delta":"hello"}

"#;
    let terminal = br#"data: {"type":"response.completed","response":{"id":"resp-1","status":"completed","output":[]}}

data: [DONE]

"#;

    let mut output = bridge.push(&first[..37]);
    output.extend(bridge.push(&first[37..]));
    output.extend(bridge.push(terminal));
    output.extend(bridge.finish());
    let output = String::from_utf8(output.concat()).expect("utf8 SSE");

    assert_eq!(output.matches("\"delta\":\"hello\"").count(), 1);
    assert!(output.contains(
        r#""output":[{"content":[{"annotations":[],"text":"hello","type":"output_text"}],"id":"msg-1","role":"assistant","status":"completed","type":"message"}]"#
    ));
}

#[test]
fn sse_bridge_injects_streamed_function_call_into_completed_output() {
    let mut bridge = CodexSseTransformer::default();
    let stream = br#"data: {"type":"response.output_item.added","output_index":0,"item":{"type":"function_call","id":"fc-1","call_id":"call-1","name":"read_file","arguments":"","status":"in_progress"}}

data: {"type":"response.function_call_arguments.delta","output_index":0,"delta":"{\"path\":\"Cargo.toml\"}"}

data: {"type":"response.completed","response":{"id":"resp-1","status":"completed","output":[]}}

"#;

    let mut output = bridge.push(stream);
    output.extend(bridge.finish());
    let output = String::from_utf8(output.concat()).expect("utf8 SSE");

    assert!(output.contains(r#""call_id":"call-1""#));
    assert!(output.contains(r#""name":"read_file""#));
    assert!(output.contains(r#""arguments":"{\"path\":\"Cargo.toml\"}""#));
}

#[test]
fn sse_bridge_drops_json_keepalive_without_dropping_adjacent_response_events() {
    let mut bridge = CodexSseTransformer::default();
    let stream = br#"data: {"type":"response.created","response":{"id":"resp-1"}}

data: {"type":"keepalive"}

data: {"type":"response.future_event","value":"preserve-me"}

data: {"type":"response.in_progress","response":{"id":"resp-1"}}

"#;

    let mut output = bridge.push(&stream[..61]);
    output.extend(bridge.push(&stream[61..]));
    output.extend(bridge.finish());
    let output = String::from_utf8(output.concat()).expect("utf8 SSE");

    assert!(output.contains(r#""type":"response.created""#));
    assert!(output.contains(r#""type":"response.in_progress""#));
    assert!(output.contains(r#""type":"response.future_event""#));
    assert!(!output.contains(r#""type":"keepalive""#));
    assert_eq!(bridge.take_filtered_keepalive_events(), 1);
    assert_eq!(bridge.take_filtered_keepalive_events(), 0);
}
