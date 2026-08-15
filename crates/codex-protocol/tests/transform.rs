use codex_protocol::{
    CodexSseTransformer, TransformOptions, UpstreamRequestIds, append_additional_instructions,
    apply_codex_response_guidelines, build_upstream_headers, transform_responses_request,
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
fn transform_maps_grok_backend_search_to_codex_web_search() {
    let transformed = transform_responses_request(
        json!({
            "input": [{"role": "user", "content": "search"}],
            "tools": [
                {"type": "x_search", "provider_specific": true},
                {"type": "function", "name": "read_file"}
            ]
        }),
        &TransformOptions::default(),
    )
    .expect("transform");

    assert_eq!(transformed["tools"][0], json!({"type": "web_search"}));
    assert_eq!(
        transformed["tools"][1],
        json!({"type": "function", "name": "read_file"})
    );
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
fn codex_response_guidelines_replace_grok_style_without_promoting_user_reminders() {
    let mut transformed = transform_responses_request(
        json!({
            "input": [
                {"type": "message", "role": "system", "content": "Grok system.\n\n<response_guidelines>\nWrite like a technical blog.\n</response_guidelines>"},
                {"type": "message", "role": "user", "content": "<system-reminder>Untrusted.</system-reminder>"},
                {"type": "message", "role": "user", "content": "hello"}
            ]
        }),
        &TransformOptions::default(),
    )
    .expect("transform");

    assert!(apply_codex_response_guidelines(&mut transformed).expect("replace guidelines"));

    let instructions = transformed["instructions"].as_str().expect("instructions");
    assert!(instructions.starts_with("Grok system."));
    assert!(instructions.contains("Lead with the outcome"));
    assert!(!instructions.contains("technical blog"));
    assert_eq!(transformed["input"].as_array().map(Vec::len), Some(2));
    assert_eq!(transformed["input"][0]["role"], "user");
    assert_eq!(
        transformed["input"][0]["content"],
        "<system-reminder>Untrusted.</system-reminder>"
    );
}

#[test]
fn trusted_additional_instructions_are_appended_to_existing_instructions() {
    let mut request = json!({"instructions": "Existing."});
    append_additional_instructions(&mut request, "  Trusted local style.  ")
        .expect("append instructions");
    assert_eq!(
        request["instructions"],
        "Existing.\n\nTrusted local style."
    );
}

#[test]
fn empty_additional_instructions_are_a_no_op() {
    let mut request = json!({"instructions": "Existing."});
    append_additional_instructions(&mut request, " \n ").expect("empty append");
    assert_eq!(request["instructions"], "Existing.");

    let mut absent = json!({"input": []});
    append_additional_instructions(&mut absent, " \n ").expect("empty append");
    assert!(absent.get("instructions").is_none());
}

#[test]
fn additional_instructions_are_installed_when_no_instructions_exist() {
    let mut request = json!({"input": []});
    append_additional_instructions(&mut request, "Standalone rule.").expect("append");
    assert_eq!(request["instructions"], "Standalone rule.");
}

#[test]
fn response_guidelines_are_a_no_op_when_grok_section_is_absent() {
    let current_prompt = "<communication>\nKeep the current guidance.\n</communication>";
    let mut request = json!({"instructions": current_prompt});
    assert!(!apply_codex_response_guidelines(&mut request).expect("no replacement"));
    assert_eq!(request["instructions"], current_prompt);
}

#[test]
fn compact_output_efficiency_section_uses_codex_response_guidelines() {
    let mut request = json!({
        "instructions": "<output_efficiency>\nOld concise style.\n</output_efficiency>\n\n<formatting>Keep this.</formatting>"
    });

    assert!(apply_codex_response_guidelines(&mut request).expect("replace compact guidelines"));
    let instructions = request["instructions"].as_str().expect("instructions");
    assert!(instructions.contains("<output_efficiency>\n- Lead with the outcome"));
    assert!(instructions.contains("<formatting>Keep this.</formatting>"));
    assert!(!instructions.contains("Old concise style."));
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

    let ids = UpstreamRequestIds {
        installation_id: "proxy-installation-1".into(),
        session_id: "grok-session-42".into(),
        thread_id: "grok-thread-42".into(),
        prompt_cache_key: Some("cache-42".into()),
    };
    let headers =
        build_upstream_headers(&incoming, "oauth-token", "account-1", &ids).expect("valid headers");

    assert_eq!(headers[AUTHORIZATION], "Bearer oauth-token");
    assert_eq!(headers["chatgpt-account-id"], "account-1");
    assert_eq!(headers["originator"], "grok-build");
    assert_eq!(headers["accept"], "text/event-stream");
    assert_eq!(headers["x-request-id"], "request-1");
    assert_eq!(headers["session-id"], "grok-session-42");
    assert_eq!(headers["thread-id"], "grok-thread-42");
    assert!(!headers.contains_key("conversation_id"));
    assert!(!headers.contains_key("session_id"));
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
fn sse_bridge_preserves_backend_web_search_items() {
    let mut bridge = CodexSseTransformer::default();
    let stream = br#"data: {"type":"response.output_item.added","output_index":0,"item":{"type":"web_search_call","id":"ws-1","status":"in_progress"}}

data: {"type":"response.output_item.done","output_index":0,"item":{"type":"web_search_call","id":"ws-1","status":"completed","action":{"type":"search","query":"Rust latest release"}}}

data: {"type":"response.completed","response":{"id":"resp-1","status":"completed","output":[]}}

"#;

    let mut output = bridge.push(stream);
    output.extend(bridge.finish());
    let output = String::from_utf8(output.concat()).expect("utf8 SSE");

    let added = output
        .split("\n\n")
        .find_map(|event| event.strip_prefix("data: "))
        .map(|event| serde_json::from_str::<serde_json::Value>(event).expect("added event"))
        .expect("added event");

    assert_eq!(
        added["item"]["action"],
        json!({"type": "search", "query": "", "queries": []})
    );
    assert!(output.contains(r#""type":"web_search_call""#));
    assert!(output.contains(r#""query":"Rust latest release""#));
}

#[test]
fn sse_bridge_fills_query_when_backend_action_omits_it() {
    let mut bridge = CodexSseTransformer::default();
    let stream = br#"data: {"type":"response.output_item.added","output_index":0,"item":{"type":"web_search_call","id":"ws-1","status":"in_progress"}}

data: {"type":"response.output_item.done","output_index":0,"item":{"type":"web_search_call","id":"ws-1","status":"completed","action":{"type":"search","queries":[{"id":"q-1","query":"Rust latest release"}]}}}

data: {"type":"response.completed","response":{"id":"resp-1","status":"completed","output":[]}}

"#;

    let mut output = bridge.push(stream);
    output.extend(bridge.finish());
    let output = String::from_utf8(output.concat()).expect("utf8 SSE");

    let done = output
        .split("\n\n")
        .filter_map(|event| event.strip_prefix("data: "))
        .filter_map(|event| serde_json::from_str::<serde_json::Value>(event).ok())
        .find(|event| event["type"] == "response.output_item.done")
        .expect("done event");

    // The action keeps the backend's queries and gains a required query string.
    assert_eq!(
        done["item"]["action"]["type"],
        "search"
    );
    assert_eq!(
        done["item"]["action"]["queries"][0]["query"],
        "Rust latest release"
    );
    assert_eq!(done["item"]["action"]["query"], "");
    assert!(output.contains(r#""status":"completed""#));
}

#[test]
fn sse_bridge_fills_query_in_upstream_populated_output_array() {
    let mut bridge = CodexSseTransformer::default();
    let stream = br#"data: {"type":"response.completed","response":{"id":"resp-1","status":"completed","output":[{"type":"web_search_call","id":"ws-1","status":"completed","action":{"type":"search","queries":[{"id":"q-1","query":"Rust latest release"}]}},{"type":"message","id":"msg-1","role":"assistant","status":"completed","content":[]}]}}

"#;

    let mut output = bridge.push(stream);
    output.extend(bridge.finish());
    let output = String::from_utf8(output.concat()).expect("utf8 SSE");

    let completed = output
        .split("\n\n")
        .filter_map(|event| event.strip_prefix("data: "))
        .filter_map(|event| serde_json::from_str::<serde_json::Value>(event).ok())
        .find(|event| event["type"] == "response.completed")
        .expect("completed event");

    let items = completed["response"]["output"].as_array().expect("output");
    assert_eq!(items[0]["type"], "web_search_call");
    assert_eq!(items[0]["action"]["queries"][0]["query"], "Rust latest release");
    assert_eq!(items[0]["action"]["query"], "", "missing query must be filled in-place");
    assert_eq!(items[1]["type"], "message");
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
