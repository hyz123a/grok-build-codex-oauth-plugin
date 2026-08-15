//! OpenAI Chat `/chat/completions` compatible backend.
//!
//! Translates Grok's Responses-format request into an OpenAI Chat request and
//! bridges the provider's SSE stream back into the Responses-format SSE events
//! that Grok's decoder accepts. Caching is the provider's implicit prefix
//! caching; no explicit `prompt_cache_key` is emitted. Web search (`x_search`)
//! is not available on this backend and is dropped from the request.

use std::collections::HashMap;

use axum::{
    body::{Body, Bytes},
    http::{HeaderMap, HeaderValue, StatusCode, header::CONTENT_TYPE},
    response::Response,
};
use codex_protocol::CodexSseTransformer;
use futures_util::StreamExt;
use serde_json::{Map, Value, json};

use crate::{
    CompletionMetrics, RequestDiagnostics, copy_response_headers, error_response,
    extract_cache_usage, extract_sse_error_type, header_value, normalize_upstream_error_status,
    sanitize_error_body,
};

use super::super::config::OpenAIBackendConfig;

pub(crate) async fn handle_openai(
    state: &crate::AppState,
    headers: &HeaderMap,
    request: &Value,
    client_request_id: &str,
) -> Response {
    let started_at = std::time::Instant::now();
    let config = state.config.openai_backend.as_ref();
    let Some(config) = config else {
        return error_response(StatusCode::INTERNAL_SERVER_ERROR, "openai backend not configured");
    };

    let chat_request = match transform_to_chat_completions(request, config) {
        Ok(chat_request) => chat_request,
        Err(message) => return error_response(StatusCode::BAD_REQUEST, &message),
    };
    let diagnostics = RequestDiagnostics::new_openai(started_at, headers, &chat_request);
    diagnostics.log_received();

    let model = chat_request
        .get("model")
        .and_then(Value::as_str)
        .unwrap_or("<missing>");
    let base_url = config.base_url.trim_end_matches('/');
    let url = format!("{base_url}/chat/completions");

    let mut upstream_headers = build_openai_headers(config, headers, client_request_id);
    upstream_headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    upstream_headers.insert(
        "accept",
        HeaderValue::from_static("text/event-stream"),
    );

    let body = match serde_json::to_vec(&chat_request) {
        Ok(body) => body,
        Err(_) => {
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to encode openai request",
            );
        }
    };

    let send = state
        .client
        .post(&url)
        .headers(upstream_headers)
        .body(body)
        .send();
    let upstream = match tokio::time::timeout(
        state.config.http_response_headers_timeout,
        send,
    )
    .await
    {
        Ok(Ok(response)) => response,
        Ok(Err(_)) => {
            diagnostics.log_request_failed(1);
            return error_response(StatusCode::BAD_GATEWAY, "openai upstream request failed");
        }
        Err(_) => {
            diagnostics.log_request_failed(1);
            return error_response(
                StatusCode::BAD_GATEWAY,
                "openai upstream response headers timed out",
            );
        }
    };
    diagnostics.log_response_headers(&upstream, 1);
    let source_headers = upstream.headers().clone();

    if !upstream.status().is_success() {
        let status = upstream.status();
        let body = upstream.bytes().await.unwrap_or_default();
        let response_status = normalize_upstream_error_status(status, &body);
        tracing::warn!(
            model,
            upstream_status = %status,
            upstream_error = %sanitize_error_body(&body),
            "OpenAI-compatible upstream rejected the request"
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
    let stream = bridge_openai_sse(upstream, diagnostics, upstream_request_id, model.to_owned());
    let mut response = Response::new(Body::from_stream(stream));
    *response.status_mut() = StatusCode::OK;
    copy_response_headers(&source_headers, &mut response);
    response.headers_mut().insert(
        "x-codex-upstream-transport",
        HeaderValue::from_static("http-sse"),
    );
    response
}

fn build_openai_headers(
    config: &OpenAIBackendConfig,
    incoming: &HeaderMap,
    client_request_id: &str,
) -> HeaderMap {
    let mut headers = HeaderMap::new();
    for name in ["x-request-id", "traceparent", "tracestate"] {
        if let Some(value) = incoming.get(name) {
            headers.insert(http::header::HeaderName::from_static(name), value.clone());
        }
    }
    if !headers.contains_key("x-request-id")
        && let Ok(value) = HeaderValue::from_str(client_request_id)
    {
        headers.insert("x-request-id", value);
    }
    if let Ok(value) = HeaderValue::from_str(&format!("Bearer {}", config.api_key)) {
        headers.insert("authorization", value);
    }
    for (name, value) in &config.extra_headers {
        if let Ok(name) = http::header::HeaderName::from_bytes(name.as_bytes())
            && let Ok(value) = HeaderValue::from_str(value)
        {
            headers.insert(name, value);
        }
    }
    headers
}

/// Translate a Grok Responses request into an OpenAI Chat completions request.
pub(super) fn transform_to_chat_completions(
    request: &Value,
    config: &OpenAIBackendConfig,
) -> Result<Value, String> {
    let object = request
        .as_object()
        .ok_or_else(|| "request body must be a JSON object".to_string())?;

    let requested = object
        .get("model")
        .and_then(Value::as_str)
        .filter(|model| !model.is_empty())
        .map(str::to_owned);
    // When we have a known model list (synced from `/models`), prefer the
    // requested model only if it's known; otherwise fall back to the configured
    // default so Grok's internal models (e.g. `grok-4.5`) map onto the provider
    // instead of being rejected upstream. Without a known list, use the request.
    let known_models = !config.model_context_windows.is_empty();
    let model = if known_models {
        requested
            .as_ref()
            .filter(|slug| config.model_context_windows.contains_key(*slug))
            .cloned()
            .or_else(|| config.default_model.clone())
            .ok_or_else(|| "missing `model`; set one in the request or configure `model`".to_string())?
    } else {
        requested
            .clone()
            .or_else(|| config.default_model.clone())
            .ok_or_else(|| "missing `model`; set one in the request or configure `model`".to_string())?
    };

    let mut messages = Vec::new();

    let instructions = object
        .get("instructions")
        .and_then(Value::as_str)
        .filter(|text| !text.trim().is_empty());
    if let Some(instructions) = instructions {
        messages.push(json!({ "role": "system", "content": instructions }));
    }

    let mut tools = Vec::new();
    if let Some(tool_list) = object.get("tools").and_then(Value::as_array) {
        for tool in tool_list {
            let Some(tool_object) = tool.as_object() else {
                continue;
            };
            if tool_object.get("type").and_then(Value::as_str) == Some("x_search") {
                continue;
            }
            let name = tool_object.get("name").and_then(Value::as_str);
            let Some(name) = name else {
                continue;
            };
            let mut function = Map::new();
            function.insert("name".into(), Value::String(name.to_owned()));
            if let Some(description) = tool_object.get("description").and_then(Value::as_str) {
                function.insert("description".into(), Value::String(description.to_owned()));
            }
            if let Some(parameters) = tool_object.get("parameters") {
                function.insert("parameters".into(), parameters.clone());
            }
            tools.push(json!({ "type": "function", "function": Value::Object(function) }));
        }
    }

    if let Some(input) = object.get("input").and_then(Value::as_array) {
        let mut pending_reasoning = String::new();
        // Consecutive `function_call` items form ONE assistant tool-call turn;
        // they must be merged so the resulting assistant `tool_calls` message is
        // followed by the corresponding `tool` results (DeepSeek rejects an
        // assistant tool_calls message without tool responses for each id).
        let mut pending_tool_calls: Vec<Value> = Vec::new();
        let flush_tool_calls = |messages: &mut Vec<Value>,
                                tool_calls: &mut Vec<Value>,
                                reasoning: &mut String| {
            if tool_calls.is_empty() {
                return;
            }
            let mut assistant = json!({ "role": "assistant", "tool_calls": std::mem::take(tool_calls) });
            if !reasoning.is_empty() {
                assistant["reasoning_content"] = Value::String(std::mem::take(reasoning));
            }
            messages.push(assistant);
        };
        for item in input {
            let kind = item.get("type").and_then(Value::as_str).unwrap_or_default();
            match kind {
                "reasoning" => {
                    if let Some(text) = extract_reasoning_text(item) {
                        pending_reasoning = text;
                    }
                }
                "function_call" => {
                    let id = item.get("call_id").and_then(Value::as_str);
                    let name = item.get("name").and_then(Value::as_str);
                    let arguments = item.get("arguments").and_then(Value::as_str).unwrap_or("");
                    if let (Some(id), Some(name)) = (id, name) {
                        pending_tool_calls.push(json!({
                            "id": id,
                            "type": "function",
                            "function": { "name": name, "arguments": arguments }
                        }));
                    }
                }
                _ => {
                    flush_tool_calls(&mut messages, &mut pending_tool_calls, &mut pending_reasoning);
                    let mut message = map_input_item(item);
                    if !pending_reasoning.is_empty()
                        && let Some(msg) = message.as_mut()
                        && msg.get("role").and_then(Value::as_str) == Some("assistant")
                        && let Some(object) = msg.as_object_mut()
                    {
                        object.insert(
                            "reasoning_content".into(),
                            Value::String(std::mem::take(&mut pending_reasoning)),
                        );
                    }
                    if let Some(message) = message {
                        messages.push(message);
                    }
                }
            }
        }
        flush_tool_calls(&mut messages, &mut pending_tool_calls, &mut pending_reasoning);
    }

    let mut chat = Map::new();
    chat.insert("model".into(), Value::String(model));
    chat.insert("messages".into(), Value::Array(messages));
    if !tools.is_empty() {
        chat.insert("tools".into(), Value::Array(tools));
    }
    chat.insert("stream".into(), Value::Bool(true));
    match config.thinking {
        Some(true) => {
            chat.insert("thinking".into(), json!({ "type": "enabled" }));
        }
        Some(false) => {
            chat.insert("thinking".into(), json!({ "type": "disabled" }));
        }
        None => {}
    }
    Ok(Value::Object(chat))
}

/// Map a single Grok Responses `input` item to an OpenAI Chat message.
fn map_input_item(item: &Value) -> Option<Value> {
    let kind = item.get("type").and_then(Value::as_str).unwrap_or_default();
    match kind {
        "message" => {
            let role = item.get("role").and_then(Value::as_str)?;
            if role != "user" && role != "assistant" {
                return None;
            }
            let content = item
                .get("content")
                .and_then(extract_text_content)
                .filter(|text| !text.trim().is_empty())?;
            Some(json!({ "role": role, "content": content }))
        }
        "function_call" => {
            let id = item.get("call_id").and_then(Value::as_str)?;
            let name = item.get("name").and_then(Value::as_str)?;
            let arguments = item.get("arguments").and_then(Value::as_str).unwrap_or("");
            Some(json!({
                "role": "assistant",
                "tool_calls": [{
                    "id": id,
                    "type": "function",
                    "function": { "name": name, "arguments": arguments }
                }]
            }))
        }
        "function_call_output" => {
            let tool_call_id = item.get("call_id").and_then(Value::as_str)?;
            let output = match item.get("output") {
                Some(Value::String(text)) => text.clone(),
                Some(value) => value.to_string(),
                None => "{}".to_string(),
            };
            Some(json!({
                "role": "tool",
                "tool_call_id": tool_call_id,
                "content": output
            }))
        }
        _ => None,
    }
}

/// Extract reasoning text from a Grok Responses `reasoning` input item, which
/// carries its content as `content` or `summary` (plaintext or `text` parts).
fn extract_reasoning_text(item: &Value) -> Option<String> {
    let content = item.get("content").or_else(|| item.get("summary"))?;
    let text = match content {
        Value::String(text) => text.clone(),
        Value::Array(parts) => parts
            .iter()
            .filter_map(|part| part.get("text").and_then(Value::as_str))
            .collect::<Vec<_>>()
            .join("\n"),
        _ => return None,
    };
    (!text.trim().is_empty()).then_some(text)
}

fn extract_text_content(content: &Value) -> Option<String> {
    match content {
        Value::String(text) => Some(text.clone()),
        Value::Array(parts) => {
            let text = parts
                .iter()
                .filter_map(|part| {
                    if part.get("type").and_then(Value::as_str) != Some("input_text")
                        && part.get("type").and_then(Value::as_str) != Some("output_text")
                        && !part.is_null()
                    {
                        part.as_str().map(str::to_owned)
                    } else {
                        part.get("text").and_then(Value::as_str).map(str::to_owned)
                    }
                })
                .collect::<Vec<_>>()
                .join("\n");
            (!text.is_empty()).then_some(text)
        }
        _ => None,
    }
}

/// Accumulate one OpenAI Chat stream chunk into Responses-format SSE events.
fn bridge_openai_sse(
    upstream: reqwest::Response,
    diagnostics: RequestDiagnostics,
    upstream_request_id: String,
    model: String,
) -> impl futures_util::Stream<Item = Result<Bytes, std::io::Error>> {
    let source = upstream.bytes_stream();
    async_stream::stream! {
        let mut source = std::pin::pin!(source);
        let mut transformer = CodexSseTransformer::default();
        let mut state = ChatStreamState::new(model.clone());
        let mut bytes_received = 0usize;
        let mut chunks_received = 0u64;
        let mut filtered_keepalive_events = 0u64;
        let mut first_event_logged = false;
        let mut sse_error_type: Option<String> = None;
        let mut first_bytes_logged = false;
        let mut input_tokens: Option<u64> = None;
        let mut cached_input_tokens: Option<u64> = None;

        while let Some(chunk) = source.next().await {
            let chunk = match chunk {
                Ok(chunk) => chunk,
                Err(error) => {
                    diagnostics_log_transport(&diagnostics, &upstream_request_id, &model);
                    diagnostics.log_completed("transport_error", &upstream_request_id, None, CompletionMetrics {
                        bytes_received, chunks_received, filtered_keepalive_events, input_tokens, cached_input_tokens,
                    });
                    yield Err(std::io::Error::other(error));
                    return;
                }
            };
            bytes_received = bytes_received.saturating_add(chunk.len());
            chunks_received = chunks_received.saturating_add(1);
            if !first_bytes_logged {
                first_bytes_logged = true;
                tracing::info!(
                    client_request_id = diagnostics.client_request_id.as_str(),
                    upstream_request_id = upstream_request_id.as_str(),
                    model = model.as_str(),
                    ttfb_ms = crate::elapsed_ms(diagnostics.started_at),
                    first_chunk_bytes = chunk.len(),
                    "OpenAI-compatible upstream first SSE bytes received"
                );
            }
            if let Some((input, cached)) = state.take_usage() {
                input_tokens = Some(input);
                cached_input_tokens = Some(cached);
            }
            let events = state.push(&chunk);
            if let Some((input, cached)) = state.take_usage() {
                input_tokens = Some(input);
                cached_input_tokens = Some(cached);
            }
            for event in events {
                if !first_event_logged {
                    first_event_logged = true;
                    tracing::info!(
                        client_request_id = diagnostics.client_request_id.as_str(),
                        model = model.as_str(),
                        ttfb_ms = crate::elapsed_ms(diagnostics.started_at),
                        transport = "openai-http-sse",
                        "OpenAI-compatible upstream first event received"
                    );
                }
                bytes_received = bytes_received.saturating_add(event.len());
                if let Some((input, cached)) = extract_cache_usage(&event) {
                    input_tokens = Some(input);
                    cached_input_tokens = Some(cached);
                }
                if sse_error_type.is_none()
                    && let Some(error_type) = extract_sse_error_type(&event)
                {
                    sse_error_type = Some(error_type);
                }
                let bridged = transformer.push(&event);
                filtered_keepalive_events = filtered_keepalive_events
                    .saturating_add(transformer.take_filtered_keepalive_events());
                for bridged_event in bridged {
                    yield Ok(Bytes::from(bridged_event));
                }
            }
        }
        for remainder in state.finish() {
            bytes_received = bytes_received.saturating_add(remainder.len());
            let bridged = transformer.push(&remainder);
            filtered_keepalive_events = filtered_keepalive_events
                .saturating_add(transformer.take_filtered_keepalive_events());
            for bridged_event in bridged {
                yield Ok(Bytes::from(bridged_event));
            }
        }
        for remainder in transformer.finish() {
            filtered_keepalive_events = filtered_keepalive_events
                .saturating_add(transformer.take_filtered_keepalive_events());
            yield Ok(Bytes::from(remainder));
        }
        let outcome = if sse_error_type.is_some() {
            "sse_error"
        } else {
            "completed"
        };
        diagnostics.log_completed(outcome, &upstream_request_id, sse_error_type.as_deref(), CompletionMetrics {
            bytes_received, chunks_received, filtered_keepalive_events, input_tokens, cached_input_tokens,
        });
    }
}

fn diagnostics_log_transport(diagnostics: &RequestDiagnostics, upstream_request_id: &str, model: &str) {
    tracing::warn!(
        client_request_id = diagnostics.client_request_id.as_str(),
        upstream_request_id,
        model,
        "OpenAI-compatible upstream SSE transport failed"
    );
}

/// Parses OpenAI Chat SSE chunks and emits Responses-format SSE events.
#[derive(Default)]
struct ChatStreamState {
    buffer: Vec<u8>,
    output_index: u64,
    tool_calls: HashMap<u64, AccumulatedToolCall>,
    pending_usage: Option<(u64, u64)>,
    sequence_number: u64,
    model: String,
    reasoning_content: String,
}

impl ChatStreamState {
    fn new(model: String) -> Self {
        Self {
            model,
            ..Self::default()
        }
    }
}

impl ChatStreamState {
    /// Attach a monotonically increasing `sequence_number` (required by Grok's
    /// Responses SSE decoder) and frame the event as SSE.
    fn emit(&mut self, mut value: Value) -> Vec<u8> {
        if let Some(object) = value.as_object_mut() {
            object.insert("sequence_number".into(), Value::from(self.sequence_number));
        }
        self.sequence_number = self.sequence_number.saturating_add(1);
        sse_event(value)
    }
}

#[derive(Default)]
struct AccumulatedToolCall {
    id: Option<String>,
    name: String,
    arguments: String,
    emitted_item: bool,
}

impl ChatStreamState {
    fn push(&mut self, bytes: &[u8]) -> Vec<Vec<u8>> {
        self.buffer.extend_from_slice(bytes);
        let mut events = Vec::new();
        while let Some((position, delimiter_length)) = find_sse_delimiter(&self.buffer) {
            let event = self
                .buffer
                .drain(..position + delimiter_length)
                .collect::<Vec<_>>();
            if let Some(text) = std::str::from_utf8(&event).ok()
                && let Some(data) = text.lines().find_map(|line| {
                    line.strip_prefix("data:")
                        .map(str::trim_start)
                        .filter(|data| !data.is_empty() && *data != "[DONE]")
                })
                && let Ok(chunk) = serde_json::from_str::<Value>(data)
            {
                events.extend(self.observe_chunk(&chunk));
            }
        }
        events
    }

    fn finish(&mut self) -> Vec<Vec<u8>> {
        let mut events = Vec::new();
        if !self.buffer.is_empty() {
            let event = std::mem::take(&mut self.buffer);
            events.extend(self.push(&event));
        }
        events.push(self.terminal_event());
        events
    }

    /// Take any cache usage reported by a trailing stream chunk.
    fn take_usage(&mut self) -> Option<(u64, u64)> {
        self.pending_usage.take()
    }

    fn terminal_event(&mut self) -> Vec<u8> {
        let (input_tokens, cached_tokens) = self.pending_usage.unwrap_or((0, 0));
        self.emit(json!({
            "type": "response.completed",
            "response": {
                "id": "resp_openai_bridge",
                "object": "response",
                "created_at": unix_timestamp_ms() as i64 / 1000,
                "model": self.model,
                "status": "completed",
                "output": [],
                "usage": {
                    "input_tokens": input_tokens,
                    "output_tokens": 0,
                    "total_tokens": input_tokens,
                    "input_tokens_details": { "cached_tokens": cached_tokens },
                    "output_tokens_details": { "reasoning_tokens": 0 }
                },
                "error": null,
                "incomplete_details": null,
                "instructions": null,
                "max_output_tokens": null,
                "metadata": {},
                "parallel_tool_calls": null,
                "previous_response_id": null,
                "reasoning": { "effort": null, "summary": null },
                "store": false,
                "temperature": null,
                "text": { "format": { "type": "text" }, "verbosity": null },
                "tool_choice": "auto",
                "tools": [],
                "top_p": null,
                "truncation": null,
                "user": null
            }
        }))
    }

    fn observe_chunk(&mut self, chunk: &Value) -> Vec<Vec<u8>> {
        let mut events = Vec::new();
        // OpenAI Chat-compatible streams report usage in a trailing chunk with an
        // empty `choices` array; surface it here so the bridge can record cache
        // accounting even when no output events are emitted for the chunk.
        if let Some((input, cached)) = extract_usage(chunk) {
            self.pending_usage = Some((input, cached));
        }
        let Some(choices) = chunk.get("choices").and_then(Value::as_array) else {
            return events;
        };
        for choice in choices {
            let Some(delta) = choice.get("delta") else {
                continue;
            };
            if let Some(reasoning) = delta.get("reasoning_content").and_then(Value::as_str)
                && !reasoning.is_empty()
            {
                self.reasoning_content.push_str(reasoning);
            }
            if let Some(content) = delta.get("content").and_then(Value::as_str)
                && !content.is_empty()
            {
                let output_index = self.output_index;
                events.push(self.emit(json!({
                    "type": "response.output_text.delta",
                    "item_id": "msg_openai_bridge",
                    "output_index": output_index,
                    "content_index": 0,
                    "delta": content
                })));
            }
            if let Some(tool_calls) = delta.get("tool_calls").and_then(Value::as_array) {
                for tool_call in tool_calls {
                    let index = tool_call.get("index").and_then(Value::as_u64).unwrap_or(0);
                    let accumulated = self.tool_calls.entry(index).or_default();
                    if let Some(id) = tool_call.get("id").and_then(Value::as_str) {
                        accumulated.id = Some(id.to_owned());
                    }
                    if let Some(function) = tool_call.get("function") {
                        if let Some(name) = function.get("name").and_then(Value::as_str) {
                            accumulated.name = name.to_owned();
                        }
                        if let Some(arguments) = function.get("arguments").and_then(Value::as_str) {
                            accumulated.arguments.push_str(arguments);
                        }
                    }
                }
            }
        }
        let finished = choices
            .iter()
            .any(|choice| choice.get("finish_reason").is_some_and(|reason| !reason.is_null()));
        if finished {
            events.extend(self.flush_tool_calls());
            events.push(self.terminal_event());
            self.output_index = self.output_index.saturating_add(1);
        }
        events
    }

    fn flush_tool_calls(&mut self) -> Vec<Vec<u8>> {
        let mut events = Vec::new();
        if !self.reasoning_content.is_empty() {
            let reasoning = std::mem::take(&mut self.reasoning_content);
            let output_index = self.output_index;
            self.output_index = self.output_index.saturating_add(1);
            events.push(self.emit(json!({
                "type": "response.output_item.added",
                "item_id": "rs_openai_bridge",
                "output_index": output_index,
                "item": {
                    "type": "reasoning",
                    "id": "rs_openai_bridge",
                    "summary": [],
                    "content": [{ "type": "reasoning_text", "text": reasoning, "annotations": [] }]
                }
            })));
        }
        let indices = self.tool_calls.keys().copied().collect::<Vec<_>>();
        for index in indices {
            let Some(mut tool_call) = self.tool_calls.remove(&index) else {
                continue;
            };
            let output_index = self.output_index;
            self.output_index = self.output_index.saturating_add(1);
            let id = tool_call.id.clone().unwrap_or_else(|| format!("call_{index}"));
            let item_id = format!("fc_{index}");
            if !tool_call.emitted_item {
                tool_call.emitted_item = true;
                events.push(self.emit(json!({
                    "type": "response.output_item.added",
                    "item_id": item_id,
                    "output_index": output_index,
                    "item": {
                        "type": "function_call",
                        "id": item_id,
                        "call_id": id,
                        "name": tool_call.name,
                        "arguments": ""
                    }
                })));
            }
            events.push(self.emit(json!({
                "type": "response.function_call_arguments.delta",
                "item_id": item_id,
                "output_index": output_index,
                "delta": tool_call.arguments
            })));
            events.push(self.emit(json!({
                "type": "response.function_call_arguments.done",
                "item_id": item_id,
                "output_index": output_index,
                "arguments": tool_call.arguments
            })));
            tool_call.arguments = String::new();
            self.tool_calls.insert(index, tool_call);
        }
        events
    }
}

fn sse_event(value: Value) -> Vec<u8> {
    format!("data: {value}\n\n").into_bytes()
}

fn unix_timestamp_ms() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

/// Read OpenAI Chat-compatible input/cached token counts from a stream chunk's
/// `usage` block (e.g. `prompt_tokens` + `prompt_tokens_details.cached_tokens`
/// as reported by DeepSeek).
fn extract_usage(chunk: &Value) -> Option<(u64, u64)> {
    let usage = chunk.get("usage").and_then(Value::as_object)?;
    let input_tokens = ["prompt_tokens", "input_tokens"]
        .iter()
        .find_map(|key| usage.get(*key).and_then(Value::as_u64))?;
    let cached_input_tokens = ["prompt_tokens_details", "input_tokens_details"]
        .iter()
        .find_map(|key| {
            usage
                .get(*key)
                .and_then(Value::as_object)
                .and_then(|details| details.get("cached_tokens"))
                .and_then(Value::as_u64)
        })
        .unwrap_or(0);
    Some((input_tokens, cached_input_tokens))
}

fn find_sse_delimiter(buffer: &[u8]) -> Option<(usize, usize)> {
    buffer
        .windows(2)
        .position(|window| window == b"\n\n")
        .map(|position| (position, 2))
        .or_else(|| {
            buffer
                .windows(4)
                .position(|window| window == b"\r\n\r\n")
                .map(|position| (position, 4))
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> OpenAIBackendConfig {
        OpenAIBackendConfig {
            base_url: "https://api.deepseek.com/v1".into(),
            api_key: "sk-test".into(),
            default_model: None,
            model_context_windows: HashMap::new(),
            thinking: None,
            reasoning_efforts: Vec::new(),
            default_reasoning_effort: None,
            extra_headers: HashMap::new(),
        }
    }

    #[test]
    fn transform_attaches_echoed_reasoning_content_to_assistant() {
        let request = json!({
            "model": "deepseek-v4-flash",
            "input": [
                {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "hi"}]},
                {"type": "reasoning", "content": [{"type": "reasoning_text", "text": "I should call the tool."}]},
                {"type": "function_call", "call_id": "c1", "name": "run_shell", "arguments": "{}"}
            ]
        });
        let chat = transform_to_chat_completions(&request, &config()).expect("transform");
        let assistant = chat["messages"]
            .as_array()
            .expect("messages")
            .iter()
            .find(|m| m.get("role").and_then(Value::as_str) == Some("assistant"))
            .expect("assistant message");
        assert_eq!(
            assistant.get("reasoning_content").and_then(Value::as_str),
            Some("I should call the tool.")
        );
    }

    #[test]
    fn transform_groups_consecutive_function_calls_into_one_assistant_message() {
        let request = json!({
            "model": "deepseek-v4-flash",
            "input": [
                {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "hi"}]},
                {"type": "function_call", "call_id": "c1", "name": "read", "arguments": "{}"},
                {"type": "function_call", "call_id": "c2", "name": "ls", "arguments": "{}"},
                {"type": "function_call_output", "call_id": "c1", "output": "a"},
                {"type": "function_call_output", "call_id": "c2", "output": "b"}
            ]
        });
        let chat = transform_to_chat_completions(&request, &config()).expect("transform");
        let msgs = chat["messages"].as_array().expect("messages");
        let assistant = msgs.iter().find(|m| m.get("role").and_then(Value::as_str) == Some("assistant")).expect("assistant");
        let tool_calls = assistant["tool_calls"].as_array().expect("tool_calls");
        assert_eq!(tool_calls.len(), 2);
        assert_eq!(tool_calls[0]["id"], "c1");
        assert_eq!(tool_calls[1]["id"], "c2");
        // Each tool_call_id has a following tool response.
        let tool_ids = msgs.iter().filter_map(|m| m.get("role").and_then(Value::as_str).filter(|r| *r == "tool").map(|_| m.get("tool_call_id").and_then(Value::as_str))).collect::<Vec<_>>();
        assert_eq!(tool_ids, vec![Some("c1"), Some("c2")]);
    }

    #[test]
    fn chat_stream_emits_reasoning_item() {
        let mut state = ChatStreamState::new("deepseek-v4-flash".into());
        state.push(
            br#"data: {"choices":[{"index":0,"delta":{"reasoning_content":"Think step by step"},"finish_reason":null}]}

"#,
        );
        state.push(
            br#"data: {"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call_1","function":{"name":"f","arguments":"{}"}}]},"finish_reason":null}]}

"#,
        );
        let mut events = Vec::new();
        events.extend(state.flush_tool_calls());
        let text = String::from_utf8(events.concat()).expect("utf-8");
        assert!(text.contains("\"type\":\"reasoning\""));
        assert!(text.contains("Think step by step"));
    }

    #[test]
    fn transforms_tools_and_messages() {
        let request = json!({
            "model": "deepseek-chat",
            "instructions": "be terse",
            "input": [
                {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "hi"}]},
                {"type": "message", "role": "assistant", "content": [{"type": "output_text", "text": "hello"}]},
                {"type": "function_call", "call_id": "c1", "name": "get_weather", "arguments": "{\"city\":\"SF\"}"},
                {"type": "function_call_output", "call_id": "c1", "output": "72"}
            ],
            "tools": [
                {"type": "function", "name": "get_weather", "description": "w", "parameters": {"type": "object"}},
                {"type": "x_search"}
            ]
        });
        let chat = transform_to_chat_completions(&request, &config()).expect("transform");
        assert_eq!(chat["model"], "deepseek-chat");
        assert_eq!(chat["stream"], true);
        assert_eq!(
            chat["messages"],
            json!([
                {"role": "system", "content": "be terse"},
                {"role": "user", "content": "hi"},
                {"role": "assistant", "content": "hello"},
                {"role": "assistant", "tool_calls": [
                    {"id": "c1", "type": "function", "function": {"name": "get_weather", "arguments": "{\"city\":\"SF\"}"}}
                ]},
                {"role": "tool", "tool_call_id": "c1", "content": "72"}
            ])
        );
        assert_eq!(
            chat["tools"],
            json!([
                {"type": "function", "function": {"name": "get_weather", "description": "w", "parameters": {"type": "object"}}}
            ])
        );
    }

    #[test]
    fn extract_usage_reads_deepseek_cache_fields() {
        let chunk = json!({
            "choices": [],
            "usage": {
                "prompt_tokens": 90,
                "completion_tokens": 25,
                "total_tokens": 115,
                "prompt_tokens_details": { "cached_tokens": 42 }
            }
        });
        assert_eq!(extract_usage(&chunk), Some((90, 42)));

        let without_usage = json!({ "choices": [{"delta": {"content": "hi"}}] });
        assert_eq!(extract_usage(&without_usage), None);
    }

    #[test]
    fn chat_stream_events_carry_incrementing_sequence_numbers() {
        let mut state = ChatStreamState::default();
        let events = state.push(
            br#"data: {"choices":[{"index":0,"delta":{"content":"hi"},"finish_reason":"stop"}]}

"#,
        );
        assert!(!events.is_empty());
        let text = String::from_utf8(events.concat()).expect("utf-8");
        assert!(text.contains("\"sequence_number\":0"));
        assert!(text.contains("\"sequence_number\":1"));
        let finish = String::from_utf8(state.finish().concat()).expect("utf-8");
        assert!(finish.contains("\"sequence_number\":2"));
    }

    #[test]
    fn chat_stream_surfaces_trailing_usage() {
        let mut state = ChatStreamState::default();
        state.push(br#"data: {"choices":[{"index":0,"delta":{"content":"hi"},"finish_reason":null}]}

"#);
        assert_eq!(state.take_usage(), None);
        state.push(br#"data: {"choices":[],"usage":{"prompt_tokens":90,"prompt_tokens_details":{"cached_tokens":42}}}

"#);
        assert_eq!(state.take_usage(), Some((90, 42)));
    }

    #[test]
    fn chat_stream_bridges_tool_calls() {
        let mut state = ChatStreamState::default();
        let mut events = Vec::new();
        events.extend(state.push(
            br#"data: {"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"get_weather","arguments":"{\"city\":"}}]},"finish_reason":null}]}

"#,
        ));
        events.extend(state.push(
            br#"data: {"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"\"SF\"}"}}]},"finish_reason":null}]}

"#,
        ));
        events.extend(state.push(
            br#"data: {"choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}

"#,
        ));
        events.extend(state.finish());

        let joined = events.concat();
        let text = String::from_utf8(joined).expect("events are utf-8");
        assert!(text.contains("\"type\":\"response.output_item.added\""));
        assert!(text.contains("\"type\":\"function_call\""));
        assert!(text.contains("\"call_id\":\"call_1\""));
        assert!(text.contains("\"name\":\"get_weather\""));
        assert!(text.contains("\"arguments\":\"{\\\"city\\\":\\\"SF\\\"}\""));
        assert!(text.contains("\"type\":\"response.function_call_arguments.done\""));
        assert!(text.contains("\"type\":\"response.completed\""));
    }
}
