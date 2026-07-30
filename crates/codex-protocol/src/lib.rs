//! Codex backend request compatibility.
//!
//! This crate intentionally works at the JSON boundary. Grok remains the owner
//! of its agent and tool-call model; this layer only applies the small set of
//! wire-level constraints required by the ChatGPT Codex Responses endpoint.

use std::collections::BTreeMap;

use http::{
    HeaderMap, HeaderValue,
    header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE},
};
use serde_json::Value;
use thiserror::Error;

pub const DEFAULT_ORIGINATOR: &str = "grok-build";

/// Stateful, incremental bridge between the Codex backend's output-item SSE
/// stream and Grok's expectation that terminal responses contain `output`.
#[derive(Debug, Default)]
pub struct CodexSseTransformer {
    buffer: Vec<u8>,
    output_items: BTreeMap<u64, Value>,
    filtered_keepalive_events: u64,
}

impl CodexSseTransformer {
    pub fn push(&mut self, bytes: &[u8]) -> Vec<Vec<u8>> {
        self.buffer.extend_from_slice(bytes);
        let mut events = Vec::new();
        while let Some((position, delimiter_length)) = find_sse_delimiter(&self.buffer) {
            let event = self
                .buffer
                .drain(..position + delimiter_length)
                .collect::<Vec<_>>();
            if let Some(event) = self.transform_event(event) {
                events.push(event);
            }
        }
        events
    }

    pub fn finish(&mut self) -> Vec<Vec<u8>> {
        if self.buffer.is_empty() {
            Vec::new()
        } else {
            let remainder = std::mem::take(&mut self.buffer);
            self.transform_event(remainder).into_iter().collect()
        }
    }

    pub fn take_filtered_keepalive_events(&mut self) -> u64 {
        std::mem::take(&mut self.filtered_keepalive_events)
    }

    fn transform_event(&mut self, original: Vec<u8>) -> Option<Vec<u8>> {
        let Ok(text) = std::str::from_utf8(&original) else {
            return Some(original);
        };
        let Some(data) = text.lines().find_map(|line| {
            line.strip_prefix("data:")
                .map(str::trim_start)
                .filter(|data| !data.is_empty() && *data != "[DONE]")
        }) else {
            return Some(original);
        };
        let Ok(mut event) = serde_json::from_str::<Value>(data) else {
            return Some(original);
        };
        if event.get("type").and_then(Value::as_str) == Some("keepalive") {
            self.filtered_keepalive_events = self.filtered_keepalive_events.saturating_add(1);
            return None;
        }
        let changed = self.observe_event(&mut event);
        if changed {
            Some(format!("data: {event}\n\n").into_bytes())
        } else {
            Some(original)
        }
    }

    fn observe_event(&mut self, event: &mut Value) -> bool {
        let kind = event
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let output_index = event
            .get("output_index")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        match kind {
            "response.output_item.added" | "response.output_item.done" => {
                if let Some(item) = event.get("item") {
                    self.output_items.insert(output_index, item.clone());
                }
                false
            }
            "response.output_text.delta" => {
                if let Some(delta) = event.get("delta").and_then(Value::as_str) {
                    let item_id = event
                        .get("item_id")
                        .and_then(Value::as_str)
                        .unwrap_or("msg_bridge");
                    append_output_text(
                        self.output_items
                            .entry(output_index)
                            .or_insert_with(|| message_item(item_id)),
                        delta,
                    );
                }
                false
            }
            "response.output_text.done" => {
                if let Some(text) = event.get("text").and_then(Value::as_str) {
                    let item_id = event
                        .get("item_id")
                        .and_then(Value::as_str)
                        .unwrap_or("msg_bridge");
                    set_output_text(
                        self.output_items
                            .entry(output_index)
                            .or_insert_with(|| message_item(item_id)),
                        text,
                    );
                }
                false
            }
            "response.function_call_arguments.delta" => {
                if let Some(delta) = event.get("delta").and_then(Value::as_str)
                    && let Some(item) = self.output_items.get_mut(&output_index)
                {
                    append_string_field(item, "arguments", delta);
                }
                false
            }
            "response.function_call_arguments.done" => {
                if let Some(arguments) = event.get("arguments").and_then(Value::as_str)
                    && let Some(item) = self.output_items.get_mut(&output_index)
                {
                    set_string_field(item, "arguments", arguments);
                }
                false
            }
            "response.completed" | "response.incomplete" => {
                let Some(response) = event.get_mut("response").and_then(Value::as_object_mut)
                else {
                    return false;
                };
                let output_is_empty = response
                    .get("output")
                    .is_none_or(|output| output.as_array().is_none_or(Vec::is_empty));
                if !output_is_empty || self.output_items.is_empty() {
                    return false;
                }
                let output = self
                    .output_items
                    .values()
                    .cloned()
                    .map(mark_item_completed)
                    .collect();
                response.insert("output".into(), Value::Array(output));
                true
            }
            _ => false,
        }
    }
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

fn message_item(item_id: &str) -> Value {
    serde_json::json!({
        "type": "message",
        "id": item_id,
        "role": "assistant",
        "status": "in_progress",
        "content": []
    })
}

fn append_output_text(item: &mut Value, delta: &str) {
    let Some(object) = item.as_object_mut() else {
        return;
    };
    let content = object
        .entry("content")
        .or_insert_with(|| Value::Array(Vec::new()));
    let Some(parts) = content.as_array_mut() else {
        return;
    };
    if let Some(text) = parts
        .iter_mut()
        .find(|part| part.get("type").and_then(Value::as_str) == Some("output_text"))
    {
        append_string_field(text, "text", delta);
    } else {
        parts.push(serde_json::json!({
            "type": "output_text",
            "text": delta,
            "annotations": []
        }));
    }
}

fn set_output_text(item: &mut Value, text: &str) {
    let Some(object) = item.as_object_mut() else {
        return;
    };
    object.insert(
        "content".into(),
        Value::Array(vec![serde_json::json!({
            "type": "output_text",
            "text": text,
            "annotations": []
        })]),
    );
}

fn append_string_field(value: &mut Value, field: &str, suffix: &str) {
    let Some(object) = value.as_object_mut() else {
        return;
    };
    let current = object
        .get(field)
        .and_then(Value::as_str)
        .unwrap_or_default();
    object.insert(field.into(), Value::String(format!("{current}{suffix}")));
}

fn set_string_field(value: &mut Value, field: &str, text: &str) {
    if let Some(object) = value.as_object_mut() {
        object.insert(field.into(), Value::String(text.into()));
    }
}

fn mark_item_completed(mut item: Value) -> Value {
    if let Some(object) = item.as_object_mut() {
        object.insert("status".into(), Value::String("completed".into()));
    }
    item
}

#[derive(Clone, Debug)]
pub struct TransformOptions {
    pub allowed_reasoning_efforts: Vec<String>,
    pub default_reasoning_effort: String,
}

impl Default for TransformOptions {
    fn default() -> Self {
        Self {
            allowed_reasoning_efforts: vec![
                "none".into(),
                "minimal".into(),
                "low".into(),
                "medium".into(),
                "high".into(),
                "xhigh".into(),
                "max".into(),
            ],
            default_reasoning_effort: "high".into(),
        }
    }
}

#[derive(Debug, Error)]
pub enum ProtocolError {
    #[error("Responses request must be a JSON object")]
    RequestMustBeObject,
    #[error("`include` must be an array when present")]
    IncludeMustBeArray,
    #[error("`instructions` must be a string when present")]
    InstructionsMustBeString,
    #[error("system/developer message content must contain text")]
    InstructionMessageMustContainText,
    #[error("invalid value for upstream header `{0}`")]
    InvalidHeader(&'static str),
}

/// Apply the stateless streaming contract while retaining all fields that Grok
/// owns, including input items and tool definitions.
pub fn transform_responses_request(
    mut request: Value,
    options: &TransformOptions,
) -> Result<Value, ProtocolError> {
    let object = request
        .as_object_mut()
        .ok_or(ProtocolError::RequestMustBeObject)?;

    object.insert("store".into(), Value::Bool(false));
    object.insert("stream".into(), Value::Bool(true));
    object.remove("max_output_tokens");
    object.remove("max_completion_tokens");
    object.remove("temperature");
    move_instruction_messages(object)?;

    match object.get_mut("include") {
        Some(Value::Array(include)) => {
            if !include
                .iter()
                .any(|item| item.as_str() == Some("reasoning.encrypted_content"))
            {
                include.push(Value::String("reasoning.encrypted_content".into()));
            }
        }
        Some(_) => return Err(ProtocolError::IncludeMustBeArray),
        None => {
            object.insert(
                "include".into(),
                Value::Array(vec![Value::String("reasoning.encrypted_content".into())]),
            );
        }
    }

    if let Some(reasoning) = object.get_mut("reasoning").and_then(Value::as_object_mut)
        && let Some(effort) = reasoning.get("effort").and_then(Value::as_str)
        && !options
            .allowed_reasoning_efforts
            .iter()
            .any(|allowed| allowed == effort)
    {
        reasoning.insert(
            "effort".into(),
            Value::String(options.default_reasoning_effort.clone()),
        );
    }

    Ok(request)
}

fn move_instruction_messages(
    object: &mut serde_json::Map<String, Value>,
) -> Result<(), ProtocolError> {
    let Some(Value::Array(input)) = object.get_mut("input") else {
        return Ok(());
    };
    let original = std::mem::take(input);
    let mut retained = Vec::with_capacity(original.len());
    let mut extracted = Vec::new();

    for item in original {
        let instruction_role = item
            .get("role")
            .and_then(Value::as_str)
            .is_some_and(|role| matches!(role, "system" | "developer"));
        if instruction_role {
            let text = item
                .get("content")
                .and_then(extract_text_content)
                .filter(|text| !text.trim().is_empty())
                .ok_or(ProtocolError::InstructionMessageMustContainText)?;
            extracted.push(text);
        } else {
            retained.push(item);
        }
    }
    *input = retained;

    if extracted.is_empty() {
        return Ok(());
    }
    let mut instructions = match object.remove("instructions") {
        Some(Value::String(existing)) => vec![existing],
        Some(Value::Null) | None => Vec::new(),
        Some(_) => return Err(ProtocolError::InstructionsMustBeString),
    };
    instructions.extend(extracted);
    object.insert(
        "instructions".into(),
        Value::String(instructions.join("\n\n")),
    );
    Ok(())
}

fn extract_text_content(content: &Value) -> Option<String> {
    match content {
        Value::String(text) => Some(text.clone()),
        Value::Array(parts) => {
            let text = parts
                .iter()
                .filter_map(|part| {
                    part.as_str()
                        .map(str::to_owned)
                        .or_else(|| part.get("text").and_then(Value::as_str).map(str::to_owned))
                })
                .collect::<Vec<_>>()
                .join("\n");
            (!text.is_empty()).then_some(text)
        }
        _ => None,
    }
}

/// Build a fresh upstream header map rather than mutating the inbound map. This
/// makes it impossible to accidentally retain Grok's placeholder bearer token.
pub fn build_upstream_headers(
    incoming: &HeaderMap,
    access_token: &str,
    account_id: &str,
    prompt_cache_key: Option<&str>,
) -> Result<HeaderMap, ProtocolError> {
    let mut headers = HeaderMap::new();

    // Correlation and tracing headers are safe and useful to preserve. Headers
    // carrying credentials, routing authority, or body framing are rebuilt.
    for name in [
        "x-request-id",
        "traceparent",
        "tracestate",
        "x-stainless-retry-count",
        "x-stainless-timeout",
    ] {
        if let Some(value) = incoming.get(name) {
            headers.insert(http::header::HeaderName::from_static(name), value.clone());
        }
    }

    let authorization = HeaderValue::from_str(&format!("Bearer {access_token}"))
        .map_err(|_| ProtocolError::InvalidHeader("authorization"))?;
    let account = HeaderValue::from_str(account_id)
        .map_err(|_| ProtocolError::InvalidHeader("chatgpt-account-id"))?;

    headers.insert(AUTHORIZATION, authorization);
    headers.insert("chatgpt-account-id", account);
    headers.insert("originator", HeaderValue::from_static(DEFAULT_ORIGINATOR));
    headers.insert(ACCEPT, HeaderValue::from_static("text/event-stream"));
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    if let Some(prompt_cache_key) = prompt_cache_key.filter(|key| !key.is_empty()) {
        let session = HeaderValue::from_str(prompt_cache_key)
            .map_err(|_| ProtocolError::InvalidHeader("prompt_cache_key"))?;
        headers.insert("session-id", session.clone());
        headers.insert("thread-id", session);
    }
    Ok(headers)
}
