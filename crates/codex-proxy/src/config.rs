//! Backend selection and OpenAI-compatible upstream configuration.
//!
//! The proxy can run one of two upstream backends:
//!
//! - `codex` (default): OAuth-backed ChatGPT/Codex Responses API with WebSocket
//!   incremental transport and an explicit `prompt_cache_key`.
//! - `openai`: any OpenAI-compatible provider configured by `base_url` and
//!   `api_key`, speaking either Chat `/chat/completions` (translated) or the
//!   Responses API `/responses` (near-passthrough). Caching defaults to
//!   implicit server-side prefix caching; `cache_policy = "explicit"` opts into
//!   an explicit `prompt_cache_key` (Responses mode only).

use std::collections::HashMap;

use serde::Deserialize;
use thiserror::Error;

/// Which upstream backend the loopback proxy drives.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum BackendKind {
    #[default]
    Codex,
    Openai,
}

/// Runtime-resolved OpenAI-compatible backend settings.
#[derive(Clone, Debug)]
pub struct OpenAIBackendConfig {
    /// Provider name (`None` for the default `[backend.openai]` provider).
    pub name: Option<String>,
    /// Model-name routing prefix. Requests whose `model` starts with this prefix
    /// route to this provider (longest-prefix match wins); the prefix is stripped
    /// before the model is sent upstream. `None` = the default fallback provider.
    pub model_prefix: Option<String>,
    pub base_url: String,
    pub api_key: String,
    pub default_model: Option<String>,
    /// Per-model context window in tokens advertised to Grok for synced models.
    /// When a model is absent, Grok applies its own default.
    pub model_context_windows: HashMap<String, i64>,
    /// DeepSeek thinking mode. `None` leaves it to the provider default; `true`
    /// / `false` sends `thinking: enabled|disabled`. Disabling avoids DeepSeek's
    /// requirement to echo `reasoning_content` back on follow-up turns.
    pub thinking: Option<bool>,
    /// Reasoning effort levels advertised to Grok so it flags synced models as
    /// reasoning-capable (and shows the thinking UI). Empty means non-reasoning.
    pub reasoning_efforts: Vec<String>,
    /// Default reasoning effort among `reasoning_efforts`.
    pub default_reasoning_effort: Option<String>,
    /// Which wire format this provider speaks: OpenAI Chat `/chat/completions`
    /// (the Grok Responses body is translated) or the Responses API
    /// `{base_url}/responses` (near-passthrough, enables provider-specific
    /// capabilities such as DeepSeek's server-side `web_search` tool).
    pub wire_format: WireFormat,
    /// Provider compatibility flags (see `CompatFlags`). Defaults preserve
    /// DeepSeek semantics.
    pub compat: CompatFlags,
    /// Cache strategy: implicit server-side prefix caching (default) or an
    /// explicit `prompt_cache_key` (Responses mode only).
    pub cache_policy: CachePolicy,
    /// Per-provider pricing in USD per 1M tokens; `None` omits cost estimates
    /// from diagnostics.
    pub cost: Option<Cost>,
    /// Keep Grok's `x_search` tool mapped to the provider's `web_search` tool
    /// (Responses mode only). Defaults to Responses mode when unset; set
    /// `false` to keep dropping search tools.
    pub web_search: bool,
    pub extra_headers: HashMap<String, String>,
}

/// Which OpenAI-compatible wire format a provider speaks.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum WireFormat {
    /// OpenAI Chat `/chat/completions`, translated from the Grok Responses
    /// body.
    #[default]
    ChatCompletions,
    /// OpenAI Responses API `{base_url}/responses`, near-passthrough.
    Responses,
}

/// Cache strategy for an OpenAI-compatible provider.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum CachePolicy {
    /// Rely on the provider's implicit server-side prefix caching; never emit
    /// an explicit `prompt_cache_key`.
    #[default]
    Implicit,
    /// Emit an explicit `prompt_cache_key` (Responses mode only), taken from
    /// the request body or derived from the `x-grok-session-id` header. For
    /// providers whose caching requires a stable key (e.g. OpenAI/xAI
    /// Responses).
    Explicit,
}

/// Which field carries the output-token budget on Chat requests.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum MaxTokensField {
    #[default]
    MaxTokens,
    MaxCompletionTokens,
}

/// Provider compatibility flags. The defaults match DeepSeek's semantics, so
/// existing configurations behave identically.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CompatFlags {
    /// Responses mode: strip `previous_response_id` (stateless providers).
    /// `false` keeps it for stateful Responses APIs (e.g. OpenAI).
    pub stateless: bool,
    /// Advertise and send `reasoning.effort`. `false` skips effort wiring for
    /// providers without a reasoning-effort parameter.
    pub supports_reasoning_effort: bool,
    /// Chat mode field for the output-token budget (`max_tokens` or
    /// `max_completion_tokens`).
    pub max_tokens_field: MaxTokensField,
}

impl Default for CompatFlags {
    fn default() -> Self {
        Self {
            stateless: true,
            supports_reasoning_effort: true,
            max_tokens_field: MaxTokensField::MaxTokens,
        }
    }
}

/// Per-provider pricing in USD per 1M tokens (fields align with pi's `cost`).
/// `cache_write` is reserved for providers that bill cache writes separately;
/// the proxy does not track write tokens yet.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Cost {
    pub input: f64,
    pub output: f64,
    pub cache_read: f64,
    pub cache_write: f64,
}

/// The full set of configured OpenAI-compatible backends: an optional default
/// (`[backend.openai]`, the fallback for models no named prefix matches) plus
/// any number of named providers (`[backend.openai.providers.<name>]`, each
/// routed by its `model_prefix`).
#[derive(Clone, Debug, Default)]
pub struct OpenAIBackends {
    pub default: Option<OpenAIBackendConfig>,
    pub named: Vec<OpenAIBackendConfig>,
}

impl OpenAIBackends {
    /// No OpenAI-compatible backend is configured at all (fall back to Codex).
    pub fn is_empty(&self) -> bool {
        self.default.is_none() && self.named.is_empty()
    }

    /// All configured providers, default first.
    pub fn all(&self) -> impl Iterator<Item = &OpenAIBackendConfig> {
        self.default.iter().chain(self.named.iter())
    }
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("failed to read config file `{path}`: {source}")]
    Read {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to parse config file `{path}`: {source}")]
    Parse {
        path: String,
        #[source]
        source: toml_edit::de::Error,
    },
    #[error("openai backend requires `api_key` (set it inline, via an env reference, or via OPENAI_API_KEY / DEEPSEEK_API_KEY)")]
    MissingApiKey,
    #[error("openai provider `{name}` requires `api_key` (set it inline or via an env reference such as $OPENCODE_ZEN_API_KEY)")]
    MissingProviderApiKey { name: String },
    #[error("openai backend requires a non-empty `base_url`")]
    MissingBaseUrl,
    #[error("invalid `{field}` value `{value}` (expected one of: {expected})")]
    InvalidEnumValue {
        field: String,
        value: String,
        expected: String,
    },
}

/// Shared field set for an OpenAI-compatible provider (`[backend.openai]` or
/// one named entry under `[backend.openai.providers.<name>]`).
#[derive(Clone, Debug, Default, Deserialize)]
struct RawProviderFields {
    base_url: Option<String>,
    api_key: Option<String>,
    model: Option<String>,
    #[serde(default)]
    model_context_windows: HashMap<String, i64>,
    thinking: Option<bool>,
    #[serde(default)]
    reasoning_efforts: Vec<String>,
    default_reasoning_effort: Option<String>,
    #[serde(default)]
    extra_headers: HashMap<String, String>,
    /// Model-name routing prefix for a named provider. Omitted on the default
    /// provider; when omitted on a named provider it defaults to `"{name}/"`.
    model_prefix: Option<String>,
    /// Wire format (`"chat-completions"` / `"responses"`). Deprecated alias:
    /// `responses_api = true` implies `"responses"`.
    wire_format: Option<String>,
    /// Responses API mode (see `OpenAIBackendConfig`). `None` = unset.
    responses_api: Option<bool>,
    /// Web search enablement in Responses mode. `None` = follow wire format.
    web_search: Option<bool>,
    /// Compatibility flags (`compat.stateless`, ...). `None` = defaults.
    compat: Option<RawCompatFields>,
    /// Cache strategy (`"implicit"` / `"explicit"`). `None` = implicit.
    cache_policy: Option<String>,
    /// Per-provider pricing. `None` = no cost estimates.
    cost: Option<RawCostFields>,
}

/// Deserialized shape of a `[backend.openai.compat]` table (or the flat
/// `.env` `OPENAI_COMPAT_*` keys). Missing fields fall back to
/// `CompatFlags::default()`.
#[derive(Clone, Debug, Default, Deserialize)]
struct RawCompatFields {
    stateless: Option<bool>,
    supports_reasoning_effort: Option<bool>,
    max_tokens_field: Option<String>,
}

/// Deserialized shape of a `[backend.openai.cost]` table (or the flat `.env`
/// `OPENAI_COST_*` keys). Missing prices default to `0.0`.
#[derive(Clone, Debug, Default, Deserialize)]
struct RawCostFields {
    input: Option<f64>,
    output: Option<f64>,
    cache_read: Option<f64>,
    cache_write: Option<f64>,
}

/// Deserialized shape of the `[backend.openai]` TOML section (or .env file).
/// Its own fields describe the default provider; nested `providers.<name>`
/// tables describe additional named providers.
#[derive(Clone, Debug, Default, Deserialize)]
struct RawOpenAIConfig {
    base_url: Option<String>,
    api_key: Option<String>,
    model: Option<String>,
    #[serde(default)]
    model_context_windows: HashMap<String, i64>,
    thinking: Option<bool>,
    #[serde(default)]
    reasoning_efforts: Vec<String>,
    default_reasoning_effort: Option<String>,
    #[serde(default)]
    extra_headers: HashMap<String, String>,
    #[serde(default)]
    providers: HashMap<String, RawProviderFields>,
    /// Wire format (`"chat-completions"` / `"responses"`). Deprecated alias:
    /// `responses_api = true` implies `"responses"`.
    wire_format: Option<String>,
    /// Responses API mode (see `OpenAIBackendConfig`). `None` = unset.
    responses_api: Option<bool>,
    /// Web search enablement in Responses mode. `None` = follow wire format.
    web_search: Option<bool>,
    /// Compatibility flags. `None` = defaults.
    compat: Option<RawCompatFields>,
    /// Cache strategy (`"implicit"` / `"explicit"`). `None` = implicit.
    cache_policy: Option<String>,
    /// Per-provider pricing. `None` = no cost estimates.
    cost: Option<RawCostFields>,
}

impl RawOpenAIConfig {
    /// The default provider's fields (no routing prefix).
    fn default_fields(&self) -> RawProviderFields {
        RawProviderFields {
            base_url: self.base_url.clone(),
            api_key: self.api_key.clone(),
            model: self.model.clone(),
            model_context_windows: self.model_context_windows.clone(),
            thinking: self.thinking,
            reasoning_efforts: self.reasoning_efforts.clone(),
            default_reasoning_effort: self.default_reasoning_effort.clone(),
            extra_headers: self.extra_headers.clone(),
            model_prefix: None,
            wire_format: self.wire_format.clone(),
            responses_api: self.responses_api,
            web_search: self.web_search,
            compat: self.compat.clone(),
            cache_policy: self.cache_policy.clone(),
            cost: self.cost.clone(),
        }
    }
}

/// Parse `[backend.openai]` (and any `[backend.openai.providers.*]`) from a
/// TOML file, resolving env-var references. Returns `None` when no OpenAI
/// backend is configured.
pub fn openai_backend_from_toml(
    path: &std::path::Path,
) -> Result<Option<OpenAIBackends>, ConfigError> {
    let contents = std::fs::read_to_string(path).map_err(|source| ConfigError::Read {
        path: path.display().to_string(),
        source,
    })?;
    #[derive(Deserialize, Default)]
    struct File {
        #[serde(default)]
        backend: Backend,
    }
    #[derive(Deserialize, Default)]
    struct Backend {
        openai: Option<RawOpenAIConfig>,
    }
    let file = toml_edit::de::from_str::<File>(&contents).map_err(|source| ConfigError::Parse {
        path: path.display().to_string(),
        source,
    })?;
    let Some(raw) = file.backend.openai else {
        return Ok(None);
    };
    let backends = resolve(raw)?;
    if backends.is_empty() {
        Ok(None)
    } else {
        Ok(Some(backends))
    }
}

/// Parse an OpenAI backend configuration from a flat `.env`-style file.
///
/// Recognised keys: `OPENAI_BASE_URL`, `OPENAI_API_KEY`, `OPENAI_MODEL`, and
/// any `OPENAI_HEADER_<NAME>` entries (upper-cased name, `-` allowed).
pub fn openai_backend_from_env_file(
    path: &std::path::Path,
) -> Result<Option<OpenAIBackends>, ConfigError> {
    let contents = std::fs::read_to_string(path).map_err(|source| ConfigError::Read {
        path: path.display().to_string(),
        source,
    })?;
    let mut base_url = None;
    let mut api_key = None;
    let mut model = None;
    let mut extra_headers = HashMap::new();
    let mut wire_format = None;
    let mut responses_api = None;
    let mut web_search = None;
    let mut compat = RawCompatFields::default();
    let mut cache_policy = None;
    let mut cost = RawCostFields::default();
    for line in contents.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((key, value)) = line.split_once('=') else {
            continue;
        };
        let key = key.trim();
        let value = strip_quotes(value.trim());
        match key {
            "OPENAI_BASE_URL" => base_url = Some(value.to_owned()),
            "OPENAI_API_KEY" => api_key = Some(value.to_owned()),
            "OPENAI_MODEL" => model = Some(value.to_owned()),
            // Enum-style keys validate eagerly so a typo cannot silently switch
            // the upstream protocol.
            "OPENAI_WIRE_FORMAT" => {
                parse_wire_format(&value)?;
                wire_format = Some(value.to_owned());
            }
            "OPENAI_RESPONSES_API" => responses_api = parse_env_bool(&value),
            "OPENAI_WEB_SEARCH" => web_search = parse_env_bool(&value),
            "OPENAI_COMPAT_STATELESS" => compat.stateless = parse_env_bool(&value),
            "OPENAI_COMPAT_SUPPORTS_REASONING_EFFORT" => {
                compat.supports_reasoning_effort = parse_env_bool(&value);
            }
            "OPENAI_COMPAT_MAX_TOKENS_FIELD" => {
                parse_max_tokens_field(&value)?;
                compat.max_tokens_field = Some(value.to_owned());
            }
            "OPENAI_CACHE_POLICY" => {
                parse_cache_policy(&value)?;
                cache_policy = Some(value.to_owned());
            }
            "OPENAI_COST_INPUT" => cost.input = value.parse().ok(),
            "OPENAI_COST_OUTPUT" => cost.output = value.parse().ok(),
            "OPENAI_COST_CACHE_READ" => cost.cache_read = value.parse().ok(),
            "OPENAI_COST_CACHE_WRITE" => cost.cache_write = value.parse().ok(),
            _ if key.starts_with("OPENAI_HEADER_") => {
                let name = key
                    .trim_start_matches("OPENAI_HEADER_")
                    .to_ascii_lowercase()
                    .replace('_', "-");
                extra_headers.insert(name, value.to_owned());
            }
            _ => {}
        }
    }
    if base_url.is_none() && api_key.is_none() && model.is_none() && extra_headers.is_empty() {
        return Ok(None);
    }
    let compat_fields = (compat.stateless.is_some()
        || compat.supports_reasoning_effort.is_some()
        || compat.max_tokens_field.is_some())
    .then_some(compat);
    let cost_fields = (cost.input.is_some()
        || cost.output.is_some()
        || cost.cache_read.is_some()
        || cost.cache_write.is_some())
    .then_some(cost);
    let fields = RawProviderFields {
        base_url,
        api_key,
        model,
        model_context_windows: HashMap::new(),
        thinking: None,
        reasoning_efforts: Vec::new(),
        default_reasoning_effort: None,
        extra_headers,
        model_prefix: None,
        wire_format,
        responses_api,
        web_search,
        compat: compat_fields,
        cache_policy,
        cost: cost_fields,
    };
    let default = resolve_provider(fields, None, None, &system_env)?;
    Ok(Some(OpenAIBackends { default, named: Vec::new() }))
}

/// Parse an `.env` boolean value (`1`, `true`, `yes` are truthy).
fn parse_env_bool(value: &str) -> Option<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}

/// Resolve the default provider plus every named `[backend.openai.providers.*]`
/// table into an `OpenAIBackends`. The default may be absent (the section may
/// host only named providers); each named provider must have a `base_url`.
fn resolve(raw: RawOpenAIConfig) -> Result<OpenAIBackends, ConfigError> {
    resolve_with_env(raw, &system_env)
}

/// `resolve` with an injectable environment lookup (test seam).
fn resolve_with_env(
    raw: RawOpenAIConfig,
    env: &dyn Fn(&str) -> Option<String>,
) -> Result<OpenAIBackends, ConfigError> {
    let default = resolve_provider(raw.default_fields(), None, None, env)?;
    let mut named = Vec::with_capacity(raw.providers.len());
    for (name, fields) in raw.providers {
        if fields.base_url.is_none() {
            return Err(ConfigError::MissingBaseUrl);
        }
        let model_prefix = fields
            .model_prefix
            .clone()
            .or_else(|| Some(format!("{name}/")));
        if let Some(provider) = resolve_provider(fields, Some(name), model_prefix, env)? {
            named.push(provider);
        }
    }
    // Deterministic order regardless of HashMap iteration.
    named.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(OpenAIBackends { default, named })
}

/// Resolve a single provider's fields into a runtime config. Returns `None`
/// when there is no `base_url` (used for the optional default provider).
fn resolve_provider(
    raw: RawProviderFields,
    name: Option<String>,
    model_prefix: Option<String>,
    env: &dyn Fn(&str) -> Option<String>,
) -> Result<Option<OpenAIBackendConfig>, ConfigError> {
    let Some(base_url) = raw.base_url else {
        return Ok(None);
    };
    let base_url = expand_env_with(&base_url, env);
    if base_url.trim().is_empty() {
        return Err(ConfigError::MissingBaseUrl);
    }
    // Named providers must supply their own key; only the default provider
    // falls back to the shared OPENAI_API_KEY / DEEPSEEK_API_KEY env vars.
    let api_key = raw
        .api_key
        .map(|key| expand_env_with(&key, env))
        .filter(|key| !key.is_empty())
        .or_else(|| {
            if name.is_none() {
                env("OPENAI_API_KEY").filter(|key| !key.is_empty())
            } else {
                None
            }
        })
        .or_else(|| {
            if name.is_none() {
                env("DEEPSEEK_API_KEY").filter(|key| !key.is_empty())
            } else {
                None
            }
        })
        .ok_or_else(|| match &name {
            Some(name) => ConfigError::MissingProviderApiKey { name: name.clone() },
            None => ConfigError::MissingApiKey,
        })?;
    let wire_format = match raw.wire_format {
        Some(value) => parse_wire_format(&value)?,
        None => {
            if raw.responses_api.unwrap_or(false) {
                WireFormat::Responses
            } else {
                WireFormat::ChatCompletions
            }
        }
    };
    let compat = match raw.compat {
        Some(fields) => resolve_compat(fields)?,
        None => CompatFlags::default(),
    };
    let cache_policy = match raw.cache_policy {
        Some(value) => parse_cache_policy(&value)?,
        None => CachePolicy::Implicit,
    };
    Ok(Some(OpenAIBackendConfig {
        name,
        model_prefix,
        base_url,
        api_key,
        default_model: raw.model,
        model_context_windows: raw.model_context_windows,
        thinking: raw.thinking,
        reasoning_efforts: raw.reasoning_efforts,
        default_reasoning_effort: raw.default_reasoning_effort,
        wire_format,
        compat,
        cache_policy,
        cost: raw.cost.map(resolve_cost),
        // Web search defaults to the Responses wire format so enabling it also
        // enables DeepSeek's server-side web search, unless explicitly disabled.
        web_search: raw.web_search.unwrap_or(wire_format == WireFormat::Responses),
        extra_headers: raw.extra_headers,
    }))
}

fn parse_wire_format(value: &str) -> Result<WireFormat, ConfigError> {
    match value.trim() {
        "chat-completions" => Ok(WireFormat::ChatCompletions),
        "responses" => Ok(WireFormat::Responses),
        other => Err(ConfigError::InvalidEnumValue {
            field: "wire_format".into(),
            value: other.into(),
            expected: "`chat-completions` or `responses`".into(),
        }),
    }
}

fn parse_cache_policy(value: &str) -> Result<CachePolicy, ConfigError> {
    match value.trim() {
        "implicit" => Ok(CachePolicy::Implicit),
        "explicit" => Ok(CachePolicy::Explicit),
        other => Err(ConfigError::InvalidEnumValue {
            field: "cache_policy".into(),
            value: other.into(),
            expected: "`implicit` or `explicit`".into(),
        }),
    }
}

fn parse_max_tokens_field(value: &str) -> Result<MaxTokensField, ConfigError> {
    match value.trim() {
        "max_tokens" => Ok(MaxTokensField::MaxTokens),
        "max_completion_tokens" => Ok(MaxTokensField::MaxCompletionTokens),
        other => Err(ConfigError::InvalidEnumValue {
            field: "compat.max_tokens_field".into(),
            value: other.into(),
            expected: "`max_tokens` or `max_completion_tokens`".into(),
        }),
    }
}

fn resolve_compat(raw: RawCompatFields) -> Result<CompatFlags, ConfigError> {
    let max_tokens_field = match raw.max_tokens_field {
        Some(value) => parse_max_tokens_field(&value)?,
        None => MaxTokensField::default(),
    };
    Ok(CompatFlags {
        stateless: raw.stateless.unwrap_or(true),
        supports_reasoning_effort: raw.supports_reasoning_effort.unwrap_or(true),
        max_tokens_field,
    })
}

fn resolve_cost(raw: RawCostFields) -> Cost {
    Cost {
        input: raw.input.unwrap_or(0.0),
        output: raw.output.unwrap_or(0.0),
        cache_read: raw.cache_read.unwrap_or(0.0),
        cache_write: raw.cache_write.unwrap_or(0.0),
    }
}

/// Read an environment variable (default env source; injectable in tests).
fn system_env(name: &str) -> Option<String> {
    std::env::var(name).ok()
}

fn expand_env_with(value: &str, env: &dyn Fn(&str) -> Option<String>) -> String {
    if let Some(variable) = value
        .strip_prefix("${")
        .and_then(|rest| rest.strip_suffix('}'))
        .map(str::trim)
        .filter(|variable| !variable.is_empty())
    {
        env(variable).unwrap_or_default()
    } else {
        value.to_owned()
    }
}

fn strip_quotes(value: &str) -> String {
    let value = value.trim();
    let (start, end) = match value.as_bytes().first().copied() {
        Some(b'"') => (1_usize, value.len().saturating_sub(1)),
        Some(b'\'') => (1_usize, value.len().saturating_sub(1)),
        _ => (0_usize, value.len()),
    };
    value[start..end].to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expand_env_preserves_plain_values() {
        let no_env = |_name: &str| None;
        assert_eq!(expand_env_with("plain", &no_env), "plain");
        assert_eq!(expand_env_with("", &no_env), "");
        // A missing env reference resolves to empty rather than panicking.
        assert_eq!(expand_env_with("${GROK_DEFINITELY_MISSING_999}", &no_env), "");
    }

    #[test]
    fn strips_quotes_around_env_values() {
        assert_eq!(strip_quotes("\"sk-123\""), "sk-123");
        assert_eq!(strip_quotes("'sk-123'"), "sk-123");
        assert_eq!(strip_quotes("sk-123"), "sk-123");
    }

    #[test]
    fn resolve_requires_base_url_and_api_key() {
        // Use an empty environment so the default-provider fallback cannot pick
        // up ambient OPENAI_API_KEY / DEEPSEEK_API_KEY from the developer shell.
        let no_env = |_name: &str| None;
        let raw = RawOpenAIConfig {
            base_url: None,
            api_key: None,
            model: None,
            model_context_windows: HashMap::new(),
            thinking: None,
            reasoning_efforts: Vec::new(),
            default_reasoning_effort: None,
            extra_headers: HashMap::new(),
            providers: HashMap::new(),
            wire_format: None,
            responses_api: None,
            web_search: None,
            compat: None,
            cache_policy: None,
            cost: None,
        };
        // No base_url anywhere -> empty (not an error), the section may host
        // only named providers.
        let backends = resolve_with_env(raw, &no_env).expect("resolve empty");
        assert!(backends.is_empty());

        let missing_key = RawOpenAIConfig {
            base_url: Some("https://api.deepseek.com/v1".into()),
            api_key: None,
            model: None,
            model_context_windows: HashMap::new(),
            thinking: None,
            reasoning_efforts: Vec::new(),
            default_reasoning_effort: None,
            extra_headers: HashMap::new(),
            providers: HashMap::new(),
            wire_format: None,
            responses_api: None,
            web_search: None,
            compat: None,
            cache_policy: None,
            cost: None,
        };
        assert!(matches!(
            resolve_with_env(missing_key, &no_env),
            Err(ConfigError::MissingApiKey)
        ));
    }

    #[test]
    fn env_file_parses_key_value_pairs() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("backend.env");
        std::fs::write(
            &path,
            "# comment\nOPENAI_BASE_URL=https://api.deepseek.com/v1\nOPENAI_API_KEY=sk-test\nOPENAI_MODEL=deepseek-chat\n",
        )
        .expect("write");
        let config = openai_backend_from_env_file(&path).expect("parse").expect("present");
        let config = config.default.expect("default provider");
        assert_eq!(config.base_url, "https://api.deepseek.com/v1");
        assert_eq!(config.api_key, "sk-test");
        assert_eq!(config.default_model.as_deref(), Some("deepseek-chat"));
        assert!(config.model_context_windows.is_empty());
        assert!(config.name.is_none());
    }

    #[test]
    fn toml_parses_per_model_context_windows() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("proxy.toml");
        std::fs::write(
            &path,
            "[backend.openai]\n\
             base_url = \"https://api.deepseek.com/v1\"\n\
             api_key = \"sk-test\"\n\
             [backend.openai.model_context_windows]\n\
             \"deepseek-v4-flash\" = 1048576\n\
             \"deepseek-v4-pro\" = 1048576\n",
        )
        .expect("write");
        let config = openai_backend_from_toml(&path).expect("parse").expect("present");
        let config = config.default.expect("default provider");
        assert_eq!(config.model_context_windows.get("deepseek-v4-flash"), Some(&1_048_576));
        assert_eq!(config.model_context_windows.get("deepseek-v4-pro"), Some(&1_048_576));
    }

    #[test]
    fn toml_parses_reasoning_efforts() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("proxy.toml");
        std::fs::write(
            &path,
            "[backend.openai]\n\
             base_url = \"https://api.deepseek.com/v1\"\n\
             api_key = \"sk-test\"\n\
             reasoning_efforts = [\"low\", \"high\", \"max\"]\n\
             default_reasoning_effort = \"high\"\n",
        )
        .expect("write");
        let config = openai_backend_from_toml(&path).expect("parse").expect("present");
        let config = config.default.expect("default provider");
        assert_eq!(config.reasoning_efforts, vec!["low".to_string(), "high".to_string(), "max".to_string()]);
        assert_eq!(config.default_reasoning_effort.as_deref(), Some("high"));
    }

    #[test]
    fn toml_parses_responses_api_and_web_search_default_follows() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("proxy.toml");
        // `responses_api = true` alone must enable web search by default.
        std::fs::write(
            &path,
            "[backend.openai]\n\
             base_url = \"https://api.deepseek.com\"\n\
             api_key = \"sk-test\"\n\
             responses_api = true\n",
        )
        .expect("write");
        let backends = openai_backend_from_toml(&path).expect("parse").expect("present");
        let config = backends.default.expect("default provider");
        assert_eq!(config.wire_format, WireFormat::Responses);
        assert!(config.web_search, "web_search should follow Responses mode by default");
    }

    #[test]
    fn toml_parses_web_search_explicitly_disabled() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("proxy.toml");
        std::fs::write(
            &path,
            "[backend.openai]\n\
             base_url = \"https://api.deepseek.com\"\n\
             api_key = \"sk-test\"\n\
             responses_api = true\n\
             web_search = false\n",
        )
        .expect("write");
        let backends = openai_backend_from_toml(&path).expect("parse").expect("present");
        let config = backends.default.expect("default provider");
        assert_eq!(config.wire_format, WireFormat::Responses);
        assert!(!config.web_search, "explicit web_search = false must be honored");
    }

    #[test]
    fn toml_parses_named_provider_responses_api() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("proxy.toml");
        std::fs::write(
            &path,
            "[backend.openai.providers.deepseek]\n\
             base_url = \"https://api.deepseek.com\"\n\
             api_key = \"sk-ds\"\n\
             responses_api = true\n\
             model_prefix = \"deepseek/\"\n",
        )
        .expect("write");
        let backends = openai_backend_from_toml(&path).expect("parse").expect("present");
        assert!(backends.default.is_none());
        assert_eq!(backends.named.len(), 1);
        assert_eq!(backends.named[0].wire_format, WireFormat::Responses);
        assert!(backends.named[0].web_search);
    }

    #[test]
    fn env_file_parses_responses_api_and_web_search() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("backend.env");
        std::fs::write(
            &path,
            "OPENAI_BASE_URL=https://api.deepseek.com\n\
             OPENAI_API_KEY=sk-test\n\
             OPENAI_RESPONSES_API=1\n\
             OPENAI_WEB_SEARCH=true\n",
        )
        .expect("write");
        let backends = openai_backend_from_env_file(&path).expect("parse").expect("present");
        let config = backends.default.expect("default provider");
        assert_eq!(config.wire_format, WireFormat::Responses);
        assert!(config.web_search);
    }

    #[test]
    fn toml_parses_named_providers_with_prefixes() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("proxy.toml");
        std::fs::write(
            &path,
            "[backend.openai]\n\
             base_url = \"https://api.deepseek.com/v1\"\n\
             api_key = \"sk-ds\"\n\
             [backend.openai.model_context_windows]\n\
             \"deepseek-v4-flash\" = 1048576\n\
             [backend.openai.providers.opencode-zen]\n\
             base_url = \"https://opencode.ai/zen/v1\"\n\
             api_key = \"sk-zen\"\n\
             model = \"opencode/deepseek-v4-flash\"\n\
             model_prefix = \"opencode/\"\n\
             [backend.openai.providers.opencode-zen.model_context_windows]\n\
             \"opencode/deepseek-v4-flash\" = 1048576\n",
        )
        .expect("write");
        let backends = openai_backend_from_toml(&path).expect("parse").expect("present");
        let default = backends.default.expect("default provider");
        assert_eq!(default.base_url, "https://api.deepseek.com/v1");
        assert_eq!(default.name, None);
        assert_eq!(default.model_prefix, None);

        assert_eq!(backends.named.len(), 1);
        let zen = &backends.named[0];
        assert_eq!(zen.name.as_deref(), Some("opencode-zen"));
        assert_eq!(zen.model_prefix.as_deref(), Some("opencode/"));
        assert_eq!(zen.base_url, "https://opencode.ai/zen/v1");
        assert_eq!(zen.api_key, "sk-zen");
        assert_eq!(zen.default_model.as_deref(), Some("opencode/deepseek-v4-flash"));
        assert_eq!(
            zen.model_context_windows.get("opencode/deepseek-v4-flash"),
            Some(&1_048_576)
        );
    }

    #[test]
    fn named_provider_prefix_defaults_to_name_slash() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("proxy.toml");
        std::fs::write(
            &path,
            "[backend.openai.providers.groq]\n\
             base_url = \"https://api.groq.com/openai/v1\"\n\
             api_key = \"sk-groq\"\n",
        )
        .expect("write");
        let backends = openai_backend_from_toml(&path).expect("parse").expect("present");
        assert!(backends.default.is_none());
        assert_eq!(backends.named.len(), 1);
        assert_eq!(backends.named[0].name.as_deref(), Some("groq"));
        assert_eq!(backends.named[0].model_prefix.as_deref(), Some("groq/"));
    }

    #[test]
    fn named_provider_requires_api_key() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("proxy.toml");
        std::fs::write(
            &path,
            "[backend.openai.providers.zen]\n\
             base_url = \"https://opencode.ai/zen/v1\"\n",
        )
        .expect("write");
        let error = openai_backend_from_toml(&path).expect_err("missing key");
        assert!(matches!(
            error,
            ConfigError::MissingProviderApiKey { name } if name == "zen"
        ));
    }

    #[test]
    fn toml_parses_wire_format_explicitly() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("proxy.toml");
        std::fs::write(
            &path,
            "[backend.openai]\n\
             base_url = \"https://api.deepseek.com\"\n\
             api_key = \"sk-test\"\n\
             wire_format = \"responses\"\n",
        )
        .expect("write");
        let backends = openai_backend_from_toml(&path).expect("parse").expect("present");
        let config = backends.default.expect("default provider");
        assert_eq!(config.wire_format, WireFormat::Responses);
        assert!(config.web_search, "web_search follows Responses wire format by default");
    }

    #[test]
    fn toml_rejects_invalid_wire_format() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("proxy.toml");
        std::fs::write(
            &path,
            "[backend.openai]\n\
             base_url = \"https://api.deepseek.com\"\n\
             api_key = \"sk-test\"\n\
             wire_format = \"banana\"\n",
        )
        .expect("write");
        let error = openai_backend_from_toml(&path).expect_err("invalid wire_format");
        assert!(matches!(
            error,
            ConfigError::InvalidEnumValue { ref field, ref value, .. }
                if field == "wire_format" && value == "banana"
        ));
    }

    #[test]
    fn responses_api_alias_still_selects_responses_mode() {
        // Deprecated `responses_api = true` keeps working without `wire_format`.
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("proxy.toml");
        std::fs::write(
            &path,
            "[backend.openai]\n\
             base_url = \"https://api.deepseek.com\"\n\
             api_key = \"sk-test\"\n\
             responses_api = true\n\
             web_search = false\n",
        )
        .expect("write");
        let backends = openai_backend_from_toml(&path).expect("parse").expect("present");
        let config = backends.default.expect("default provider");
        assert_eq!(config.wire_format, WireFormat::Responses);
        assert!(!config.web_search);
    }

    #[test]
    fn toml_parses_compat_flags_with_defaults() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("proxy.toml");
        std::fs::write(
            &path,
            "[backend.openai]\n\
             base_url = \"https://api.deepseek.com\"\n\
             api_key = \"sk-test\"\n\
             [backend.openai.compat]\n\
             stateless = false\n\
             supports_reasoning_effort = false\n\
             max_tokens_field = \"max_completion_tokens\"\n",
        )
        .expect("write");
        let backends = openai_backend_from_toml(&path).expect("parse").expect("present");
        let config = backends.default.expect("default provider");
        assert!(!config.compat.stateless);
        assert!(!config.compat.supports_reasoning_effort);
        assert_eq!(config.compat.max_tokens_field, MaxTokensField::MaxCompletionTokens);
    }

    #[test]
    fn compat_defaults_preserve_deepseek_semantics() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("proxy.toml");
        std::fs::write(
            &path,
            "[backend.openai]\n\
             base_url = \"https://api.deepseek.com\"\n\
             api_key = \"sk-test\"\n",
        )
        .expect("write");
        let backends = openai_backend_from_toml(&path).expect("parse").expect("present");
        let config = backends.default.expect("default provider");
        assert_eq!(config.compat, CompatFlags::default());
        assert!(config.compat.stateless);
        assert!(config.compat.supports_reasoning_effort);
        assert_eq!(config.compat.max_tokens_field, MaxTokensField::MaxTokens);
    }

    #[test]
    fn toml_rejects_invalid_max_tokens_field() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("proxy.toml");
        std::fs::write(
            &path,
            "[backend.openai]\n\
             base_url = \"https://api.deepseek.com\"\n\
             api_key = \"sk-test\"\n\
             [backend.openai.compat]\n\
             max_tokens_field = \"banana\"\n",
        )
        .expect("write");
        let error = openai_backend_from_toml(&path).expect_err("invalid max_tokens_field");
        assert!(matches!(
            error,
            ConfigError::InvalidEnumValue { ref field, .. }
                if field == "compat.max_tokens_field"
        ));
    }

    #[test]
    fn toml_parses_cache_policy_and_cost() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("proxy.toml");
        std::fs::write(
            &path,
            "[backend.openai]\n\
             base_url = \"https://api.deepseek.com\"\n\
             api_key = \"sk-test\"\n\
             cache_policy = \"explicit\"\n\
             [backend.openai.cost]\n\
             input = 0.27\n\
             output = 1.10\n\
             cache_read = 0.07\n\
             cache_write = 0.27\n",
        )
        .expect("write");
        let backends = openai_backend_from_toml(&path).expect("parse").expect("present");
        let config = backends.default.expect("default provider");
        assert_eq!(config.cache_policy, CachePolicy::Explicit);
        let cost = config.cost.expect("cost table");
        assert_eq!(cost.input, 0.27);
        assert_eq!(cost.output, 1.10);
        assert_eq!(cost.cache_read, 0.07);
        assert_eq!(cost.cache_write, 0.27);
    }

    #[test]
    fn toml_rejects_invalid_cache_policy() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("proxy.toml");
        std::fs::write(
            &path,
            "[backend.openai]\n\
             base_url = \"https://api.deepseek.com\"\n\
             api_key = \"sk-test\"\n\
             cache_policy = \"banana\"\n",
        )
        .expect("write");
        let error = openai_backend_from_toml(&path).expect_err("invalid cache_policy");
        assert!(matches!(
            error,
            ConfigError::InvalidEnumValue { ref field, .. } if field == "cache_policy"
        ));
    }

    #[test]
    fn env_file_parses_wire_format_compat_cache_and_cost() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("backend.env");
        std::fs::write(
            &path,
            "OPENAI_BASE_URL=https://api.deepseek.com\n\
             OPENAI_API_KEY=sk-test\n\
             OPENAI_WIRE_FORMAT=responses\n\
             OPENAI_COMPAT_STATELESS=0\n\
             OPENAI_COMPAT_SUPPORTS_REASONING_EFFORT=0\n\
             OPENAI_COMPAT_MAX_TOKENS_FIELD=max_completion_tokens\n\
             OPENAI_CACHE_POLICY=explicit\n\
             OPENAI_COST_INPUT=0.27\n\
             OPENAI_COST_OUTPUT=1.10\n\
             OPENAI_COST_CACHE_READ=0.07\n",
        )
        .expect("write");
        let backends = openai_backend_from_env_file(&path).expect("parse").expect("present");
        let config = backends.default.expect("default provider");
        assert_eq!(config.wire_format, WireFormat::Responses);
        assert!(!config.compat.stateless);
        assert!(!config.compat.supports_reasoning_effort);
        assert_eq!(config.compat.max_tokens_field, MaxTokensField::MaxCompletionTokens);
        assert_eq!(config.cache_policy, CachePolicy::Explicit);
        let cost = config.cost.expect("cost");
        assert_eq!(cost.input, 0.27);
        assert_eq!(cost.cache_write, 0.0, "unset cost field defaults to zero");
    }

    #[test]
    fn env_file_rejects_invalid_wire_format_eagerly() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("backend.env");
        std::fs::write(
            &path,
            "OPENAI_BASE_URL=https://api.deepseek.com\n\
             OPENAI_API_KEY=sk-test\n\
             OPENAI_WIRE_FORMAT=banana\n",
        )
        .expect("write");
        let error = openai_backend_from_env_file(&path).expect_err("invalid wire_format");
        assert!(matches!(
            error,
            ConfigError::InvalidEnumValue { ref field, .. } if field == "wire_format"
        ));
    }

    #[test]
    fn shipped_proxy_toml_example_parses() {
        // The example shipped in plugin/assets must always stay parseable, so a
        // schema change can never silently break the documented configuration.
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../plugin/assets/proxy.toml.example"
        );
        let backends =
            openai_backend_from_toml(std::path::Path::new(path)).expect("parse").expect("present");
        let default = backends.default.expect("default provider");
        assert_eq!(default.wire_format, WireFormat::Responses);
        assert_eq!(default.cache_policy, CachePolicy::Implicit);
        assert_eq!(default.compat, CompatFlags::default());
        let cost = default.cost.expect("default provider cost table");
        assert_eq!(cost.input, 1.5);
        assert_eq!(cost.cache_read, 0.05);
        assert_eq!(backends.named.len(), 1);
        assert_eq!(backends.named[0].name.as_deref(), Some("opencode-zen"));
        assert_eq!(backends.named[0].wire_format, WireFormat::ChatCompletions);
        assert!(backends.named[0].cost.is_none(), "zen cost table stays commented out");
    }
}
