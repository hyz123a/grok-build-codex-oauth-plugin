//! Backend selection and OpenAI-compatible upstream configuration.
//!
//! The proxy can run one of two upstream backends:
//!
//! - `codex` (default): OAuth-backed ChatGPT/Codex Responses API with WebSocket
//!   incremental transport and an explicit `prompt_cache_key`.
//! - `openai`: any OpenAI Chat `/chat/completions` compatible provider
//!   (DeepSeek, Groq, Together, a local gateway, ...) configured by `base_url`
//!   and `api_key`. Caching is implicit server-side prefix caching; no explicit
//!   key is emitted.

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
    pub extra_headers: HashMap<String, String>,
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
    #[error("openai backend requires a non-empty `base_url`")]
    MissingBaseUrl,
}

/// Deserialized shape of the `[backend.openai]` TOML section (or .env file).
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
}

/// Parse `[backend.openai]` from a TOML file, resolving env-var references.
pub fn openai_backend_from_toml(
    path: &std::path::Path,
) -> Result<Option<OpenAIBackendConfig>, ConfigError> {
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
    resolve(raw)
}

/// Parse an OpenAI backend configuration from a flat `.env`-style file.
///
/// Recognised keys: `OPENAI_BASE_URL`, `OPENAI_API_KEY`, `OPENAI_MODEL`, and
/// any `OPENAI_HEADER_<NAME>` entries (upper-cased name, `-` allowed).
pub fn openai_backend_from_env_file(
    path: &std::path::Path,
) -> Result<Option<OpenAIBackendConfig>, ConfigError> {
    let contents = std::fs::read_to_string(path).map_err(|source| ConfigError::Read {
        path: path.display().to_string(),
        source,
    })?;
    let mut base_url = None;
    let mut api_key = None;
    let mut model = None;
    let mut extra_headers = HashMap::new();
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
    resolve(RawOpenAIConfig {
        base_url,
        api_key,
        model,
        model_context_windows: HashMap::new(),
        thinking: None,
        reasoning_efforts: Vec::new(),
        default_reasoning_effort: None,
        extra_headers,
    })
}

fn resolve(raw: RawOpenAIConfig) -> Result<Option<OpenAIBackendConfig>, ConfigError> {
    let Some(base_url) = raw.base_url else {
        return Ok(None);
    };
    let base_url = expand_env(&base_url);
    if base_url.trim().is_empty() {
        return Err(ConfigError::MissingBaseUrl);
    }
    let api_key = raw
        .api_key
        .map(|key| expand_env(&key))
        .filter(|key| !key.is_empty())
        .or_else(|| std::env::var("OPENAI_API_KEY").ok().filter(|key| !key.is_empty()))
        .or_else(|| {
            std::env::var("DEEPSEEK_API_KEY")
                .ok()
                .filter(|key| !key.is_empty())
        })
        .ok_or(ConfigError::MissingApiKey)?;
    Ok(Some(OpenAIBackendConfig {
        base_url,
        api_key,
        default_model: raw.model,
        model_context_windows: raw.model_context_windows,
        thinking: raw.thinking,
        reasoning_efforts: raw.reasoning_efforts,
        default_reasoning_effort: raw.default_reasoning_effort,
        extra_headers: raw.extra_headers,
    }))
}

fn expand_env(value: &str) -> String {
    if let Some(variable) = value
        .strip_prefix("${")
        .and_then(|rest| rest.strip_suffix('}'))
        .map(str::trim)
        .filter(|variable| !variable.is_empty())
    {
        std::env::var(variable).unwrap_or_default()
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
        assert_eq!(expand_env("plain"), "plain");
        assert_eq!(expand_env(""), "");
        // A missing env reference resolves to empty rather than panicking.
        assert_eq!(expand_env("${GROK_DEFINITELY_MISSING_999}"), "");
    }

    #[test]
    fn strips_quotes_around_env_values() {
        assert_eq!(strip_quotes("\"sk-123\""), "sk-123");
        assert_eq!(strip_quotes("'sk-123'"), "sk-123");
        assert_eq!(strip_quotes("sk-123"), "sk-123");
    }

    #[test]
    fn resolve_requires_base_url_and_api_key() {
        let raw = RawOpenAIConfig {
            base_url: None,
            api_key: None,
            model: None,
            model_context_windows: HashMap::new(),
            thinking: None,
            reasoning_efforts: Vec::new(),
            default_reasoning_effort: None,
            extra_headers: HashMap::new(),
        };
        assert!(matches!(resolve(raw), Ok(None)));

        let missing_key = RawOpenAIConfig {
            base_url: Some("https://api.deepseek.com/v1".into()),
            api_key: None,
            model: None,
            model_context_windows: HashMap::new(),
            thinking: None,
            reasoning_efforts: Vec::new(),
            default_reasoning_effort: None,
            extra_headers: HashMap::new(),
        };
        assert!(matches!(resolve(missing_key), Err(ConfigError::MissingApiKey)));
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
        assert_eq!(config.base_url, "https://api.deepseek.com/v1");
        assert_eq!(config.api_key, "sk-test");
        assert_eq!(config.default_model.as_deref(), Some("deepseek-chat"));
        assert!(config.model_context_windows.is_empty());
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
        assert_eq!(config.reasoning_efforts, vec!["low".to_string(), "high".to_string(), "max".to_string()]);
        assert_eq!(config.default_reasoning_effort.as_deref(), Some("high"));
    }
}
