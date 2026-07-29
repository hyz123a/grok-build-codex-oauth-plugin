//! Idempotent management of the Grok Codex model configuration.

use std::{
    fs,
    io::Write,
    path::{Path, PathBuf},
};

use rand::RngCore;
use thiserror::Error;
use toml_edit::{Array, DocumentMut, InlineTable, Item, Table, Value, value};

const GROK_REASONING_EFFORTS: &[&str] =
    &["none", "minimal", "low", "medium", "high", "xhigh", "max"];

pub const MANAGED_MODEL_ALIASES: &[&str] = &[
    "codex",
    "codex-sol",
    "codex-terra",
    "codex-luna",
    "codex-gpt-5-5",
    "codex-gpt-5-2",
];

const CODEX_MODEL_CATALOG: &[(&str, &str, &str)] = &[
    ("codex-sol", "gpt-5.6-sol", "Codex GPT-5.6 Sol"),
    ("codex-terra", "gpt-5.6-terra", "Codex GPT-5.6 Terra"),
    ("codex-luna", "gpt-5.6-luna", "Codex GPT-5.6 Luna"),
    ("codex-gpt-5-5", "gpt-5.5", "Codex GPT-5.5"),
    ("codex-gpt-5-2", "gpt-5.2", "Codex GPT-5.2"),
];

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SelectableModel {
    pub alias: String,
    pub model: String,
    pub display_name: String,
    pub context_window: Option<i64>,
    pub default_reasoning_effort: Option<String>,
    pub supported_reasoning_efforts: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ModelConfig {
    pub model: String,
    pub port: u16,
    pub local_secret: String,
    pub context_window: i64,
}

impl ModelConfig {
    pub fn new(model: impl Into<String>, port: u16, local_secret: impl Into<String>) -> Self {
        Self {
            model: model.into(),
            port,
            local_secret: local_secret.into(),
            context_window: 400_000,
        }
    }
}

pub fn read_model_config(path: &Path) -> Result<Option<ModelConfig>, ConfigError> {
    let original = read_or_empty(path)?;
    if original.is_empty() {
        return Ok(None);
    }
    let document = parse(&original)?;
    let Some(codex) = document
        .get("model")
        .and_then(Item::as_table)
        .and_then(|models| models.get("codex"))
        .and_then(Item::as_table)
    else {
        return Ok(None);
    };
    let model = required_string(codex, "model")?;
    let base_url = required_string(codex, "base_url")?;
    let local_secret = required_string(codex, "api_key")?;
    let context_window = codex
        .get("context_window")
        .and_then(Item::as_integer)
        .unwrap_or(400_000);
    let port = base_url
        .strip_prefix("http://127.0.0.1:")
        .and_then(|remainder| remainder.split('/').next())
        .and_then(|port| port.parse().ok())
        .ok_or(ConfigError::InvalidManagedBaseUrl)?;
    Ok(Some(ModelConfig {
        model,
        port,
        local_secret,
        context_window,
    }))
}

pub fn install_model(path: &Path, config: &ModelConfig) -> Result<(), ConfigError> {
    let original = read_or_empty(path)?;
    let mut document = parse(&original)?;
    ensure_table(&mut document, "model")?;
    let models = document["model"]
        .as_table_mut()
        .ok_or(ConfigError::ConflictingValue("model"))?;
    if !models.contains_key("codex") {
        models.insert("codex", Item::Table(Table::new()));
    }
    let codex = models["codex"]
        .as_table_mut()
        .ok_or(ConfigError::ConflictingValue("model.codex"))?;
    codex["model"] = value(&config.model);
    codex["base_url"] = value(format!("http://127.0.0.1:{}/v1", config.port));
    codex["name"] = value("Codex via ChatGPT OAuth");
    codex["api_backend"] = value("responses");
    codex["api_key"] = value(&config.local_secret);
    codex["context_window"] = value(config.context_window);

    persist_if_changed(path, &original, &document.to_string())
}

pub fn install_model_catalog(path: &Path, config: &ModelConfig) -> Result<(), ConfigError> {
    let catalog = CODEX_MODEL_CATALOG
        .iter()
        .map(|(alias, model, display_name)| SelectableModel {
            alias: (*alias).into(),
            model: (*model).into(),
            display_name: (*display_name).into(),
            context_window: None,
            default_reasoning_effort: None,
            supported_reasoning_efforts: Vec::new(),
        })
        .collect::<Vec<_>>();
    replace_model_catalog(path, config, &catalog, &[]).map(|_| ())
}

pub fn replace_model_catalog(
    path: &Path,
    config: &ModelConfig,
    catalog: &[SelectableModel],
    previously_managed: &[String],
) -> Result<Vec<String>, ConfigError> {
    let original = read_or_empty(path)?;
    let mut document = parse(&original)?;
    ensure_table(&mut document, "model")?;
    let models = document["model"]
        .as_table_mut()
        .ok_or(ConfigError::ConflictingValue("model"))?;
    for alias in previously_managed {
        if alias != "codex" {
            models.remove(alias);
        }
    }
    let default_model = SelectableModel {
        alias: "codex".into(),
        model: config.model.clone(),
        display_name: "Codex via ChatGPT OAuth".into(),
        context_window: None,
        default_reasoning_effort: None,
        supported_reasoning_efforts: Vec::new(),
    };
    upsert_model_table(models, &default_model, config)?;
    let mut installed = vec!["codex".to_string()];
    for selectable in catalog {
        if selectable.alias == "codex" || installed.contains(&selectable.alias) {
            continue;
        }
        upsert_model_table(models, selectable, config)?;
        installed.push(selectable.alias.clone());
    }
    persist_if_changed(path, &original, &document.to_string())?;
    Ok(installed)
}

pub fn uninstall_model(path: &Path) -> Result<(), ConfigError> {
    let aliases = MANAGED_MODEL_ALIASES
        .iter()
        .map(|alias| (*alias).to_string())
        .collect::<Vec<_>>();
    uninstall_model_aliases(path, &aliases)
}

pub fn uninstall_model_aliases(path: &Path, aliases: &[String]) -> Result<(), ConfigError> {
    let original = read_or_empty(path)?;
    if original.is_empty() {
        return Ok(());
    }
    let mut document = parse(&original)?;
    if let Some(models) = document.get_mut("model").and_then(Item::as_table_mut) {
        for alias in aliases {
            models.remove(alias);
        }
        if models.is_empty() {
            document.remove("model");
        }
    }
    persist_if_changed(path, &original, &document.to_string())
}

fn upsert_model_table(
    models: &mut Table,
    selectable: &SelectableModel,
    config: &ModelConfig,
) -> Result<(), ConfigError> {
    let alias = &selectable.alias;
    if !models.contains_key(alias) {
        models.insert(alias, Item::Table(Table::new()));
    }
    let entry = models[alias]
        .as_table_mut()
        .ok_or_else(|| ConfigError::ConflictingModel(alias.to_string()))?;
    entry["model"] = value(&selectable.model);
    entry["base_url"] = value(format!("http://127.0.0.1:{}/v1", config.port));
    entry["name"] = value(&selectable.display_name);
    entry["api_backend"] = value("responses");
    entry["api_key"] = value(&config.local_secret);
    entry["context_window"] = value(selectable.context_window.unwrap_or(config.context_window));
    write_reasoning_config(
        entry,
        selectable.default_reasoning_effort.as_deref(),
        &selectable.supported_reasoning_efforts,
    );
    Ok(())
}

fn write_reasoning_config(
    entry: &mut Table,
    default_reasoning_effort: Option<&str>,
    supported_reasoning_efforts: &[String],
) {
    let supported = supported_reasoning_efforts
        .iter()
        .map(String::as_str)
        .filter(|effort| GROK_REASONING_EFFORTS.contains(effort))
        .collect::<Vec<_>>();
    if supported.is_empty() {
        entry.remove("supports_reasoning_effort");
        entry.remove("reasoning_effort");
        entry.remove("reasoning_efforts");
        return;
    }

    let default = default_reasoning_effort
        .filter(|effort| supported.contains(effort))
        .unwrap_or(supported[0]);
    entry["supports_reasoning_effort"] = value(true);
    entry["reasoning_effort"] = value(default);
    let mut options = Array::new();
    for effort in supported {
        let mut option = InlineTable::new();
        option.insert("id", Value::from(effort));
        option.insert("value", Value::from(effort));
        option.insert("label", Value::from(reasoning_effort_label(effort)));
        option.insert("default", Value::from(effort == default));
        options.push(option);
    }
    entry["reasoning_efforts"] = value(options);
}

fn reasoning_effort_label(effort: &str) -> &'static str {
    match effort {
        "none" => "None",
        "minimal" => "Minimal",
        "low" => "Low",
        "medium" => "Medium",
        "high" => "High",
        "xhigh" => "X high",
        "max" => "Max",
        _ => "Custom",
    }
}

fn ensure_table(document: &mut DocumentMut, key: &'static str) -> Result<(), ConfigError> {
    if !document.contains_key(key) {
        document.insert(key, Item::Table(Table::new()));
    }
    if !document[key].is_table() {
        return Err(ConfigError::ConflictingValue(key));
    }
    Ok(())
}

fn required_string(table: &Table, key: &'static str) -> Result<String, ConfigError> {
    table
        .get(key)
        .and_then(Item::as_str)
        .map(str::to_owned)
        .ok_or(ConfigError::MissingManagedField(key))
}

fn read_or_empty(path: &Path) -> Result<String, ConfigError> {
    match fs::read_to_string(path) {
        Ok(content) => Ok(content),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(String::new()),
        Err(error) => Err(error.into()),
    }
}

fn parse(content: &str) -> Result<DocumentMut, ConfigError> {
    if content.trim().is_empty() {
        Ok(DocumentMut::new())
    } else {
        content.parse().map_err(ConfigError::InvalidToml)
    }
}

fn persist_if_changed(path: &Path, original: &str, updated: &str) -> Result<(), ConfigError> {
    if original == updated {
        return Ok(());
    }
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    if path.exists() {
        let backup = backup_path(path);
        fs::copy(path, backup)?;
    }
    atomic_write(path, updated.as_bytes())
}

fn backup_path(path: &Path) -> PathBuf {
    let mut extension = path.extension().unwrap_or_default().to_os_string();
    extension.push(".bak");
    path.with_extension(extension)
}

fn atomic_write(path: &Path, contents: &[u8]) -> Result<(), ConfigError> {
    let parent = path.parent().ok_or(ConfigError::MissingParent)?;
    let mut random = [0_u8; 12];
    rand::rng().fill_bytes(&mut random);
    let suffix = random
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let temp = parent.join(format!(".config-{suffix}.tmp"));
    let mut options = fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(&temp)?;
    let result = (|| -> Result<(), std::io::Error> {
        file.write_all(contents)?;
        file.sync_all()?;
        fs::rename(&temp, path)?;
        fs::File::open(parent)?.sync_all()
    })();
    if result.is_err() {
        let _ = fs::remove_file(temp);
    }
    result.map_err(ConfigError::Io)
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("Grok config contains a non-table value at `{0}`")]
    ConflictingValue(&'static str),
    #[error("Grok config contains a non-table model entry at `{0}`")]
    ConflictingModel(String),
    #[error("Grok config path has no parent")]
    MissingParent,
    #[error("managed Grok model is missing string field `{0}`")]
    MissingManagedField(&'static str),
    #[error("managed Grok model has an invalid loopback base URL")]
    InvalidManagedBaseUrl,
    #[error("Grok config is invalid TOML")]
    InvalidToml(#[source] toml_edit::TomlError),
    #[error("Grok config I/O failed")]
    Io(#[from] std::io::Error),
}
