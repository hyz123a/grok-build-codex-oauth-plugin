//! Account-aware discovery and caching of models exposed by the ChatGPT Codex backend.

use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

use codex_oauth::AccessCredential;
use rand::RngCore;
use reqwest::header::{AUTHORIZATION, HeaderValue};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use url::Url;

pub const CODEX_MODELS_URL: &str = "https://chatgpt.com/backend-api/codex/models";

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct CatalogModel {
    pub slug: String,
    pub display_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_window: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_reasoning_effort: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub supported_reasoning_efforts: Vec<String>,
    /// When set (e.g. `"codex/"`), the Grok `model` value sent to the proxy is
    /// `{route_prefix}{slug}`. The proxy routes any request whose model starts
    /// with this prefix to the built-in Codex backend (the signed-in ChatGPT
    /// account) and strips the prefix before forwarding upstream. OpenAI
    /// compatible-provider models leave this `None`; their routing prefix is
    /// already embedded in `slug`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub route_prefix: Option<String>,
}

#[derive(Deserialize)]
struct ModelsResponse {
    models: Vec<RemoteModel>,
}

#[derive(Deserialize)]
struct RemoteModel {
    slug: String,
    display_name: String,
    visibility: String,
    #[serde(default)]
    supported_in_api: bool,
    #[serde(default)]
    priority: i32,
    #[serde(default)]
    context_window: Option<i64>,
    #[serde(default)]
    default_reasoning_level: Option<String>,
    #[serde(default)]
    supported_reasoning_levels: Vec<RemoteReasoningLevel>,
}

#[derive(Deserialize)]
struct RemoteReasoningLevel {
    effort: String,
}

pub fn parse_visible_models(bytes: &[u8]) -> Result<Vec<CatalogModel>, CatalogError> {
    let mut models = serde_json::from_slice::<ModelsResponse>(bytes)?
        .models
        .into_iter()
        .filter(|model| model.visibility == "list" && model.supported_in_api)
        .collect::<Vec<_>>();
    models.sort_by(|left, right| {
        left.priority
            .cmp(&right.priority)
            .then_with(|| left.slug.cmp(&right.slug))
    });
    Ok(models
        .into_iter()
        .map(|model| CatalogModel {
            slug: model.slug,
            display_name: model.display_name,
            context_window: model.context_window,
            default_reasoning_effort: model.default_reasoning_level,
            supported_reasoning_efforts: model
                .supported_reasoning_levels
                .into_iter()
                .map(|level| level.effort)
                .collect(),
            route_prefix: None,
        })
        .collect())
}

pub fn model_alias(slug: &str) -> String {
    match slug {
        "gpt-5.6-sol" => return "codex-sol".into(),
        "gpt-5.6-terra" => return "codex-terra".into(),
        "gpt-5.6-luna" => return "codex-luna".into(),
        _ => {}
    }

    let normalized = slug
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() {
                character.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect::<String>()
        .split('-')
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join("-");
    if normalized.is_empty() {
        "codex-model".into()
    } else {
        format!("codex-{normalized}")
    }
}

#[derive(Clone)]
pub struct ModelCatalogClient {
    client: reqwest::Client,
    endpoint: Url,
}

impl ModelCatalogClient {
    pub fn production(client_version: &str) -> Result<Self, CatalogError> {
        Self::new(CODEX_MODELS_URL, Some(client_version))
    }

    #[doc(hidden)]
    pub fn for_test(endpoint: impl AsRef<str>) -> Result<Self, CatalogError> {
        Self::new(endpoint.as_ref(), Some("test-version"))
    }

    fn new(endpoint: &str, client_version: Option<&str>) -> Result<Self, CatalogError> {
        let mut endpoint = Url::parse(endpoint)?;
        if let Some(client_version) = client_version {
            endpoint
                .query_pairs_mut()
                .append_pair("client_version", client_version);
        }
        Ok(Self {
            client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(10))
                .build()?,
            endpoint,
        })
    }

    pub async fn fetch(
        &self,
        credential: &AccessCredential,
    ) -> Result<Vec<CatalogModel>, CatalogError> {
        let response = self
            .client
            .get(self.endpoint.clone())
            .header(
                AUTHORIZATION,
                HeaderValue::from_str(&format!("Bearer {}", credential.access_token))
                    .map_err(|_| CatalogError::InvalidCredentialHeader)?,
            )
            .header(
                "chatgpt-account-id",
                HeaderValue::from_str(&credential.account_id)
                    .map_err(|_| CatalogError::InvalidCredentialHeader)?,
            )
            .header("originator", "grok-build")
            .send()
            .await?;
        let status = response.status();
        if !status.is_success() {
            return Err(CatalogError::Endpoint(status));
        }
        parse_visible_models(&response.bytes().await?)
    }
}

pub fn load_cached_catalog(path: &Path) -> Result<Option<Vec<CatalogModel>>, CatalogError> {
    match fs::read(path) {
        Ok(bytes) => Ok(Some(serde_json::from_slice(&bytes)?)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

pub fn save_cached_catalog(path: &Path, models: &[CatalogModel]) -> Result<(), CatalogError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let encoded = serde_json::to_vec_pretty(models)?;
    let temporary = temporary_path(path);
    let result = (|| -> Result<(), CatalogError> {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)?;
        set_private_file_permissions(&temporary)?;
        file.write_all(&encoded)?;
        file.sync_all()?;
        fs::rename(&temporary, path)?;
        set_private_file_permissions(path)?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

fn temporary_path(path: &Path) -> PathBuf {
    let mut random = [0_u8; 8];
    rand::rng().fill_bytes(&mut random);
    let suffix = random
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    path.with_extension(format!("tmp-{suffix}"))
}

#[cfg(unix)]
fn set_private_file_permissions(path: &Path) -> Result<(), std::io::Error> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))
}

#[cfg(not(unix))]
fn set_private_file_permissions(_path: &Path) -> Result<(), std::io::Error> {
    Ok(())
}

#[derive(Debug, Error)]
pub enum CatalogError {
    #[error("{0}")]
    Authentication(String),
    #[error("model catalog request failed: HTTP {0}")]
    Endpoint(reqwest::StatusCode),
    #[error("OAuth credential contains invalid header characters")]
    InvalidCredentialHeader,
    #[error("model catalog JSON is invalid: {0}")]
    Json(#[from] serde_json::Error),
    #[error("model catalog HTTP request failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("model catalog URL is invalid: {0}")]
    Url(#[from] url::ParseError),
    #[error("model catalog cache I/O failed: {0}")]
    Io(#[from] std::io::Error),
}
