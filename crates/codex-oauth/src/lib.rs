//! OAuth and credential management for ChatGPT-backed Codex access.

use std::{
    fmt,
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use rand::RngCore;
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use thiserror::Error;
use url::Url;

pub const DEFAULT_ISSUER: &str = "https://auth.openai.com";
pub const DEFAULT_CLIENT_ID: &str = "app_EMoamEEZ73f0CkXaXp7hrann";
pub const DEFAULT_SCOPE: &str =
    "openid profile email offline_access api.connectors.read api.connectors.invoke";

#[derive(Clone, Debug)]
pub struct OAuthConfig {
    pub issuer: String,
    pub client_id: String,
    pub scope: String,
}

impl Default for OAuthConfig {
    fn default() -> Self {
        Self {
            issuer: DEFAULT_ISSUER.into(),
            client_id: DEFAULT_CLIENT_ID.into(),
            scope: DEFAULT_SCOPE.into(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PkceCodes {
    pub verifier: String,
    pub challenge: String,
}

impl PkceCodes {
    pub fn generate() -> Self {
        let mut bytes = [0_u8; 64];
        rand::rng().fill_bytes(&mut bytes);
        let verifier = URL_SAFE_NO_PAD.encode(bytes);
        // A generated verifier is always valid input for SHA-256.
        Self::from_verifier(&verifier).unwrap_or_else(|_| Self {
            verifier,
            challenge: String::new(),
        })
    }

    pub fn from_verifier(verifier: &str) -> Result<Self, OAuthError> {
        if verifier.is_empty() {
            return Err(OAuthError::InvalidPkceVerifier);
        }
        let challenge = URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()));
        Ok(Self {
            verifier: verifier.into(),
            challenge,
        })
    }
}

pub fn generate_state() -> String {
    let mut bytes = [0_u8; 32];
    rand::rng().fill_bytes(&mut bytes);
    URL_SAFE_NO_PAD.encode(bytes)
}

pub fn build_authorize_url(
    config: &OAuthConfig,
    redirect_uri: &str,
    pkce: &PkceCodes,
    state: &str,
) -> Result<Url, OAuthError> {
    let mut url = Url::parse(&format!(
        "{}/oauth/authorize",
        config.issuer.trim_end_matches('/')
    ))?;
    url.query_pairs_mut()
        .append_pair("response_type", "code")
        .append_pair("client_id", &config.client_id)
        .append_pair("redirect_uri", redirect_uri)
        .append_pair("scope", &config.scope)
        .append_pair("code_challenge", &pkce.challenge)
        .append_pair("code_challenge_method", "S256")
        .append_pair("id_token_add_organizations", "true")
        .append_pair("codex_cli_simplified_flow", "true")
        .append_pair("state", state)
        .append_pair("originator", "grok-build");
    Ok(url)
}

pub fn validate_callback(
    code: Option<&str>,
    returned_state: Option<&str>,
    expected_state: &str,
) -> Result<String, OAuthError> {
    if returned_state != Some(expected_state) {
        return Err(OAuthError::StateMismatch);
    }
    code.filter(|code| !code.is_empty())
        .map(str::to_owned)
        .ok_or(OAuthError::MissingAuthorizationCode)
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Credentials {
    pub access_token: String,
    pub refresh_token: String,
    pub id_token: String,
    pub account_id: String,
    #[serde(with = "system_time_seconds")]
    pub expires_at: SystemTime,
}

impl fmt::Debug for Credentials {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Credentials")
            .field("access_token", &"<redacted>")
            .field("refresh_token", &"<redacted>")
            .field("id_token", &"<redacted>")
            .field("account_id", &"<redacted>")
            .field("expires_at", &self.expires_at)
            .finish()
    }
}

impl Credentials {
    pub fn needs_refresh(&self, now: SystemTime, refresh_window: Duration) -> bool {
        now.checked_add(refresh_window)
            .is_none_or(|threshold| threshold >= self.expires_at)
    }

    pub fn access_credential(&self) -> AccessCredential {
        AccessCredential {
            access_token: self.access_token.clone(),
            account_id: self.account_id.clone(),
        }
    }
}

#[derive(Clone)]
pub struct AccessCredential {
    pub access_token: String,
    pub account_id: String,
}

impl fmt::Debug for AccessCredential {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AccessCredential")
            .field("access_token", &"<redacted>")
            .field("account_id", &"<redacted>")
            .finish()
    }
}

#[async_trait]
pub trait CredentialStore: Send + Sync {
    async fn load(&self) -> Result<Option<Credentials>, CredentialStoreError>;
    async fn save(&self, credentials: &Credentials) -> Result<(), CredentialStoreError>;
    async fn delete(&self) -> Result<(), CredentialStoreError>;
}

#[derive(Clone, Debug)]
pub struct FileCredentialStore {
    path: PathBuf,
}

/// Prefer the operating-system credential vault and transparently fall back to
/// the private atomic file store when no usable vault is available.
#[derive(Clone, Debug)]
pub struct SystemCredentialStore {
    service: String,
    user: String,
    fallback: FileCredentialStore,
}

impl SystemCredentialStore {
    pub fn new(
        service: impl Into<String>,
        user: impl Into<String>,
        fallback_path: PathBuf,
    ) -> Self {
        Self {
            service: service.into(),
            user: user.into(),
            fallback: FileCredentialStore::new(fallback_path),
        }
    }

    pub fn fallback_path(&self) -> &Path {
        self.fallback.path()
    }

    fn entry(&self) -> Result<keyring::Entry, keyring::Error> {
        keyring::Entry::new(&self.service, &self.user)
    }
}

#[async_trait]
impl CredentialStore for SystemCredentialStore {
    async fn load(&self) -> Result<Option<Credentials>, CredentialStoreError> {
        if let Ok(entry) = self.entry()
            && let Ok(encoded) = entry.get_password()
        {
            return serde_json::from_str(&encoded)
                .map(Some)
                .map_err(CredentialStoreError::Json);
        }
        self.fallback.load().await
    }

    async fn save(&self, credentials: &Credentials) -> Result<(), CredentialStoreError> {
        let encoded = serde_json::to_string(credentials)?;
        if let Ok(entry) = self.entry()
            && entry.set_password(&encoded).is_ok()
        {
            self.fallback.delete().await?;
            return Ok(());
        }
        self.fallback.save(credentials).await
    }

    async fn delete(&self) -> Result<(), CredentialStoreError> {
        if let Ok(entry) = self.entry() {
            match entry.delete_credential() {
                Ok(()) | Err(keyring::Error::NoEntry) => {}
                Err(_) => {
                    // Continue deleting the fallback. Logout remains idempotent
                    // even on hosts where the keyring service is unavailable.
                }
            }
        }
        self.fallback.delete().await
    }
}

impl FileCredentialStore {
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[async_trait]
impl CredentialStore for FileCredentialStore {
    async fn load(&self) -> Result<Option<Credentials>, CredentialStoreError> {
        match fs::read(&self.path) {
            Ok(contents) => Ok(Some(serde_json::from_slice(&contents)?)),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    async fn save(&self, credentials: &Credentials) -> Result<(), CredentialStoreError> {
        let parent = self
            .path
            .parent()
            .ok_or(CredentialStoreError::MissingParent)?;
        fs::create_dir_all(parent)?;
        set_private_directory_permissions(parent)?;

        let suffix = generate_state();
        let temp_path = parent.join(format!(".credentials-{suffix}.tmp"));
        let encoded = serde_json::to_vec(credentials)?;
        let result = write_private_atomic(&temp_path, &self.path, &encoded);
        if result.is_err() {
            let _ = fs::remove_file(&temp_path);
        }
        result
    }

    async fn delete(&self) -> Result<(), CredentialStoreError> {
        match fs::remove_file(&self.path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }
}

fn write_private_atomic(
    temp_path: &Path,
    destination: &Path,
    contents: &[u8],
) -> Result<(), CredentialStoreError> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(temp_path)?;
    file.write_all(contents)?;
    file.sync_all()?;
    fs::rename(temp_path, destination)?;
    set_private_file_permissions(destination)?;
    if let Some(parent) = destination.parent() {
        let directory = fs::File::open(parent)?;
        directory.sync_all()?;
    }
    Ok(())
}

#[cfg(unix)]
fn set_private_directory_permissions(path: &Path) -> Result<(), CredentialStoreError> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    Ok(())
}

#[cfg(not(unix))]
fn set_private_directory_permissions(_path: &Path) -> Result<(), CredentialStoreError> {
    Ok(())
}

#[cfg(unix)]
fn set_private_file_permissions(path: &Path) -> Result<(), CredentialStoreError> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    Ok(())
}

#[cfg(not(unix))]
fn set_private_file_permissions(_path: &Path) -> Result<(), CredentialStoreError> {
    Ok(())
}

pub fn extract_account_id(jwt: &str) -> Option<String> {
    let claims = decode_jwt_payload(jwt)?;
    claims
        .get("https://api.openai.com/auth")
        .and_then(Value::as_object)
        .and_then(|auth| auth.get("chatgpt_account_id"))
        .and_then(Value::as_str)
        .or_else(|| claims.get("chatgpt_account_id").and_then(Value::as_str))
        .map(str::to_owned)
}

fn jwt_expiry(jwt: &str) -> Option<SystemTime> {
    let seconds = decode_jwt_payload(jwt)?.get("exp")?.as_u64()?;
    UNIX_EPOCH.checked_add(Duration::from_secs(seconds))
}

fn decode_jwt_payload(jwt: &str) -> Option<Value> {
    let mut parts = jwt.split('.');
    let _header = parts.next()?;
    let payload = parts.next()?;
    let _signature = parts.next()?;
    let decoded = URL_SAFE_NO_PAD.decode(payload).ok()?;
    serde_json::from_slice(&decoded).ok()
}

#[derive(Clone)]
pub struct OAuthClient {
    config: OAuthConfig,
    client: reqwest::Client,
}

impl OAuthClient {
    pub fn new(config: OAuthConfig) -> Self {
        Self {
            config,
            client: reqwest::Client::new(),
        }
    }

    pub fn with_client(config: OAuthConfig, client: reqwest::Client) -> Self {
        Self { config, client }
    }

    pub async fn exchange_code(
        &self,
        code: &str,
        redirect_uri: &str,
        pkce: &PkceCodes,
    ) -> Result<Credentials, OAuthError> {
        let endpoint = format!("{}/oauth/token", self.config.issuer.trim_end_matches('/'));
        let response = self
            .client
            .post(endpoint)
            .form(&[
                ("grant_type", "authorization_code"),
                ("code", code),
                ("redirect_uri", redirect_uri),
                ("client_id", self.config.client_id.as_str()),
                ("code_verifier", pkce.verifier.as_str()),
            ])
            .send()
            .await?;
        parse_token_response(response, None).await
    }

    pub async fn refresh(&self, current: &Credentials) -> Result<Credentials, OAuthError> {
        #[derive(Serialize)]
        struct RefreshRequest<'a> {
            client_id: &'a str,
            grant_type: &'static str,
            refresh_token: &'a str,
        }
        let endpoint = format!("{}/oauth/token", self.config.issuer.trim_end_matches('/'));
        let response = self
            .client
            .post(endpoint)
            .json(&RefreshRequest {
                client_id: &self.config.client_id,
                grant_type: "refresh_token",
                refresh_token: &current.refresh_token,
            })
            .send()
            .await?;
        parse_token_response(response, Some(current)).await
    }

    pub async fn request_device_code(&self) -> Result<DeviceCode, OAuthError> {
        #[derive(Serialize)]
        struct Request<'a> {
            client_id: &'a str,
        }
        let endpoint = format!(
            "{}/api/accounts/deviceauth/usercode",
            self.config.issuer.trim_end_matches('/')
        );
        let response = self
            .client
            .post(endpoint)
            .json(&Request {
                client_id: &self.config.client_id,
            })
            .send()
            .await?;
        let status = response.status();
        if !status.is_success() {
            return Err(OAuthError::DeviceCodeEndpoint(status));
        }
        let value: Value = response.json().await?;
        let device_auth_id = required_string(&value, "device_auth_id")?;
        let user_code = value
            .get("user_code")
            .or_else(|| value.get("usercode"))
            .and_then(Value::as_str)
            .map(str::to_owned)
            .ok_or(OAuthError::MissingTokenField("user_code"))?;
        let interval = value
            .get("interval")
            .and_then(|value| {
                value
                    .as_u64()
                    .or_else(|| value.as_str().and_then(|text| text.parse().ok()))
            })
            .unwrap_or(5);
        Ok(DeviceCode {
            verification_url: format!("{}/codex/device", self.config.issuer.trim_end_matches('/')),
            user_code,
            device_auth_id,
            interval,
        })
    }

    pub async fn complete_device_code(
        &self,
        device_code: &DeviceCode,
    ) -> Result<Credentials, OAuthError> {
        #[derive(Serialize)]
        struct PollRequest<'a> {
            device_auth_id: &'a str,
            user_code: &'a str,
        }
        let endpoint = format!(
            "{}/api/accounts/deviceauth/token",
            self.config.issuer.trim_end_matches('/')
        );
        let deadline = tokio::time::Instant::now() + Duration::from_secs(15 * 60);
        loop {
            let response = self
                .client
                .post(&endpoint)
                .json(&PollRequest {
                    device_auth_id: &device_code.device_auth_id,
                    user_code: &device_code.user_code,
                })
                .send()
                .await?;
            if response.status().is_success() {
                let value: Value = response.json().await?;
                let code = required_string(&value, "authorization_code")?;
                let verifier = required_string(&value, "code_verifier")?;
                let pkce = PkceCodes::from_verifier(&verifier)?;
                let redirect_uri = format!(
                    "{}/deviceauth/callback",
                    self.config.issuer.trim_end_matches('/')
                );
                return self.exchange_code(&code, &redirect_uri, &pkce).await;
            }
            if !matches!(
                response.status(),
                StatusCode::FORBIDDEN | StatusCode::NOT_FOUND
            ) {
                return Err(OAuthError::DeviceCodeEndpoint(response.status()));
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(OAuthError::DeviceCodeTimeout);
            }
            tokio::time::sleep(Duration::from_secs(device_code.interval.max(1))).await;
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeviceCode {
    pub verification_url: String,
    pub user_code: String,
    device_auth_id: String,
    interval: u64,
}

fn required_string(value: &Value, field: &'static str) -> Result<String, OAuthError> {
    value
        .get(field)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or(OAuthError::MissingTokenField(field))
}

#[derive(Deserialize)]
struct TokenResponse {
    access_token: Option<String>,
    refresh_token: Option<String>,
    id_token: Option<String>,
    expires_in: Option<u64>,
}

async fn parse_token_response(
    response: reqwest::Response,
    current: Option<&Credentials>,
) -> Result<Credentials, OAuthError> {
    let status = response.status();
    if !status.is_success() {
        return Err(OAuthError::TokenEndpoint {
            status,
            message: sanitized_oauth_error(response).await,
        });
    }
    let tokens: TokenResponse = response.json().await?;
    let access_token = tokens
        .access_token
        .ok_or(OAuthError::MissingTokenField("access_token"))?;
    let refresh_token = tokens
        .refresh_token
        .or_else(|| current.map(|value| value.refresh_token.clone()))
        .ok_or(OAuthError::MissingTokenField("refresh_token"))?;
    let id_token = tokens
        .id_token
        .or_else(|| current.map(|value| value.id_token.clone()))
        .ok_or(OAuthError::MissingTokenField("id_token"))?;
    let account_id = extract_account_id(&id_token)
        .or_else(|| current.map(|value| value.account_id.clone()))
        .ok_or(OAuthError::MissingAccountId)?;
    let expires_at = jwt_expiry(&access_token).unwrap_or_else(|| {
        SystemTime::now() + Duration::from_secs(tokens.expires_in.unwrap_or(3600))
    });
    Ok(Credentials {
        access_token,
        refresh_token,
        id_token,
        account_id,
        expires_at,
    })
}

async fn sanitized_oauth_error(response: reqwest::Response) -> String {
    let body = response.text().await.unwrap_or_default();
    serde_json::from_str::<Value>(&body)
        .ok()
        .and_then(|value| {
            value
                .pointer("/error/message")
                .or_else(|| value.get("error_description"))
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .unwrap_or_else(|| "OAuth token request failed".into())
}

#[derive(Debug, Error)]
pub enum OAuthError {
    #[error("invalid PKCE verifier")]
    InvalidPkceVerifier,
    #[error("OAuth callback state did not match")]
    StateMismatch,
    #[error("OAuth callback did not contain an authorization code")]
    MissingAuthorizationCode,
    #[error("OAuth token response omitted `{0}`")]
    MissingTokenField(&'static str),
    #[error("OAuth token did not contain a ChatGPT account ID")]
    MissingAccountId,
    #[error("OAuth token endpoint returned {status}: {message}")]
    TokenEndpoint { status: StatusCode, message: String },
    #[error("device-code endpoint returned {0}")]
    DeviceCodeEndpoint(StatusCode),
    #[error("device-code login timed out after 15 minutes")]
    DeviceCodeTimeout,
    #[error(transparent)]
    Url(#[from] url::ParseError),
    #[error("OAuth request failed")]
    Request(#[source] reqwest::Error),
}

impl From<reqwest::Error> for OAuthError {
    fn from(error: reqwest::Error) -> Self {
        Self::Request(error)
    }
}

#[derive(Debug, Error)]
pub enum CredentialStoreError {
    #[error("credential path has no parent directory")]
    MissingParent,
    #[error("credential storage I/O failed")]
    Io(#[from] std::io::Error),
    #[error("credential file is invalid")]
    Json(#[from] serde_json::Error),
}

mod system_time_seconds {
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(value: &SystemTime, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let seconds = value
            .duration_since(UNIX_EPOCH)
            .map_err(serde::ser::Error::custom)?
            .as_secs();
        serializer.serialize_u64(seconds)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<SystemTime, D::Error>
    where
        D: Deserializer<'de>,
    {
        let seconds = u64::deserialize(deserializer)?;
        Ok(UNIX_EPOCH + Duration::from_secs(seconds))
    }
}
