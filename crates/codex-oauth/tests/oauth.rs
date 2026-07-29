use std::time::{Duration, SystemTime};

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use codex_oauth::{
    Credentials, FileCredentialStore, OAuthConfig, PkceCodes, build_authorize_url,
    extract_account_id, validate_callback,
};

#[test]
fn pkce_challenge_is_the_sha256_of_the_verifier() {
    let pkce = PkceCodes::from_verifier("test-verifier").expect("valid verifier");
    assert_eq!(
        pkce.challenge,
        "JBbiqONGWPaAmwXk_8bT6UnlPfrn65D32eZlJS-zGG0"
    );
}

#[test]
fn authorize_url_uses_official_codex_parameters() {
    let config = OAuthConfig::default();
    let pkce = PkceCodes::from_verifier("test-verifier").expect("valid verifier");
    let url = build_authorize_url(
        &config,
        "http://localhost:1455/auth/callback",
        &pkce,
        "state-1",
    )
    .expect("authorize url");
    let query: std::collections::HashMap<_, _> = url.query_pairs().into_owned().collect();

    assert_eq!(
        url.as_str().split('?').next(),
        Some("https://auth.openai.com/oauth/authorize")
    );
    assert_eq!(query.get("response_type").map(String::as_str), Some("code"));
    assert_eq!(
        query.get("client_id").map(String::as_str),
        Some(config.client_id.as_str())
    );
    assert_eq!(
        query.get("code_challenge_method").map(String::as_str),
        Some("S256")
    );
    assert_eq!(query.get("state").map(String::as_str), Some("state-1"));
    assert!(
        query
            .get("scope")
            .is_some_and(|scope| scope.contains("offline_access"))
    );
    assert_eq!(
        query.get("codex_cli_simplified_flow").map(String::as_str),
        Some("true")
    );
}

#[test]
fn callback_rejects_state_mismatch() {
    let error = validate_callback(Some("code-1"), Some("attacker"), "expected")
        .expect_err("must reject CSRF");
    assert!(error.to_string().contains("state"));
}

#[test]
fn account_id_is_extracted_from_nested_auth_claim() {
    let header = URL_SAFE_NO_PAD.encode(br#"{"alg":"none"}"#);
    let payload = URL_SAFE_NO_PAD
        .encode(br#"{"https://api.openai.com/auth":{"chatgpt_account_id":"account-123"}}"#);
    let jwt = format!("{header}.{payload}.");

    assert_eq!(extract_account_id(&jwt).as_deref(), Some("account-123"));
}

#[test]
fn credentials_refresh_before_expiry_window() {
    let credentials = Credentials {
        access_token: "access".into(),
        refresh_token: "refresh".into(),
        id_token: "id".into(),
        account_id: "account".into(),
        expires_at: SystemTime::now() + Duration::from_secs(60),
    };
    assert!(credentials.needs_refresh(SystemTime::now(), Duration::from_secs(300)));
}

#[tokio::test]
async fn file_store_round_trips_and_uses_private_permissions() {
    use codex_oauth::CredentialStore;
    use std::os::unix::fs::PermissionsExt;

    let directory = tempfile::tempdir().expect("tempdir");
    let store = FileCredentialStore::new(directory.path().join("credentials.json"));
    let credentials = Credentials {
        access_token: "access-secret".into(),
        refresh_token: "refresh-secret".into(),
        id_token: "id-secret".into(),
        account_id: "account-123".into(),
        expires_at: SystemTime::UNIX_EPOCH + Duration::from_secs(2_000_000_000),
    };

    store.save(&credentials).await.expect("save");
    assert_eq!(store.load().await.expect("load"), Some(credentials));
    assert_eq!(
        std::fs::metadata(directory.path())
            .expect("dir metadata")
            .permissions()
            .mode()
            & 0o777,
        0o700
    );
    assert_eq!(
        std::fs::metadata(store.path())
            .expect("file metadata")
            .permissions()
            .mode()
            & 0o777,
        0o600
    );

    store.delete().await.expect("delete");
    assert_eq!(store.load().await.expect("load after delete"), None);
}
