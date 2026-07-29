use std::sync::Arc;

use axum::{Json, Router, extract::State, http::HeaderMap, routing::get};
use codex_models::{
    CatalogModel, ModelCatalogClient, load_cached_catalog, model_alias, parse_visible_models,
    save_cached_catalog,
};
use codex_oauth::AccessCredential;
use serde_json::{Value, json};
use tempfile::tempdir;
use tokio::{net::TcpListener, sync::Mutex};

#[test]
fn catalog_keeps_only_visible_api_models_and_sorts_by_priority() {
    let models = parse_visible_models(
        &serde_json::to_vec(&json!({
            "models": [
                {
                    "slug": "gpt-5.7",
                    "display_name": "GPT-5.7",
                    "visibility": "list",
                    "supported_in_api": true,
                    "priority": 2,
                    "context_window": 300000
                },
                {
                    "slug": "hidden-preview",
                    "display_name": "Hidden",
                    "visibility": "hide",
                    "supported_in_api": true,
                    "priority": 1
                },
                {
                    "slug": "not-in-api",
                    "display_name": "Not in API",
                    "visibility": "list",
                    "supported_in_api": false,
                    "priority": 0
                },
                {
                    "slug": "gpt-5.7-mini",
                    "display_name": "GPT-5.7 Mini",
                    "visibility": "list",
                    "supported_in_api": true,
                    "priority": 1
                }
            ]
        }))
        .expect("serialize fixture"),
    )
    .expect("parse catalog");

    assert_eq!(
        models,
        vec![
            CatalogModel {
                slug: "gpt-5.7-mini".into(),
                display_name: "GPT-5.7 Mini".into(),
                context_window: None,
            },
            CatalogModel {
                slug: "gpt-5.7".into(),
                display_name: "GPT-5.7".into(),
                context_window: Some(300_000),
            },
        ]
    );
}

#[test]
fn aliases_are_stable_and_new_slugs_get_a_safe_generic_alias() {
    assert_eq!(model_alias("gpt-5.6-sol"), "codex-sol");
    assert_eq!(model_alias("gpt-5.6-terra"), "codex-terra");
    assert_eq!(model_alias("gpt-5.7-codex"), "codex-gpt-5-7-codex");
    assert_eq!(
        model_alias("Vendor/Model Preview"),
        "codex-vendor-model-preview"
    );
}

#[tokio::test]
async fn client_uses_codex_account_auth_and_an_explicit_client_version() {
    #[derive(Clone, Default)]
    struct Captured(Arc<Mutex<Option<(HeaderMap, String)>>>);

    async fn models(
        State(captured): State<Captured>,
        headers: HeaderMap,
        uri: axum::http::Uri,
    ) -> Json<Value> {
        *captured.0.lock().await = Some((headers, uri.to_string()));
        Json(json!({
            "models": [{
                "slug": "gpt-5.7",
                "display_name": "GPT-5.7",
                "visibility": "list",
                "supported_in_api": true,
                "priority": 1
            }]
        }))
    }

    let captured = Captured::default();
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let address = listener.local_addr().expect("address");
    let app = Router::new()
        .route("/models", get(models))
        .with_state(captured.clone());
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });

    let client =
        ModelCatalogClient::for_test(format!("http://{address}/models")).expect("test client");
    let models = client
        .fetch(&AccessCredential {
            access_token: "access-secret".into(),
            account_id: "account-123".into(),
        })
        .await
        .expect("fetch");

    assert_eq!(models[0].slug, "gpt-5.7");
    let (headers, uri) = captured.0.lock().await.clone().expect("captured request");
    assert_eq!(
        headers
            .get("authorization")
            .and_then(|value| value.to_str().ok()),
        Some("Bearer access-secret")
    );
    assert_eq!(
        headers
            .get("chatgpt-account-id")
            .and_then(|value| value.to_str().ok()),
        Some("account-123")
    );
    assert_eq!(uri, "/models?client_version=test-version");
}

#[test]
fn cached_catalog_round_trips_with_private_permissions() {
    let directory = tempdir().expect("tempdir");
    let path = directory.path().join("models-cache.json");
    let models = vec![CatalogModel {
        slug: "gpt-5.7".into(),
        display_name: "GPT-5.7".into(),
        context_window: Some(300_000),
    }];

    save_cached_catalog(&path, &models).expect("save cache");
    assert_eq!(
        load_cached_catalog(&path).expect("load cache"),
        Some(models)
    );

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(path)
                .expect("metadata")
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
    }
}
