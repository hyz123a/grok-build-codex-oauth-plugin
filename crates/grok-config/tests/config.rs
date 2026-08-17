use grok_config::{
    ModelConfig, SelectableModel, install_model, install_model_catalog, read_model_config,
    replace_model_catalog, uninstall_model, uninstall_model_aliases,
};

#[test]
fn setup_is_idempotent_and_preserves_unrelated_configuration() {
    let directory = tempfile::tempdir().expect("tempdir");
    let path = directory.path().join("config.toml");
    std::fs::write(
        &path,
        "theme = \"dark\"\n\n[model.other]\nmodel = \"grok\"\n",
    )
    .expect("seed config");
    let managed = ModelConfig::new("gpt-5.4", 1456, "secret-1");

    install_model(&path, &managed).expect("first setup");
    let first = std::fs::read_to_string(&path).expect("read first");
    install_model(&path, &managed).expect("second setup");
    let second = std::fs::read_to_string(&path).expect("read second");

    assert_eq!(first, second);
    assert!(second.contains("theme = \"dark\""));
    assert!(second.contains("[model.other]"));
    assert!(second.contains("[model.codex]"));
    assert!(second.contains("base_url = \"http://127.0.0.1:1456/v1\""));
    assert!(second.contains("api_key = \"secret-1\""));
    assert!(second.contains("supports_backend_search = true"));
    assert!(
        !second.contains("web_search"),
        "the plugin must not pin a web_search model"
    );
    assert!(path.with_extension("toml.bak").exists());
}

#[test]
fn uninstall_removes_only_the_managed_model() {
    let directory = tempfile::tempdir().expect("tempdir");
    let path = directory.path().join("config.toml");
    std::fs::write(
        &path,
        "[model.codex]\nmodel = \"gpt-5.4\"\n\n[model.other]\nmodel = \"grok\"\n",
    )
    .expect("seed config");

    uninstall_model(&path).expect("uninstall");
    let result = std::fs::read_to_string(&path).expect("read");

    assert!(!result.contains("[model.codex]"));
    assert!(result.contains("[model.other]"));
}

#[test]
fn catalog_installs_selectable_codex_model_aliases() {
    let directory = tempfile::tempdir().expect("tempdir");
    let path = directory.path().join("config.toml");
    let managed = ModelConfig::new("gpt-5.4", 1456, "secret-1");

    install_model_catalog(&path, &managed).expect("install catalog");
    let result = std::fs::read_to_string(&path).expect("read");

    assert!(!result.contains("[model.codex]"));
    assert!(result.contains("context_window = 272000"));
    for (alias, model) in [
        ("codex-sol", "gpt-5.6-sol"),
        ("codex-terra", "gpt-5.6-terra"),
        ("codex-luna", "gpt-5.6-luna"),
        ("codex-gpt-5-5", "gpt-5.5"),
        ("codex-gpt-5-2", "gpt-5.2"),
    ] {
        assert!(result.contains(&format!("[model.{alias}]")));
        assert!(result.contains(&format!("model = \"{model}\"")));
    }
    assert!(result.contains("supports_backend_search = true"));
    assert!(
        !result.contains("web_search"),
        "the plugin must not pin a web_search model"
    );
}

#[test]
fn catalog_preserves_an_explicit_non_codex_web_search_model() {
    let directory = tempfile::tempdir().expect("tempdir");
    let path = directory.path().join("config.toml");
    std::fs::write(&path, "[models]\nweb_search = \"my-search\"\n").expect("seed config");

    install_model_catalog(&path, &ModelConfig::new("gpt-5.6-sol", 1456, "secret"))
        .expect("install catalog");

    let result = std::fs::read_to_string(path).expect("read");
    assert!(result.contains("web_search = \"my-search\""));
}

#[test]
fn uninstall_catalog_preserves_similarly_named_user_model() {
    let directory = tempfile::tempdir().expect("tempdir");
    let path = directory.path().join("config.toml");
    std::fs::write(
        &path,
        "[model.codex-sol]\nmodel = \"gpt-5.6-sol\"\n\n[model.codex-custom]\nmodel = \"mine\"\n",
    )
    .expect("seed config");

    uninstall_model(&path).expect("uninstall");
    let result = std::fs::read_to_string(&path).expect("read");

    assert!(!result.contains("[model.codex-sol]"));
    assert!(result.contains("[model.codex-custom]"));
}

#[test]
fn dynamic_catalog_replaces_stale_managed_aliases_and_preserves_user_models() {
    let directory = tempfile::tempdir().expect("tempdir");
    let path = directory.path().join("config.toml");
    std::fs::write(
        &path,
        "[model.codex]\nmodel = \"gpt-5.4\"\n\n[model.codex-old]\nmodel = \"old\"\n\n[model.codex-user]\nmodel = \"mine\"\n",
    )
    .expect("seed config");
    let config = ModelConfig::new("gpt-5.4", 1456, "local-secret");

    let installed = replace_model_catalog(
        &path,
        &config,
        &[SelectableModel {
            alias: "codex-gpt-5-7".into(),
            model: "gpt-5.7".into(),
            display_name: "Codex GPT-5.7".into(),
            context_window: Some(300_000),
            default_reasoning_effort: Some("medium".into()),
            supported_reasoning_efforts: vec![
                "low".into(),
                "medium".into(),
                "high".into(),
                "xhigh".into(),
                "max".into(),
                "ultra".into(),
            ],
        }],
        &["codex-old".into()],
    )
    .expect("replace catalog");

    let result = std::fs::read_to_string(path).expect("read config");
    assert!(!result.contains("[model.codex]"));
    assert!(!result.contains("[model.codex-old]"));
    assert!(result.contains("[model.codex-user]"));
    assert!(result.contains("[model.codex-gpt-5-7]"));
    assert!(result.contains("model = \"gpt-5.7\""));
    assert!(result.contains("context_window = 300000"));
    assert!(result.contains("supports_reasoning_effort = true"));
    assert!(result.contains("reasoning_effort = \"medium\""));
    assert!(result.contains("value = \"xhigh\""));
    assert!(result.contains("value = \"max\""));
    assert!(!result.contains("value = \"ultra\""));
    assert_eq!(installed, vec!["codex-gpt-5-7".to_string()]);
}

#[test]
fn managed_config_can_be_read_from_a_specific_model_alias() {
    let directory = tempfile::tempdir().expect("tempdir");
    let path = directory.path().join("config.toml");
    std::fs::write(
        &path,
        "[model.codex-sol]\nmodel = \"gpt-5.6-sol\"\nbase_url = \"http://127.0.0.1:1555/v1\"\nname = \"Codex GPT-5.6 Sol\"\napi_backend = \"responses\"\napi_key = \"specific-secret\"\ncontext_window = 272000\n",
    )
    .expect("seed config");

    assert_eq!(
        read_model_config(&path).expect("read"),
        Some(ModelConfig {
            model: "gpt-5.6-sol".into(),
            port: 1555,
            local_secret: "specific-secret".into(),
            context_window: 272_000,
        })
    );
}

#[test]
fn new_model_config_uses_the_codex_context_window_default() {
    assert_eq!(
        ModelConfig::new("gpt-5.6-sol", 1456, "secret").context_window,
        272_000
    );
}

#[test]
fn legacy_generic_context_window_is_normalized_during_migration() {
    let directory = tempfile::tempdir().expect("tempdir");
    let path = directory.path().join("config.toml");
    install_model(
        &path,
        &ModelConfig {
            model: "gpt-5.4".into(),
            port: 1456,
            local_secret: "legacy-secret".into(),
            context_window: 400_000,
        },
    )
    .expect("install legacy model");

    assert_eq!(
        read_model_config(&path)
            .expect("read")
            .expect("managed config")
            .context_window,
        272_000
    );
}

#[test]
fn existing_managed_model_config_can_be_reused_by_sync() {
    let directory = tempfile::tempdir().expect("tempdir");
    let path = directory.path().join("config.toml");
    install_model(
        &path,
        &ModelConfig {
            model: "custom-default".into(),
            port: 1555,
            local_secret: "existing-secret".into(),
            context_window: 123_456,
        },
    )
    .expect("install");

    assert_eq!(
        read_model_config(&path).expect("read"),
        Some(ModelConfig {
            model: "custom-default".into(),
            port: 1555,
            local_secret: "existing-secret".into(),
            context_window: 123_456,
        })
    );
}

#[test]
fn uninstall_removes_dynamic_managed_aliases_but_not_user_aliases() {
    let directory = tempfile::tempdir().expect("tempdir");
    let path = directory.path().join("config.toml");
    std::fs::write(
        &path,
        "[model.codex]\nmodel = \"default\"\n\n[model.codex-gpt-5-7]\nmodel = \"gpt-5.7\"\n\n[model.codex-user]\nmodel = \"mine\"\n\n[models]\ndefault = \"codex-user\"\nweb_search = \"codex-gpt-5-7\"\n",
    )
    .expect("seed");

    uninstall_model_aliases(&path, &["codex".into(), "codex-gpt-5-7".into()]).expect("uninstall");

    let result = std::fs::read_to_string(path).expect("read");
    assert!(!result.contains("[model.codex]"));
    assert!(!result.contains("[model.codex-gpt-5-7]"));
    assert!(result.contains("[model.codex-user]"));
    assert!(result.contains("default = \"codex-user\""));
    assert!(!result.contains("web_search"));
}

#[cfg(unix)]
#[test]
fn setup_writes_private_config_permissions() {
    use std::os::unix::fs::PermissionsExt;

    let directory = tempfile::tempdir().expect("tempdir");
    let path = directory.path().join("config.toml");
    install_model(&path, &ModelConfig::new("gpt-5.4", 1456, "secret")).expect("install model");

    assert_eq!(
        std::fs::metadata(path)
            .expect("metadata")
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
}
