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
        "[model.codex-old]\nmodel = \"old\"\n\n[model.codex-user]\nmodel = \"mine\"\n",
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
        }],
        &["codex-old".into()],
    )
    .expect("replace catalog");

    let result = std::fs::read_to_string(path).expect("read config");
    assert!(!result.contains("[model.codex-old]"));
    assert!(result.contains("[model.codex-user]"));
    assert!(result.contains("[model.codex-gpt-5-7]"));
    assert!(result.contains("model = \"gpt-5.7\""));
    assert!(result.contains("context_window = 300000"));
    assert_eq!(
        installed,
        vec!["codex".to_string(), "codex-gpt-5-7".to_string()]
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
        "[model.codex]\nmodel = \"default\"\n\n[model.codex-gpt-5-7]\nmodel = \"gpt-5.7\"\n\n[model.codex-user]\nmodel = \"mine\"\n",
    )
    .expect("seed");

    uninstall_model_aliases(&path, &["codex".into(), "codex-gpt-5-7".into()]).expect("uninstall");

    let result = std::fs::read_to_string(path).expect("read");
    assert!(!result.contains("[model.codex]"));
    assert!(!result.contains("[model.codex-gpt-5-7]"));
    assert!(result.contains("[model.codex-user]"));
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
