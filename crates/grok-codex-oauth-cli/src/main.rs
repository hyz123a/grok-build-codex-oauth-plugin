use std::{
    collections::HashMap,
    fs,
    io::Write,
    net::SocketAddr,
    path::{Path, PathBuf},
    process::{Command, ExitCode, Stdio},
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result, anyhow, bail};
use axum::{
    Router,
    extract::{Query, State},
    response::Html,
    routing::get,
};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use clap::{Parser, Subcommand};
use codex_models::{
    CatalogModel, ModelCatalogClient, load_cached_catalog, model_alias, save_cached_catalog,
};
use codex_oauth::{
    CredentialStore, OAuthClient, OAuthConfig, PkceCodes, SystemCredentialStore,
    build_authorize_url, generate_state, validate_callback,
};
use codex_proxy::{CredentialProvider, ProxyConfig, StoredCredentialProvider, build_router};
use directories::BaseDirs;
use grok_config::{
    MANAGED_MODEL_ALIASES, ModelConfig, SelectableModel, install_model_catalog, read_model_config,
    replace_model_catalog, uninstall_model_aliases,
};
use rand::RngCore;
use tokio::sync::{Mutex, oneshot};

const DEFAULT_PORT: u16 = 1456;
const DEFAULT_MODEL: &str = "gpt-5.4";
const FALLBACK_CODEX_CLIENT_VERSION: &str = "0.145.0";

#[derive(Parser)]
#[command(version, about = "ChatGPT/Codex OAuth companion for Grok Build")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Sign in with ChatGPT in a browser.
    Login {
        /// Use the device-code flow for a remote or headless machine.
        #[arg(long)]
        device_code: bool,
    },
    /// Delete OAuth credentials and stop the local daemon.
    Logout,
    /// Show login, configuration, and daemon status.
    Status {
        #[arg(long)]
        json: bool,
    },
    /// Install or update the managed Grok model entry.
    Setup {
        #[arg(long, default_value = DEFAULT_MODEL)]
        model: String,
        #[arg(long, default_value_t = DEFAULT_PORT)]
        port: u16,
    },
    /// Discover and update models available to the signed-in ChatGPT account.
    Models {
        #[command(subcommand)]
        command: ModelsCommand,
    },
    /// Remove only the managed Grok model entry.
    UninstallConfig,
    /// Run the loopback Responses proxy.
    Serve {
        #[arg(long, default_value_t = DEFAULT_PORT)]
        port: u16,
    },
    /// Start the daemon when configured and logged in.
    EnsureDaemon {
        #[arg(long, default_value_t = DEFAULT_PORT)]
        port: u16,
    },
    /// Diagnose local setup without exposing credentials.
    Doctor {
        #[arg(long, default_value_t = DEFAULT_PORT)]
        port: u16,
    },
}

#[derive(Subcommand)]
enum ModelsCommand {
    /// Fetch the Codex model catalog and update Grok's /model choices.
    Sync,
}

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "grok_codex_oauth=info,codex_proxy=info".into()),
        )
        .with_target(false)
        .without_time()
        .init();

    match run(Cli::parse()).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("error: {error:#}");
            ExitCode::FAILURE
        }
    }
}

async fn run(cli: Cli) -> Result<()> {
    let paths = AppPaths::discover()?;
    match cli.command {
        Commands::Login { device_code } => login(&paths, device_code).await,
        Commands::Logout => logout(&paths).await,
        Commands::Status { json } => status(&paths, DEFAULT_PORT, json).await,
        Commands::Setup { model, port } => setup(&paths, &model, port),
        Commands::Models {
            command: ModelsCommand::Sync,
        } => sync_models(&paths).await,
        Commands::UninstallConfig => {
            let cached = load_cached_catalog(&paths.models_cache)?;
            uninstall_model_aliases(&paths.grok_config, &managed_aliases(cached.as_deref()))?;
            println!("Removed managed Codex models; other Grok settings were preserved.");
            Ok(())
        }
        Commands::Serve { port } => serve(&paths, port).await,
        Commands::EnsureDaemon { port } => ensure_daemon(&paths, port).await,
        Commands::Doctor { port } => doctor(&paths, port).await,
    }
}

#[derive(Clone)]
struct AppPaths {
    data_dir: PathBuf,
    credentials: PathBuf,
    proxy_secret: PathBuf,
    daemon_pid: PathBuf,
    daemon_log: PathBuf,
    grok_config: PathBuf,
    models_cache: PathBuf,
}

impl AppPaths {
    fn discover() -> Result<Self> {
        let base = BaseDirs::new().ok_or_else(|| anyhow!("could not locate the home directory"))?;
        let grok_dir = base.home_dir().join(".grok");
        let data_dir = grok_dir.join("codex-oauth");
        Ok(Self {
            credentials: data_dir.join("credentials.json"),
            proxy_secret: data_dir.join("proxy-secret"),
            daemon_pid: data_dir.join("daemon.pid"),
            daemon_log: data_dir.join("daemon.log"),
            grok_config: grok_dir.join("config.toml"),
            models_cache: data_dir.join("models-cache.json"),
            data_dir,
        })
    }

    fn store(&self) -> SystemCredentialStore {
        SystemCredentialStore::new(
            "grok-codex-oauth",
            "chatgpt-codex",
            self.credentials.clone(),
        )
    }
}

async fn sync_models(paths: &AppPaths) -> Result<()> {
    let config = read_model_config(&paths.grok_config)?
        .ok_or_else(|| anyhow!("not configured; run `grok-codex-oauth setup` first"))?;
    let cached = load_cached_catalog(&paths.models_cache)?;
    let previous_aliases = managed_aliases(cached.as_deref());
    let provider = StoredCredentialProvider::new(
        Arc::new(paths.store()),
        OAuthClient::new(OAuthConfig::default()),
    );
    let client_version = discover_codex_client_version();
    let remote = match provider.access_credential().await {
        Ok(credential) => {
            ModelCatalogClient::production(&client_version)?
                .fetch(&credential)
                .await
        }
        Err(error) => Err(codex_models::CatalogError::Authentication(error)),
    };

    match remote {
        Ok(models) if !models.is_empty() => {
            install_discovered_models(&paths.grok_config, &config, &models, &previous_aliases)?;
            save_cached_catalog(&paths.models_cache, &models)?;
            println!(
                "Synced {} selectable Codex models from the signed-in ChatGPT account.",
                models.len()
            );
        }
        Ok(_) => {
            install_fallback_catalog(paths, &config, cached.as_deref(), &previous_aliases)?;
            eprintln!(
                "warning: the remote catalog had no selectable models; used fallback catalog"
            );
        }
        Err(error) => {
            install_fallback_catalog(paths, &config, cached.as_deref(), &previous_aliases)?;
            eprintln!("warning: model sync failed ({error}); used fallback catalog");
        }
    }
    Ok(())
}

fn discover_codex_client_version() -> String {
    if let Ok(version) = std::env::var("GROK_CODEX_OAUTH_CLIENT_VERSION")
        && is_version(&version)
    {
        return version;
    }
    if let Ok(output) = Command::new("codex").arg("--version").output()
        && output.status.success()
        && let Some(version) = parse_codex_version(&String::from_utf8_lossy(&output.stdout))
    {
        return version;
    }
    FALLBACK_CODEX_CLIENT_VERSION.into()
}

fn parse_codex_version(output: &str) -> Option<String> {
    output.split_whitespace().find_map(|part| {
        let version = part.trim_start_matches('v');
        is_version(version).then(|| version.to_string())
    })
}

fn is_version(value: &str) -> bool {
    value.split('.').count() == 3
        && value.split('.').all(|part| {
            !part.is_empty() && part.chars().all(|character| character.is_ascii_digit())
        })
}

fn install_fallback_catalog(
    paths: &AppPaths,
    config: &ModelConfig,
    cached: Option<&[CatalogModel]>,
    previous_aliases: &[String],
) -> Result<()> {
    if let Some(models) = cached.filter(|models| !models.is_empty()) {
        install_discovered_models(&paths.grok_config, config, models, previous_aliases)?;
        println!(
            "Loaded {} selectable Codex models from cache.",
            models.len()
        );
    } else {
        install_model_catalog(&paths.grok_config, config)?;
        println!("Loaded the built-in Codex model catalog.");
    }
    Ok(())
}

fn install_discovered_models(
    path: &Path,
    config: &ModelConfig,
    models: &[CatalogModel],
    previous_aliases: &[String],
) -> Result<Vec<String>> {
    let selectable = models
        .iter()
        .map(|model| SelectableModel {
            alias: model_alias(&model.slug),
            model: model.slug.clone(),
            display_name: format!("Codex {}", model.display_name),
            context_window: model.context_window,
        })
        .collect::<Vec<_>>();
    replace_model_catalog(path, config, &selectable, previous_aliases).map_err(Into::into)
}

fn managed_aliases(cached: Option<&[CatalogModel]>) -> Vec<String> {
    let mut aliases = MANAGED_MODEL_ALIASES
        .iter()
        .map(|alias| (*alias).to_string())
        .collect::<Vec<_>>();
    for alias in cached
        .unwrap_or_default()
        .iter()
        .map(|model| model_alias(&model.slug))
    {
        if !aliases.contains(&alias) {
            aliases.push(alias);
        }
    }
    aliases
}

async fn login(paths: &AppPaths, device_code: bool) -> Result<()> {
    let oauth = OAuthClient::new(OAuthConfig::default());
    let credentials = if device_code {
        let device = oauth.request_device_code().await?;
        println!(
            "Open {} and enter code {}\nThe code expires in 15 minutes.",
            device.verification_url, device.user_code
        );
        oauth.complete_device_code(&device).await?
    } else {
        browser_login(&oauth).await?
    };
    paths.store().save(&credentials).await?;
    println!("Login successful. ChatGPT account credentials were stored locally.");
    Ok(())
}

#[derive(Clone)]
struct CallbackState {
    expected_state: String,
    sender: SharedCallbackSender,
}

type CallbackResult = Result<String, String>;
type SharedCallbackSender = Arc<Mutex<Option<oneshot::Sender<CallbackResult>>>>;

async fn browser_login(oauth: &OAuthClient) -> Result<codex_oauth::Credentials> {
    let listener = bind_callback_listener().await?;
    let port = listener.local_addr()?.port();
    let redirect_uri = format!("http://localhost:{port}/auth/callback");
    let expected_state = generate_state();
    let pkce = PkceCodes::generate();
    let authorize_url = build_authorize_url(
        &OAuthConfig::default(),
        &redirect_uri,
        &pkce,
        &expected_state,
    )?;
    let (sender, receiver) = oneshot::channel();
    let callback_state = CallbackState {
        expected_state,
        sender: Arc::new(Mutex::new(Some(sender))),
    };
    let app = Router::new()
        .route("/auth/callback", get(oauth_callback))
        .with_state(callback_state);
    let server = tokio::spawn(async move { axum::serve(listener, app).await });

    eprintln!("Open this URL to sign in:\n\n{authorize_url}\n");
    if let Err(error) = webbrowser::open(authorize_url.as_str()) {
        tracing::warn!("could not open a browser automatically: {error}");
    }
    let code = tokio::time::timeout(Duration::from_secs(10 * 60), receiver)
        .await
        .context("OAuth login timed out after 10 minutes")?
        .context("OAuth callback server stopped")?
        .map_err(|message| anyhow!(message))?;
    server.abort();
    oauth
        .exchange_code(&code, &redirect_uri, &pkce)
        .await
        .map_err(Into::into)
}

async fn bind_callback_listener() -> Result<tokio::net::TcpListener> {
    for port in [1455_u16, 1457_u16] {
        if let Ok(listener) = tokio::net::TcpListener::bind(("127.0.0.1", port)).await {
            return Ok(listener);
        }
    }
    bail!("OAuth callback ports 1455 and 1457 are both unavailable")
}

async fn oauth_callback(
    State(state): State<CallbackState>,
    Query(query): Query<HashMap<String, String>>,
) -> Html<&'static str> {
    let result = if let Some(error) = query.get("error") {
        Err(format!("OAuth provider returned `{error}`"))
    } else {
        validate_callback(
            query.get("code").map(String::as_str),
            query.get("state").map(String::as_str),
            &state.expected_state,
        )
        .map_err(|error| error.to_string())
    };
    let success = result.is_ok();
    if let Some(sender) = state.sender.lock().await.take() {
        let _ = sender.send(result);
    }
    if success {
        Html("<h1>Authentication successful</h1><p>You can return to the terminal.</p>")
    } else {
        Html("<h1>Authentication failed</h1><p>Return to the terminal for details.</p>")
    }
}

fn setup(paths: &AppPaths, model: &str, port: u16) -> Result<()> {
    fs::create_dir_all(&paths.data_dir)?;
    set_private_dir(&paths.data_dir)?;
    let secret = match fs::read_to_string(&paths.proxy_secret) {
        Ok(secret) if !secret.trim().is_empty() => secret.trim().to_owned(),
        _ => {
            let generated = generate_local_secret();
            write_private_file(&paths.proxy_secret, generated.as_bytes())?;
            generated
        }
    };
    install_model_catalog(&paths.grok_config, &ModelConfig::new(model, port, secret))?;
    println!(
        "Configured selectable Codex models at http://127.0.0.1:{port}/v1; `codex` uses {model}."
    );
    Ok(())
}

fn generate_local_secret() -> String {
    let mut bytes = [0_u8; 32];
    rand::rng().fill_bytes(&mut bytes);
    URL_SAFE_NO_PAD.encode(bytes)
}

async fn logout(paths: &AppPaths) -> Result<()> {
    paths.store().delete().await?;
    stop_daemon(paths)?;
    println!("Logged out. The managed Grok model configuration was left in place.");
    Ok(())
}

async fn serve(paths: &AppPaths, port: u16) -> Result<()> {
    let secret = read_secret(paths)?;
    if paths.store().load().await?.is_none() {
        bail!("not logged in; run `grok-codex-oauth login`");
    }
    fs::create_dir_all(&paths.data_dir)?;
    let _pid_guard = PidGuard::create(&paths.daemon_pid)?;
    let credentials = Arc::new(StoredCredentialProvider::new(
        Arc::new(paths.store()),
        OAuthClient::new(OAuthConfig::default()),
    ));
    let app = build_router(ProxyConfig::production(secret)?, credentials)?;
    let address = SocketAddr::from(([127, 0, 0, 1], port));
    let listener = tokio::net::TcpListener::bind(address)
        .await
        .with_context(|| format!("could not bind loopback proxy at {address}"))?;
    tracing::info!("Codex OAuth proxy listening on {address}");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let terminate = async {
        if let Ok(mut signal) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            signal.recv().await;
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        () = ctrl_c => {},
        () = terminate => {},
    }
}

async fn ensure_daemon(paths: &AppPaths, port: u16) -> Result<()> {
    if health(port).await {
        return Ok(());
    }
    if paths.store().load().await?.is_none() {
        eprintln!("Codex OAuth is not logged in; daemon was not started.");
        return Ok(());
    }
    if !paths.proxy_secret.exists() {
        eprintln!("Codex OAuth is not configured; run `grok-codex-oauth setup`.");
        return Ok(());
    }
    fs::create_dir_all(&paths.data_dir)?;
    let log = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&paths.daemon_log)?;
    let error_log = log.try_clone()?;
    Command::new(std::env::current_exe()?)
        .arg("serve")
        .arg("--port")
        .arg(port.to_string())
        .stdin(Stdio::null())
        .stdout(Stdio::from(log))
        .stderr(Stdio::from(error_log))
        .spawn()
        .context("failed to start Codex OAuth daemon")?;
    for _ in 0..40 {
        if health(port).await {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    bail!(
        "daemon did not become healthy; inspect {}",
        paths.daemon_log.display()
    )
}

async fn status(paths: &AppPaths, port: u16, json_output: bool) -> Result<()> {
    let logged_in = paths.store().load().await?.is_some();
    let configured = paths.proxy_secret.exists() && paths.grok_config.exists();
    let daemon = health(port).await;
    if json_output {
        println!(
            "{}",
            serde_json::json!({
                "logged_in": logged_in,
                "configured": configured,
                "daemon_running": daemon,
                "port": port
            })
        );
    } else {
        println!("Login: {}", yes_no(logged_in));
        println!("Grok model configured: {}", yes_no(configured));
        println!("Proxy running: {}", yes_no(daemon));
    }
    Ok(())
}

async fn doctor(paths: &AppPaths, port: u16) -> Result<()> {
    let logged_in = paths.store().load().await?.is_some();
    let secret = paths.proxy_secret.exists();
    let config = paths.grok_config.exists();
    let daemon = health(port).await;
    println!("credentials: {}", check(logged_in));
    println!("proxy secret: {}", check(secret));
    println!("Grok config: {}", check(config));
    println!("daemon health: {}", check(daemon));
    if logged_in && secret && config {
        Ok(())
    } else {
        bail!("setup is incomplete")
    }
}

async fn health(port: u16) -> bool {
    reqwest::Client::new()
        .get(format!("http://127.0.0.1:{port}/health"))
        .timeout(Duration::from_millis(300))
        .send()
        .await
        .is_ok_and(|response| response.status().is_success())
}

fn read_secret(paths: &AppPaths) -> Result<String> {
    let secret = fs::read_to_string(&paths.proxy_secret)
        .context("proxy secret is missing; run `grok-codex-oauth setup`")?;
    let secret = secret.trim();
    if secret.is_empty() {
        bail!("proxy secret is empty; rerun setup")
    }
    Ok(secret.to_owned())
}

fn stop_daemon(paths: &AppPaths) -> Result<()> {
    let Ok(pid) = fs::read_to_string(&paths.daemon_pid) else {
        return Ok(());
    };
    let pid = pid.trim();
    if !pid.chars().all(|character| character.is_ascii_digit()) {
        bail!("daemon PID file is invalid")
    }
    #[cfg(unix)]
    let status = Command::new("kill").arg("-TERM").arg(pid).status()?;
    #[cfg(windows)]
    let status = Command::new("taskkill").args(["/PID", pid]).status()?;
    if !status.success() {
        tracing::warn!("daemon process was not running");
    }
    let _ = fs::remove_file(&paths.daemon_pid);
    Ok(())
}

struct PidGuard {
    path: PathBuf,
}

impl PidGuard {
    fn create(path: &Path) -> Result<Self> {
        write_private_file(path, std::process::id().to_string().as_bytes())?;
        Ok(Self {
            path: path.to_owned(),
        })
    }
}

impl Drop for PidGuard {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

fn write_private_file(path: &Path, contents: &[u8]) -> Result<()> {
    let parent = path.parent().ok_or_else(|| anyhow!("path has no parent"))?;
    fs::create_dir_all(parent)?;
    let mut options = fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(path)?;
    file.write_all(contents)?;
    file.sync_all()?;
    Ok(())
}

#[cfg(unix)]
fn set_private_dir(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    Ok(())
}

#[cfg(not(unix))]
fn set_private_dir(_path: &Path) -> Result<()> {
    Ok(())
}

fn yes_no(value: bool) -> &'static str {
    if value { "yes" } else { "no" }
}

fn check(value: bool) -> &'static str {
    if value { "ok" } else { "missing" }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn models_sync_is_a_supported_command() {
        let cli =
            Cli::try_parse_from(["grok-codex-oauth", "models", "sync"]).expect("parse models sync");
        assert!(matches!(
            cli.command,
            Commands::Models {
                command: ModelsCommand::Sync
            }
        ));
    }

    #[test]
    fn installed_codex_version_output_is_parsed_for_model_discovery() {
        assert_eq!(
            parse_codex_version("codex-cli 0.145.0\n"),
            Some("0.145.0".into())
        );
        assert_eq!(parse_codex_version("unexpected output"), None);
    }

    #[test]
    fn plugin_skills_do_not_depend_on_hook_only_environment() {
        let skills = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../plugin/skills");
        for entry in std::fs::read_dir(skills).expect("read plugin skills") {
            let path = entry.expect("skill entry").path().join("SKILL.md");
            let content = std::fs::read_to_string(&path).expect("read SKILL.md");
            assert!(
                !content.contains("GROK_PLUGIN_ROOT"),
                "{} must not require hook-only GROK_PLUGIN_ROOT",
                path.display()
            );
            assert!(
                content.contains("grok-codex-oauth"),
                "{} must invoke the installed CLI",
                path.display()
            );
        }
    }
}
