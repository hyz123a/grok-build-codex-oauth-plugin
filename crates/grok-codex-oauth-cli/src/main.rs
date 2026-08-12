#[cfg(target_os = "linux")]
use std::os::unix::ffi::OsStrExt;
use std::{
    collections::{HashMap, VecDeque},
    fs,
    io::{BufRead, BufReader, Seek, SeekFrom, Write},
    net::SocketAddr,
    path::{Path, PathBuf},
    process::{Command, ExitCode, Stdio},
    sync::{Arc, Mutex as StdMutex},
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
use codex_proxy::{
    CredentialProvider, ProxyConfig, StoredCredentialProvider, build_router,
    system_prompt_policy_fingerprint, websocket_proxy_configured_from_env,
};
use directories::BaseDirs;
use fs2::FileExt;
use grok_config::{
    BUILTIN_CODEX_MODEL_CATALOG, MANAGED_MODEL_ALIASES, ModelConfig, SelectableModel,
    install_model_catalog, read_model_config, replace_model_catalog, uninstall_model_aliases,
};
use rand::RngCore;
use tokio::sync::{Mutex, oneshot};
use tracing_subscriber::fmt::MakeWriter;

const DEFAULT_PORT: u16 = 1456;
const DEFAULT_MODEL: &str = "gpt-5.4";
const FALLBACK_CODEX_CLIENT_VERSION: &str = "0.145.0";
const DEFAULT_LOG_LINES: usize = 100;
const MAX_DAEMON_LOG_BYTES: u64 = 10 * 1024 * 1024;
const DAEMON_LOG_BACKUPS: usize = 3;
const DAEMON_STOP_GRACE_PERIOD: Duration = Duration::from_secs(5);
const DAEMON_KILL_WAIT: Duration = Duration::from_secs(2);
const DAEMON_PROCESS_POLL_INTERVAL: Duration = Duration::from_millis(50);

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
        /// Write daemon tracing through the size-limited rotating log writer.
        #[arg(long, hide = true)]
        daemon_log: Option<PathBuf>,
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
    /// Show or follow proxy diagnostic logs.
    Logs {
        /// Continue printing new log lines.
        #[arg(short, long)]
        follow: bool,
        /// Number of matching historical lines to show.
        #[arg(short = 'n', long, default_value_t = DEFAULT_LOG_LINES)]
        lines: usize,
        /// Show only warnings, errors, and failed request outcomes.
        #[arg(long)]
        errors: bool,
        /// Show only entries containing this client or upstream request ID.
        #[arg(long)]
        request_id: Option<String>,
    },
}

#[derive(Subcommand)]
enum ModelsCommand {
    /// Fetch the Codex model catalog and update Grok's /model choices.
    Sync,
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    let daemon_logging = matches!(
        &cli.command,
        Commands::Serve {
            daemon_log: Some(_),
            ..
        }
    );
    if let Err(error) = init_tracing(&cli) {
        eprintln!("error: could not initialize logging: {error:#}");
        return ExitCode::FAILURE;
    }

    match run(cli).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            if daemon_logging {
                tracing::error!(error = %format!("{error:#}"), "Codex OAuth daemon stopped");
            } else {
                eprintln!("error: {error:#}");
            }
            ExitCode::FAILURE
        }
    }
}

fn init_tracing(cli: &Cli) -> Result<()> {
    let daemon_log = match &cli.command {
        Commands::Serve {
            daemon_log: Some(path),
            ..
        } => Some(path),
        _ => None,
    };
    if let Some(path) = daemon_log {
        let writer =
            RotatingLogWriter::new(path.clone(), MAX_DAEMON_LOG_BYTES, DAEMON_LOG_BACKUPS)?;
        tracing_subscriber::fmt()
            .with_env_filter(default_tracing_filter())
            .with_target(false)
            .with_ansi(false)
            .with_writer(writer)
            .init();
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(default_tracing_filter())
            .with_target(false)
            .with_ansi(false)
            .init();
    }
    Ok(())
}

fn default_tracing_filter() -> tracing_subscriber::EnvFilter {
    tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| "grok_codex_oauth=info,codex_proxy=info".into())
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
        Commands::Serve { port, .. } => serve(&paths, port).await,
        Commands::EnsureDaemon { port } => ensure_daemon(&paths, port).await,
        Commands::Doctor { port } => doctor(&paths, port).await,
        Commands::Logs {
            follow,
            lines,
            errors,
            request_id,
        } => logs(&paths, follow, lines, errors, request_id.as_deref()),
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
            default_reasoning_effort: model.default_reasoning_effort.clone(),
            supported_reasoning_efforts: model.supported_reasoning_efforts.clone(),
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
    println!("Configured selectable Codex models at http://127.0.0.1:{port}/v1.");
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
    let mut allowed_models = BUILTIN_CODEX_MODEL_CATALOG
        .iter()
        .map(|(_, model, _)| (*model).to_string())
        .collect::<Vec<_>>();
    let mut proxy_config = ProxyConfig::production(secret)?.with_websocket_proxy_from_env()?;
    match load_cached_catalog(&paths.models_cache) {
        Ok(Some(models)) => {
            for model in models {
                if !allowed_models.contains(&model.slug) {
                    allowed_models.push(model.slug.clone());
                }
                proxy_config = proxy_config.with_model_reasoning(
                    model.slug,
                    model.supported_reasoning_efforts,
                    model.default_reasoning_effort,
                );
            }
        }
        Ok(None) => {}
        Err(error) => {
            tracing::warn!(
                error = %error,
                "Could not load model reasoning metadata; using compatibility defaults"
            );
        }
    }
    proxy_config = proxy_config.with_allowed_models(allowed_models);
    let app = build_router(proxy_config, credentials)?;
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
    let expected_websocket_proxy = websocket_proxy_configured_from_env()?;
    let expected_system_prompt_policy_fingerprint = system_prompt_policy_fingerprint();
    if let Some(active) = health_status(port).await {
        if daemon_configuration_matches(
            &active,
            expected_websocket_proxy,
            &expected_system_prompt_policy_fingerprint,
        ) {
            return Ok(());
        }
        tracing::info!(
            active_websocket_proxy = active.websocket_proxy,
            expected_websocket_proxy,
            system_prompt_policy_changed = active.system_prompt_policy_fingerprint
                != expected_system_prompt_policy_fingerprint,
            "Restarting Codex OAuth daemon because runtime configuration changed"
        );
        stop_daemon(paths)?;
        let mut stopped = false;
        for _ in 0..40 {
            if health_status(port).await.is_none() {
                stopped = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        if !stopped {
            bail!(
                "daemon is still listening on port {port} after the stop attempt; inspect {}",
                paths.daemon_log.display()
            )
        }
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
    spawn_daemon(paths, port)?;
    for _ in 0..40 {
        if let Some(active) = health_status(port).await
            && daemon_configuration_matches(
                &active,
                expected_websocket_proxy,
                &expected_system_prompt_policy_fingerprint,
            )
        {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    bail!(
        "daemon did not become healthy; inspect {}",
        paths.daemon_log.display()
    )
}

fn spawn_daemon(paths: &AppPaths, port: u16) -> Result<()> {
    let executable = std::env::current_exe()?;
    #[cfg(target_os = "linux")]
    let mut command = {
        let mut command = Command::new("setsid");
        command.arg("--fork").arg(executable);
        command
    };
    #[cfg(all(unix, not(target_os = "linux")))]
    let mut command = {
        use std::os::unix::process::CommandExt;
        let mut command = Command::new(executable);
        command.process_group(0);
        command
    };
    #[cfg(windows)]
    let mut command = {
        use std::os::windows::process::CommandExt;
        const DETACHED_PROCESS: u32 = 0x0000_0008;
        const CREATE_NEW_PROCESS_GROUP: u32 = 0x0000_0200;
        let mut command = Command::new(executable);
        command.creation_flags(DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP);
        command
    };
    command
        .arg("serve")
        .arg("--port")
        .arg(port.to_string())
        .arg("--daemon-log")
        .arg(&paths.daemon_log)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .context("failed to start detached Codex OAuth daemon")?;
    Ok(())
}

fn daemon_configuration_matches(
    active: &DaemonHealth,
    expected_websocket_proxy: bool,
    expected_system_prompt_policy_fingerprint: &str,
) -> bool {
    active.websocket_proxy == expected_websocket_proxy
        && active.system_prompt_policy_fingerprint == expected_system_prompt_policy_fingerprint
}

fn logs(
    paths: &AppPaths,
    follow: bool,
    lines: usize,
    errors: bool,
    request_id: Option<&str>,
) -> Result<()> {
    for line in read_log_tail(&paths.daemon_log, lines, errors, request_id)? {
        println!("{line}");
    }
    if follow {
        follow_log(&paths.daemon_log, errors, request_id)?;
    }
    Ok(())
}

fn read_log_tail(
    path: &Path,
    lines: usize,
    errors: bool,
    request_id: Option<&str>,
) -> Result<Vec<String>> {
    let file = fs::File::open(path)
        .with_context(|| format!("could not open proxy log {}", path.display()))?;
    let mut tail = VecDeque::with_capacity(lines);
    if lines == 0 {
        return Ok(Vec::new());
    }
    for line in BufReader::new(file).lines() {
        let line = line?;
        if !log_line_matches(&line, errors, request_id) {
            continue;
        }
        if tail.len() == lines {
            tail.pop_front();
        }
        tail.push_back(line);
    }
    Ok(tail.into_iter().collect())
}

fn follow_log(path: &Path, errors: bool, request_id: Option<&str>) -> Result<()> {
    let mut offset = fs::metadata(path)
        .map(|metadata| metadata.len())
        .unwrap_or(0);
    loop {
        if let Ok(mut file) = fs::File::open(path) {
            let length = file.metadata()?.len();
            if length < offset {
                offset = 0;
            }
            if length > offset {
                file.seek(SeekFrom::Start(offset))?;
                let mut reader = BufReader::new(file);
                let mut line = String::new();
                while reader.read_line(&mut line)? != 0 {
                    if log_line_matches(line.trim_end_matches(['\r', '\n']), errors, request_id) {
                        print!("{line}");
                    }
                    line.clear();
                }
                offset = reader.stream_position()?;
                std::io::stdout().flush()?;
            }
        }
        std::thread::sleep(Duration::from_millis(250));
    }
}

fn log_line_matches(line: &str, errors: bool, request_id: Option<&str>) -> bool {
    if request_id.is_some_and(|request_id| !line.contains(request_id)) {
        return false;
    }
    !errors || is_error_log_line(line)
}

fn is_error_log_line(line: &str) -> bool {
    line.contains(" WARN ")
        || line.starts_with("WARN ")
        || line.contains(" ERROR ")
        || line.starts_with("ERROR ")
        || line.contains("SSE error")
        || line.contains("request failed")
        || line.contains("transport failed")
        || [
            "sse_error",
            "http_error",
            "transport_error",
            "client_disconnected",
            "upstream_request_error",
        ]
        .iter()
        .any(|outcome| line.contains(&format!("outcome=\"{outcome}\"")))
}

fn rotate_log_if_needed(path: &Path, max_bytes: u64, backups: usize) -> Result<()> {
    let Ok(metadata) = fs::metadata(path) else {
        return Ok(());
    };
    if metadata.len() < max_bytes {
        return Ok(());
    }
    rotate_log_files(path, backups)?;
    Ok(())
}

fn rotate_log_files(path: &Path, backups: usize) -> std::io::Result<()> {
    if backups == 0 {
        fs::File::create(path)?;
        return Ok(());
    }
    for index in (1..=backups).rev() {
        let source = if index == 1 {
            path.to_owned()
        } else {
            log_backup_path(path, index - 1)
        };
        let target = log_backup_path(path, index);
        if target.exists() {
            fs::remove_file(&target)?;
        }
        if source.exists() {
            fs::rename(source, target)?;
        }
    }
    Ok(())
}

fn log_backup_path(path: &Path, index: usize) -> PathBuf {
    let mut path = path.as_os_str().to_owned();
    path.push(format!(".{index}"));
    PathBuf::from(path)
}

#[derive(Clone)]
struct RotatingLogWriter {
    state: Arc<StdMutex<RotatingLogState>>,
}

struct RotatingLogState {
    path: PathBuf,
    file: Option<fs::File>,
    bytes_written: u64,
    max_bytes: u64,
    backups: usize,
}

struct RotatingEventWriter {
    state: Arc<StdMutex<RotatingLogState>>,
    buffer: Vec<u8>,
}

impl RotatingLogWriter {
    fn new(path: PathBuf, max_bytes: u64, backups: usize) -> Result<Self> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        rotate_log_if_needed(&path, max_bytes, backups)?;
        let file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;
        let bytes_written = file.metadata()?.len();
        Ok(Self {
            state: Arc::new(StdMutex::new(RotatingLogState {
                path,
                file: Some(file),
                bytes_written,
                max_bytes,
                backups,
            })),
        })
    }
}

impl<'writer> MakeWriter<'writer> for RotatingLogWriter {
    type Writer = RotatingEventWriter;

    fn make_writer(&'writer self) -> Self::Writer {
        RotatingEventWriter {
            state: self.state.clone(),
            buffer: Vec::new(),
        }
    }
}

impl RotatingEventWriter {
    fn commit(&mut self) -> std::io::Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_| std::io::Error::other("rotating log lock poisoned"))?;
        state.write_event(&self.buffer)?;
        self.buffer.clear();
        Ok(())
    }
}

impl Write for RotatingEventWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.buffer.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.commit()
    }
}

impl Drop for RotatingEventWriter {
    fn drop(&mut self) {
        let _ = self.commit();
    }
}

impl RotatingLogState {
    fn write_event(&mut self, event: &[u8]) -> std::io::Result<()> {
        let event_bytes = u64::try_from(event.len()).unwrap_or(u64::MAX);
        if self.bytes_written > 0 && self.bytes_written.saturating_add(event_bytes) > self.max_bytes
        {
            self.rotate()?;
        }
        let file = self
            .file
            .as_mut()
            .ok_or_else(|| std::io::Error::other("rotating log file is not open"))?;
        file.write_all(event)?;
        file.flush()?;
        self.bytes_written = self.bytes_written.saturating_add(event_bytes);
        Ok(())
    }

    fn rotate(&mut self) -> std::io::Result<()> {
        self.file.take();
        rotate_log_files(&self.path, self.backups)?;
        self.file = Some(
            fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&self.path)?,
        );
        self.bytes_written = 0;
        Ok(())
    }
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
    health_status(port).await.is_some()
}

#[derive(Debug, PartialEq, Eq)]
struct DaemonHealth {
    websocket_proxy: bool,
    system_prompt_policy_fingerprint: String,
}

async fn health_status(port: u16) -> Option<DaemonHealth> {
    let response = reqwest::Client::new()
        .get(format!("http://127.0.0.1:{port}/health"))
        .timeout(Duration::from_millis(300))
        .send()
        .await
        .ok()?;
    if !response.status().is_success() {
        return None;
    }
    let body = response.json::<serde_json::Value>().await.ok()?;
    Some(DaemonHealth {
        websocket_proxy: body
            .get("websocket_proxy")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false),
        system_prompt_policy_fingerprint: body
            .get("system_prompt_policy_fingerprint")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_owned(),
    })
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
    if pid.is_empty() || !pid.chars().all(|character| character.is_ascii_digit()) {
        bail!("daemon PID file is invalid")
    }
    if !is_managed_daemon_process(pid)? {
        tracing::warn!(pid, "Ignoring stale daemon PID file");
        remove_pid_file_if_owned(&paths.daemon_pid, pid);
        return Ok(());
    }
    #[cfg(unix)]
    let status = Command::new("kill")
        .arg("-TERM")
        .arg(pid)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()?;
    #[cfg(windows)]
    let status = Command::new("taskkill").args(["/PID", pid]).status()?;
    if !status.success() && process_exists(pid) {
        bail!("could not stop daemon process {pid}")
    }
    if !wait_for_process_exit(pid, DAEMON_STOP_GRACE_PERIOD) {
        tracing::warn!(pid, "Daemon did not stop gracefully; terminating it");
        #[cfg(unix)]
        let status = Command::new("kill")
            .arg("-KILL")
            .arg(pid)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()?;
        #[cfg(windows)]
        let status = Command::new("taskkill")
            .args(["/F", "/PID", pid])
            .status()?;
        if (!status.success() && process_exists(pid))
            || !wait_for_process_exit(pid, DAEMON_KILL_WAIT)
        {
            bail!("daemon process {pid} did not exit")
        }
    }
    remove_pid_file_if_owned(&paths.daemon_pid, pid);
    Ok(())
}

#[derive(Debug)]
struct PidGuard {
    path: PathBuf,
    pid: String,
    _lock: fs::File,
}

impl PidGuard {
    fn create(path: &Path) -> Result<Self> {
        let lock_path = path.with_extension("lock");
        let parent = lock_path
            .parent()
            .ok_or_else(|| anyhow!("daemon lock path has no parent"))?;
        fs::create_dir_all(parent)?;
        let mut options = fs::OpenOptions::new();
        options.read(true).write(true).create(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let lock = options.open(&lock_path)?;
        FileExt::try_lock_exclusive(&lock)
            .with_context(|| "another Codex OAuth daemon is already running")?;
        let pid = std::process::id().to_string();
        write_private_file(path, pid.as_bytes())?;
        Ok(Self {
            path: path.to_owned(),
            pid,
            _lock: lock,
        })
    }
}

impl Drop for PidGuard {
    fn drop(&mut self) {
        remove_pid_file_if_owned(&self.path, &self.pid);
    }
}

fn remove_pid_file_if_owned(path: &Path, expected_pid: &str) {
    if fs::read_to_string(path).is_ok_and(|pid| pid.trim() == expected_pid) {
        let _ = fs::remove_file(path);
    }
}

fn wait_for_process_exit(pid: &str, timeout: Duration) -> bool {
    let deadline = std::time::Instant::now() + timeout;
    while process_exists(pid) {
        if std::time::Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(DAEMON_PROCESS_POLL_INTERVAL);
    }
    true
}

#[cfg(unix)]
fn process_exists(pid: &str) -> bool {
    Command::new("kill")
        .arg("-0")
        .arg(pid)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|status| status.success())
}

#[cfg(windows)]
fn process_exists(pid: &str) -> bool {
    Command::new("tasklist")
        .args(["/FI", &format!("PID eq {pid}"), "/NH"])
        .output()
        .is_ok_and(|output| {
            output.status.success() && String::from_utf8_lossy(&output.stdout).contains(pid)
        })
}

#[cfg(target_os = "linux")]
fn is_managed_daemon_process(pid: &str) -> Result<bool> {
    let command_line = match fs::read(format!("/proc/{pid}/cmdline")) {
        Ok(command_line) => command_line,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error.into()),
    };
    let arguments = command_line
        .split(|byte| *byte == 0)
        .filter(|argument| !argument.is_empty())
        .collect::<Vec<_>>();
    let executable_matches = arguments.first().is_some_and(|argument| {
        Path::new(std::ffi::OsStr::from_bytes(argument))
            .file_name()
            .is_some_and(|name| name == "grok-codex-oauth")
    });
    Ok(executable_matches && arguments.iter().any(|argument| *argument == b"serve"))
}

#[cfg(all(unix, not(target_os = "linux")))]
fn is_managed_daemon_process(pid: &str) -> Result<bool> {
    let output = Command::new("ps")
        .args(["-p", pid, "-o", "command="])
        .output()?;
    let command = String::from_utf8_lossy(&output.stdout);
    Ok(output.status.success()
        && command.contains("grok-codex-oauth")
        && command
            .split_whitespace()
            .any(|argument| argument == "serve"))
}

#[cfg(windows)]
fn is_managed_daemon_process(pid: &str) -> Result<bool> {
    let output = Command::new("tasklist")
        .args(["/FI", &format!("PID eq {pid}"), "/NH"])
        .output()?;
    Ok(output.status.success()
        && String::from_utf8_lossy(&output.stdout).contains("grok-codex-oauth.exe"))
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
    fn logs_command_supports_follow_lines_errors_and_request_id() {
        let cli = Cli::try_parse_from([
            "grok-codex-oauth",
            "logs",
            "--follow",
            "--lines",
            "25",
            "--errors",
            "--request-id",
            "request-42",
        ])
        .expect("parse logs");

        assert!(matches!(
            cli.command,
            Commands::Logs {
                follow: true,
                lines: 25,
                errors: true,
                request_id: Some(ref request_id),
            } if request_id == "request-42"
        ));
    }

    #[test]
    fn log_tail_applies_error_and_request_id_filters_before_limiting_lines() {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("daemon.log");
        fs::write(
            &path,
            concat!(
                "INFO request completed client_request_id=\"request-1\" outcome=\"completed\"\n",
                "WARN SSE error client_request_id=\"request-2\" outcome=\"sse_error\"\n",
                "WARN SSE error client_request_id=\"request-1\" outcome=\"server_error\"\n",
                "INFO request completed client_request_id=\"request-1\" outcome=\"completed\"\n",
            ),
        )
        .expect("write log");

        let lines =
            read_log_tail(&path, 1, true, Some("request-1")).expect("read filtered log tail");

        assert_eq!(
            lines,
            vec!["WARN SSE error client_request_id=\"request-1\" outcome=\"server_error\""]
        );
    }

    #[test]
    fn log_rotation_keeps_three_backups_and_discards_the_oldest() {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("daemon.log");

        for contents in ["first-log", "second-log", "third-log", "fourth-log"] {
            fs::write(&path, contents).expect("write active log");
            rotate_log_if_needed(&path, 1, 3).expect("rotate log");
        }

        assert!(!path.exists());
        assert_eq!(
            fs::read_to_string(path.with_extension("log.1")).expect("backup 1"),
            "fourth-log"
        );
        assert_eq!(
            fs::read_to_string(path.with_extension("log.2")).expect("backup 2"),
            "third-log"
        );
        assert_eq!(
            fs::read_to_string(path.with_extension("log.3")).expect("backup 3"),
            "second-log"
        );
        assert!(!path.with_extension("log.4").exists());
    }

    #[test]
    fn rotating_writer_enforces_the_size_limit_while_the_daemon_is_running() {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("daemon.log");
        let writer = RotatingLogWriter::new(path.clone(), 10, 3).expect("rotating writer");

        for contents in ["first-log", "second-log", "third-log", "fourth-log"] {
            let mut event = writer.make_writer();
            writeln!(event, "{contents}").expect("write event");
        }

        assert_eq!(
            fs::read_to_string(&path).expect("active log"),
            "fourth-log\n"
        );
        assert_eq!(
            fs::read_to_string(path.with_extension("log.1")).expect("backup 1"),
            "third-log\n"
        );
        assert_eq!(
            fs::read_to_string(path.with_extension("log.2")).expect("backup 2"),
            "second-log\n"
        );
        assert_eq!(
            fs::read_to_string(path.with_extension("log.3")).expect("backup 3"),
            "first-log\n"
        );
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
    fn pid_guard_does_not_remove_a_replaced_pid_file() {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("daemon.pid");
        let guard = PidGuard::create(&path).expect("create PID guard");

        fs::write(&path, "987654").expect("replace PID file");
        drop(guard);

        assert_eq!(fs::read_to_string(path).expect("read PID file"), "987654");
    }

    #[test]
    fn pid_guard_prevents_two_daemons_from_owning_the_same_pid_file() {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("daemon.pid");
        let first = PidGuard::create(&path).expect("create first PID guard");

        let error = PidGuard::create(&path).expect_err("reject second PID guard");
        assert!(
            error
                .to_string()
                .contains("another Codex OAuth daemon is already running")
        );

        drop(first);
        let replacement = PidGuard::create(&path).expect("create replacement PID guard");
        drop(replacement);
        assert!(!path.exists());
    }

    #[test]
    fn pid_file_is_only_removed_for_its_current_owner() {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("daemon.pid");
        fs::write(&path, "123").expect("write PID file");

        remove_pid_file_if_owned(&path, "456");
        assert!(path.exists());

        remove_pid_file_if_owned(&path, "123");
        assert!(!path.exists());
    }

    #[test]
    fn daemon_health_only_matches_the_expected_runtime_configuration() {
        let active = DaemonHealth {
            websocket_proxy: false,
            system_prompt_policy_fingerprint: "current-policy".into(),
        };

        assert!(daemon_configuration_matches(
            &active,
            false,
            "current-policy"
        ));
        assert!(!daemon_configuration_matches(
            &active,
            true,
            "current-policy"
        ));
        assert!(!daemon_configuration_matches(&active, false, "old-policy"));
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
