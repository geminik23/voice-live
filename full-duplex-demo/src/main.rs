use std::path::PathBuf;
use std::sync::Arc;

mod gateway;

use gateway::{AppState, build_router};
use voice_live::{VoiceRuntime, VoiceRuntimeConfig};

fn parse_config_path() -> PathBuf {
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" | "-c" => {
                if let Some(path) = args.next() {
                    return PathBuf::from(path);
                }
            }
            other if other.ends_with(".yaml") || other.ends_with(".yml") => {
                return PathBuf::from(other);
            }
            _ => {}
        }
    }

    PathBuf::from("configs/voice-runtime.yaml")
}

/// Locates the browser client.
///
/// `VOICE_WEB_DIR` wins; otherwise the directory is resolved relative to the
/// config file, so the demo runs from any working directory.
fn resolve_web_dir(config_path: &std::path::Path) -> PathBuf {
    if let Ok(dir) = std::env::var("VOICE_WEB_DIR") {
        return PathBuf::from(dir);
    }

    config_path
        .parent()
        .map(|dir| dir.join("../web"))
        .unwrap_or_else(|| PathBuf::from("web"))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let config_path = parse_config_path();
    let config = VoiceRuntimeConfig::from_file(&config_path)?;

    let web_dir = resolve_web_dir(&config_path);
    if !web_dir.is_dir() {
        anyhow::bail!(
            "browser client directory not found at {}; set VOICE_WEB_DIR",
            web_dir.display()
        );
    }

    let runtime = VoiceRuntime::build(config).await?;
    let app = build_router(AppState::new(Arc::clone(&runtime), &web_dir));

    let addr = std::env::var("VOICE_BIND_ADDR").unwrap_or_else(|_| "0.0.0.0:8080".into());
    let listener = tokio::net::TcpListener::bind(&addr).await?;

    tracing::info!(web_dir = %web_dir.display(), "voice demo listening on {addr}");
    tracing::info!(
        sessions = runtime.session_count(),
        "runtime ready; open http://{addr} in a browser"
    );

    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
            tracing::info!("shutdown requested");
        })
        .await?;

    Ok(())
}
