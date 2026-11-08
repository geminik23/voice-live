use std::path::PathBuf;
use std::sync::Arc;

use browser_example::gateway::{AppState, build_router};
use browser_example::reservation::{ReservationStore, speculative_tools, task_tools};
use voice_live::{RuntimePolicies, VoiceRuntime, VoiceRuntimeConfig};

/// Command-line paths: the runtime config and, optionally, the restaurant list.
struct Args {
    config: PathBuf,
    restaurants: Option<PathBuf>,
}

fn parse_args() -> Args {
    let mut config = None;
    let mut restaurants = None;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" | "-c" => config = args.next().map(PathBuf::from),
            "--restaurants" => restaurants = args.next().map(PathBuf::from),
            other if other.ends_with(".yaml") || other.ends_with(".yml") => {
                config = Some(PathBuf::from(other));
            }
            _ => {}
        }
    }
    Args {
        config: config.unwrap_or_else(|| example_dir().join("configs/ko/voice-runtime.yaml")),
        restaurants,
    }
}

fn example_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

/// Locates the browser client: `VOICE_WEB_DIR` wins, otherwise the example's own `web/` directory.
fn resolve_web_dir() -> PathBuf {
    std::env::var("VOICE_WEB_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| example_dir().join("web"))
}

/// The restaurant list: `--restaurants` wins, otherwise `restaurants.yaml` next to the config, so each language directory carries its own.
fn resolve_restaurants(args: &Args) -> PathBuf {
    args.restaurants.clone().unwrap_or_else(|| {
        args.config
            .parent()
            .unwrap_or_else(|| std::path::Path::new("."))
            .join("restaurants.yaml")
    })
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let args = parse_args();
    let config = VoiceRuntimeConfig::from_file(&args.config)?;

    let web_dir = resolve_web_dir();
    if !web_dir.is_dir() {
        anyhow::bail!(
            "browser client directory not found at {}; set VOICE_WEB_DIR",
            web_dir.display()
        );
    }

    // The reservation backend is shared by every session, like an external system of record.
    let store = ReservationStore::from_yaml_file(&resolve_restaurants(&args))?;
    let policies = RuntimePolicies::default()
        .with_task_tools(task_tools(Arc::clone(&store)))
        .with_speculative_tools(speculative_tools(store));
    let runtime = VoiceRuntime::build_with_policies(config, None, policies).await?;
    let app = build_router(AppState::new(Arc::clone(&runtime), &web_dir));

    let addr = std::env::var("VOICE_BIND_ADDR").unwrap_or_else(|_| "0.0.0.0:8080".into());
    let listener = tokio::net::TcpListener::bind(&addr).await?;

    tracing::info!(web_dir = %web_dir.display(), "browser example listening on {addr}");
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
