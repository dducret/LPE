use anyhow::Result;
use lpe_admin_api::router;
use std::env;
use tokio::net::TcpListener;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let bind_address = env::var("LPE_BIND_ADDRESS").unwrap_or_else(|_| "127.0.0.1:8080".to_string());
    let listener = TcpListener::bind(&bind_address).await?;
    info!("lpe admin api listening on http://{bind_address}");

    axum::serve(listener, router()).await?;
    Ok(())
}

