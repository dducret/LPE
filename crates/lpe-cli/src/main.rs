use anyhow::Result;
use lpe_admin_api::router;
use lpe_storage::Storage;
use std::env;
use tokio::net::TcpListener;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let bind_address =
        env::var("LPE_BIND_ADDRESS").unwrap_or_else(|_| "127.0.0.1:8080".to_string());
    let database_url = env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://lpe:change-me@localhost:5432/lpe".to_string());
    let storage = Storage::connect(&database_url).await?;
    let listener = TcpListener::bind(&bind_address).await?;
    info!("lpe admin api listening on http://{bind_address}");

    axum::serve(listener, router(storage)).await?;
    Ok(())
}
