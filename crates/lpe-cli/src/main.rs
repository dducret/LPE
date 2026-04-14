use anyhow::Result;
use lpe_admin_api::router;
use tokio::net::TcpListener;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let listener = TcpListener::bind("127.0.0.1:8080").await?;
    info!("lpe admin api listening on http://127.0.0.1:8080");

    axum::serve(listener, router()).await?;
    Ok(())
}

