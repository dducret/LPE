use anyhow::Result;
use lpe_admin_api::{
    bootstrap_admin, bootstrap_admin_request_from_env, integration_shared_secret, router,
};
use lpe_domain::{OutboundMessageHandoffRequest, OutboundMessageHandoffResponse};
use lpe_imap::ImapServer;
use lpe_storage::Storage;
use std::{env, time::Duration};
use tokio::{net::TcpListener, time::sleep};
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    if env::args().nth(1).as_deref() == Some("bootstrap-admin") {
        return run_bootstrap_admin_command().await;
    }

    let bind_address =
        env::var("LPE_BIND_ADDRESS").unwrap_or_else(|_| "127.0.0.1:8080".to_string());
    let imap_bind_address =
        env::var("LPE_IMAP_BIND_ADDRESS").unwrap_or_else(|_| "127.0.0.1:1143".to_string());
    let database_url = env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://lpe:change-me@localhost:5432/lpe".to_string());
    integration_shared_secret()?;
    let storage = Storage::connect(&database_url).await?;
    let listener = TcpListener::bind(&bind_address).await?;
    let imap_listener = TcpListener::bind(&imap_bind_address).await?;
    info!("lpe admin api listening on http://{bind_address}");
    info!("lpe imap listening on {imap_bind_address}");

    let api_storage = storage.clone();
    let worker_storage = storage.clone();
    let imap_storage = storage.clone();
    let api_task = tokio::spawn(async move {
        axum::serve(listener, router(api_storage)).await?;
        Result::<()>::Ok(())
    });
    let imap_task =
        tokio::spawn(async move { ImapServer::new(imap_storage).serve(imap_listener).await });
    let worker_task = tokio::spawn(async move { run_outbound_worker(worker_storage).await });

    tokio::select! {
        result = api_task => result??,
        result = imap_task => result??,
        result = worker_task => result??,
    }

    Ok(())
}

async fn run_bootstrap_admin_command() -> Result<()> {
    let database_url = env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://lpe:change-me@localhost:5432/lpe".to_string());
    let storage = Storage::connect(&database_url).await?;
    let request = bootstrap_admin_request_from_env()?;
    let result = bootstrap_admin(&storage, request).await?;
    info!(
        "bootstrap administrator created for {} ({})",
        result.email, result.display_name
    );
    Ok(())
}

async fn run_outbound_worker(storage: Storage) -> Result<()> {
    let base_url = env::var("LPE_CT_API_BASE_URL")
        .unwrap_or_else(|_| "http://127.0.0.1:8380".to_string())
        .trim_end_matches('/')
        .to_string();
    let integration_key = integration_shared_secret()?;
    let interval_ms = env::var("LPE_OUTBOUND_WORKER_INTERVAL_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(5_000)
        .max(250);
    let batch_size = env::var("LPE_OUTBOUND_WORKER_BATCH_SIZE")
        .ok()
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or(10)
        .max(1);
    let client = reqwest::Client::builder().build()?;

    info!(
        "lpe outbound worker active against {base_url} with interval {} ms",
        interval_ms
    );

    loop {
        let batch = storage.fetch_outbound_handoff_batch(batch_size).await?;
        if batch.is_empty() {
            sleep(Duration::from_millis(interval_ms)).await;
            continue;
        }

        for item in batch {
            dispatch_outbound_message(&storage, &client, &base_url, &integration_key, item).await;
        }

        sleep(Duration::from_millis(interval_ms)).await;
    }
}

async fn dispatch_outbound_message(
    storage: &Storage,
    client: &reqwest::Client,
    base_url: &str,
    integration_key: &str,
    item: OutboundMessageHandoffRequest,
) {
    let endpoint = format!("{base_url}/api/v1/integration/outbound-messages");
    let queue_id = item.queue_id;
    let subject = item.subject.clone();

    match send_outbound_handoff(client, &endpoint, integration_key, &item).await {
        Ok(response) => {
            let status = response.status.clone();
            if let Err(error) = storage.update_outbound_queue_status(&response).await {
                warn!("unable to persist outbound status for {queue_id}: {error}");
            } else {
                info!(
                    "outbound queue {queue_id} updated to {} for subject {:?}",
                    status.as_str(),
                    subject
                );
            }
        }
        Err(error) => {
            warn!("outbound handoff failed for {queue_id}: {error}");
            if let Err(update_error) = storage
                .mark_outbound_queue_attempt_failure(queue_id, &error)
                .await
            {
                warn!("unable to mark queue {queue_id} as deferred: {update_error}");
            }
        }
    }
}

async fn send_outbound_handoff(
    client: &reqwest::Client,
    endpoint: &str,
    integration_key: &str,
    item: &OutboundMessageHandoffRequest,
) -> std::result::Result<OutboundMessageHandoffResponse, String> {
    let response = client
        .post(endpoint)
        .header("x-lpe-integration-key", integration_key)
        .json(item)
        .send()
        .await
        .map_err(|error| error.to_string())?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(format!("handoff endpoint returned {status}: {body}"));
    }

    response.json().await.map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use super::send_outbound_handoff;
    use axum::{extract::State, http::HeaderMap, routing::post, Json, Router};
    use lpe_domain::{
        OutboundMessageHandoffRequest, OutboundMessageHandoffResponse, TransportDeliveryStatus,
        TransportRecipient,
    };
    use std::sync::{Arc, Mutex};
    use tokio::net::TcpListener;
    use uuid::Uuid;

    #[tokio::test]
    async fn handoff_client_posts_json_and_header() {
        #[derive(Clone, Default)]
        struct Capture {
            header: Arc<Mutex<Option<String>>>,
            queue_id: Arc<Mutex<Option<Uuid>>>,
        }

        async fn accept(
            State(capture): State<Capture>,
            headers: HeaderMap,
            Json(request): Json<OutboundMessageHandoffRequest>,
        ) -> Json<OutboundMessageHandoffResponse> {
            *capture.header.lock().unwrap() = headers
                .get("x-lpe-integration-key")
                .and_then(|value| value.to_str().ok())
                .map(ToString::to_string);
            *capture.queue_id.lock().unwrap() = Some(request.queue_id);
            Json(OutboundMessageHandoffResponse {
                queue_id: request.queue_id,
                status: TransportDeliveryStatus::Relayed,
                trace_id: "ct-trace-1".to_string(),
                detail: None,
                remote_message_ref: Some("remote-1".to_string()),
                retry: None,
                dsn: None,
                technical: None,
                route: None,
                throttle: None,
            })
        }

        let capture = Capture::default();
        let router = Router::new()
            .route("/api/v1/integration/outbound-messages", post(accept))
            .with_state(capture.clone());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });

        let request = OutboundMessageHandoffRequest {
            queue_id: Uuid::new_v4(),
            message_id: Uuid::new_v4(),
            account_id: Uuid::new_v4(),
            from_address: "sender@example.test".to_string(),
            from_display: None,
            to: vec![TransportRecipient {
                address: "dest@example.test".to_string(),
                display_name: None,
            }],
            cc: Vec::new(),
            bcc: Vec::new(),
            subject: "Subject".to_string(),
            body_text: "Body".to_string(),
            body_html_sanitized: None,
            internet_message_id: None,
            attempt_count: 0,
            last_attempt_error: None,
        };
        let client = reqwest::Client::builder().build().unwrap();
        let response = send_outbound_handoff(
            &client,
            &format!("http://{address}/api/v1/integration/outbound-messages"),
            "shared-secret",
            &request,
        )
        .await
        .unwrap();

        assert_eq!(response.status, TransportDeliveryStatus::Relayed);
        assert_eq!(response.trace_id, "ct-trace-1");
        assert_eq!(response.remote_message_ref.as_deref(), Some("remote-1"));
        assert_eq!(
            capture.header.lock().unwrap().as_deref(),
            Some("shared-secret")
        );
        assert_eq!(*capture.queue_id.lock().unwrap(), Some(request.queue_id));
    }
}
