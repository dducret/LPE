use axum::{routing::get, Json, Router};
use lpe_ai::{LocalModelProvider, StubLocalModelProvider};
use lpe_attachments::AttachmentFormat;
use lpe_core::CoreService;
use serde::Serialize;
use uuid::Uuid;

#[derive(Debug, Serialize)]
struct HealthResponse {
    service: &'static str,
    status: &'static str,
}

#[derive(Debug, Serialize)]
struct BootstrapResponse {
    email: String,
    display_name: String,
}

#[derive(Debug, Serialize)]
struct LocalAiHealthResponse {
    provider: &'static str,
    models: Vec<String>,
    bootstrap_summary_payload: String,
}

#[derive(Debug, Serialize)]
struct AttachmentSupportResponse {
    formats: Vec<&'static str>,
}

pub fn router() -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/bootstrap/admin", get(bootstrap_admin))
        .route("/health/local-ai", get(local_ai_health))
        .route("/capabilities/attachments", get(attachment_support))
}

async fn health() -> Json<HealthResponse> {
    Json(HealthResponse {
        service: "lpe-admin-api",
        status: "ok",
    })
}

async fn bootstrap_admin() -> Json<BootstrapResponse> {
    let core = CoreService;
    let account = core
        .bootstrap_admin_account()
        .expect("bootstrap admin account");

    Json(BootstrapResponse {
        email: account.primary_email,
        display_name: account.display_name,
    })
}

async fn local_ai_health() -> Json<LocalAiHealthResponse> {
    let provider = StubLocalModelProvider;
    let core = CoreService;
    let models = provider
        .describe_models()
        .into_iter()
        .map(|model| model.id)
        .collect();
    let bootstrap_summary_payload = core
        .summarize_bootstrap_projection(&provider, Uuid::new_v4())
        .expect("bootstrap summary");

    Json(LocalAiHealthResponse {
        provider: "stub-local",
        models,
        bootstrap_summary_payload,
    })
}

async fn attachment_support() -> Json<AttachmentSupportResponse> {
    let core = CoreService;
    let formats = core
        .supported_attachment_formats()
        .into_iter()
        .map(|format| match format {
            AttachmentFormat::Pdf => "pdf",
            AttachmentFormat::Docx => "docx",
            AttachmentFormat::Odt => "odt",
        })
        .collect();

    Json(AttachmentSupportResponse { formats })
}
