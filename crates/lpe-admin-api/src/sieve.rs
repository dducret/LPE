use crate::{
    bad_request_error,
    http::internal_error,
    require_account,
    types::{
        ApiResult, RenameSieveScriptRequest, SetActiveSieveScriptRequest, SieveOverviewResponse,
        UpsertSieveScriptRequest,
    },
};
use axum::{
    extract::{Path as AxumPath, State},
    http::{HeaderMap, StatusCode},
    Json,
};
use lpe_storage::{AuditEntryInput, HealthResponse, SieveScriptDocument, Storage};

pub(crate) async fn get_sieve_overview(
    State(storage): State<Storage>,
    headers: HeaderMap,
) -> ApiResult<SieveOverviewResponse> {
    let account = require_account(&storage, &headers).await?;
    let scripts = storage
        .list_sieve_scripts(account.account_id)
        .await
        .map_err(internal_error)?;
    let active_script = storage
        .fetch_active_sieve_script(account.account_id)
        .await
        .map_err(internal_error)?;
    Ok(Json(SieveOverviewResponse {
        scripts,
        active_script,
    }))
}

pub(crate) async fn get_sieve_script(
    State(storage): State<Storage>,
    headers: HeaderMap,
    AxumPath(name): AxumPath<String>,
) -> ApiResult<SieveScriptDocument> {
    let account = require_account(&storage, &headers).await?;
    let script = storage
        .get_sieve_script(account.account_id, &name)
        .await
        .map_err(bad_request_error)?
        .ok_or((StatusCode::NOT_FOUND, "sieve script not found".to_string()))?;
    Ok(Json(script))
}

pub(crate) async fn put_sieve_script(
    State(storage): State<Storage>,
    headers: HeaderMap,
    Json(request): Json<UpsertSieveScriptRequest>,
) -> ApiResult<SieveScriptDocument> {
    let account = require_account(&storage, &headers).await?;
    let subject = request.name.clone();
    Ok(Json(
        storage
            .put_sieve_script(
                account.account_id,
                &request.name,
                &request.content,
                request.activate,
                AuditEntryInput {
                    actor: account.email,
                    action: "sieve-script-upsert".to_string(),
                    subject,
                },
            )
            .await
            .map_err(bad_request_error)?,
    ))
}

pub(crate) async fn rename_sieve_script(
    State(storage): State<Storage>,
    headers: HeaderMap,
    Json(request): Json<RenameSieveScriptRequest>,
) -> ApiResult<lpe_storage::SieveScriptSummary> {
    let account = require_account(&storage, &headers).await?;
    Ok(Json(
        storage
            .rename_sieve_script(
                account.account_id,
                &request.old_name,
                &request.new_name,
                AuditEntryInput {
                    actor: account.email,
                    action: "sieve-script-rename".to_string(),
                    subject: format!("{} -> {}", request.old_name, request.new_name),
                },
            )
            .await
            .map_err(bad_request_error)?,
    ))
}

pub(crate) async fn set_active_sieve_script(
    State(storage): State<Storage>,
    headers: HeaderMap,
    Json(request): Json<SetActiveSieveScriptRequest>,
) -> ApiResult<HealthResponse> {
    let account = require_account(&storage, &headers).await?;
    let subject = request.name.clone().unwrap_or_else(|| "none".to_string());
    storage
        .set_active_sieve_script(
            account.account_id,
            request.name.as_deref(),
            AuditEntryInput {
                actor: account.email,
                action: "sieve-script-activate".to_string(),
                subject,
            },
        )
        .await
        .map_err(bad_request_error)?;
    Ok(Json(HealthResponse {
        service: "lpe-admin-api",
        status: "ok",
    }))
}

pub(crate) async fn delete_sieve_script(
    State(storage): State<Storage>,
    headers: HeaderMap,
    AxumPath(name): AxumPath<String>,
) -> ApiResult<HealthResponse> {
    let account = require_account(&storage, &headers).await?;
    let subject = name.clone();
    storage
        .delete_sieve_script(
            account.account_id,
            &name,
            AuditEntryInput {
                actor: account.email,
                action: "sieve-script-delete".to_string(),
                subject,
            },
        )
        .await
        .map_err(bad_request_error)?;
    Ok(Json(HealthResponse {
        service: "lpe-admin-api",
        status: "ok",
    }))
}
