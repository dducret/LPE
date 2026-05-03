use axum::{
    body::Bytes,
    extract::{Query, State},
    http::{HeaderMap, StatusCode},
    response::Response,
    routing::{on, MethodFilter},
    Router,
};
use lpe_mail_auth::authenticate_account;
use lpe_storage::Storage;

use crate::{
    constants::ACTIVE_SYNC_PATH,
    response::{auth_challenge_response, empty_response, http_error},
    service::ActiveSyncService,
    store::ActiveSyncStore,
    types::ActiveSyncQuery,
};

pub fn router() -> Router<Storage> {
    Router::new().route(
        ACTIVE_SYNC_PATH,
        on(MethodFilter::OPTIONS, options_handler).post(post_handler),
    )
}

async fn options_handler(
    State(storage): State<Storage>,
    Query(query): Query<ActiveSyncQuery>,
    headers: HeaderMap,
) -> Response {
    options_response_for_store(&storage, &query, &headers).await
}

pub(crate) async fn options_response_for_store<S: ActiveSyncStore>(
    storage: &S,
    query: &ActiveSyncQuery,
    headers: &HeaderMap,
) -> Response {
    match authenticate_account(storage, query.user.as_deref(), headers, "activesync").await {
        Ok(_) => empty_response(),
        Err(_) => auth_challenge_response(),
    }
}

async fn post_handler(
    State(storage): State<Storage>,
    Query(query): Query<ActiveSyncQuery>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, (StatusCode, String)> {
    let service = ActiveSyncService::new(storage);
    service
        .handle_request(query, &headers, body.as_ref())
        .await
        .map_err(http_error)
}
