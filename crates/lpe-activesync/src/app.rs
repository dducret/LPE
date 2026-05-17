use axum::{
    body::Bytes,
    extract::{RawQuery, State},
    http::HeaderMap,
    response::Response,
    routing::{on, MethodFilter},
    Router,
};
use lpe_mail_auth::authenticate_account;
use lpe_storage::Storage;

use crate::{
    constants::ACTIVE_SYNC_PATH,
    response::{auth_challenge_response, empty_response, error_response},
    service::ActiveSyncService,
    store::ActiveSyncStore,
    types::{ActiveSyncQuery, ParsedActiveSyncQuery},
};

pub fn router() -> Router<Storage> {
    Router::new().route(
        ACTIVE_SYNC_PATH,
        on(MethodFilter::OPTIONS, options_handler).post(post_handler),
    )
}

async fn options_handler(
    State(storage): State<Storage>,
    RawQuery(raw_query): RawQuery,
    headers: HeaderMap,
) -> Response {
    let parsed = match ParsedActiveSyncQuery::from_raw_query(raw_query.as_deref()) {
        Ok(parsed) => parsed,
        Err(error) => return error_response(error),
    };
    options_response_for_store(&storage, &parsed.query, &headers).await
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
    RawQuery(raw_query): RawQuery,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let parsed = match ParsedActiveSyncQuery::from_raw_query(raw_query.as_deref()) {
        Ok(parsed) => parsed,
        Err(error) => return error_response(error),
    };
    let service = ActiveSyncService::new(storage);
    match service
        .handle_parsed_request(parsed, &headers, body.as_ref())
        .await
    {
        Ok(response) => response,
        Err(error) => error_response(error),
    }
}
