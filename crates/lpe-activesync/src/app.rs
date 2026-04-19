use axum::{
    body::Bytes,
    extract::{Query, State},
    http::{HeaderMap, StatusCode},
    response::Response,
    routing::{on, MethodFilter},
    Router,
};
use lpe_storage::Storage;

use crate::{
    constants::ACTIVE_SYNC_PATH,
    response::{empty_response, http_error},
    service::ActiveSyncService,
    types::ActiveSyncQuery,
};

pub fn router() -> Router<Storage> {
    Router::new().route(
        ACTIVE_SYNC_PATH,
        on(MethodFilter::OPTIONS, options_handler).post(post_handler),
    )
}

async fn options_handler() -> Response {
    empty_response()
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
