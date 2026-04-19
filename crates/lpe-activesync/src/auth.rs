use axum::http::HeaderMap;

use crate::constants::ACTIVE_SYNC_VERSION;

pub(crate) fn protocol_version(headers: &HeaderMap) -> String {
    headers
        .get("ms-asprotocolversion")
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(ACTIVE_SYNC_VERSION)
        .to_string()
}
