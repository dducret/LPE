use anyhow::{bail, Result};
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

pub(crate) fn ensure_supported_protocol_version(protocol_version: &str) -> Result<()> {
    if protocol_version == ACTIVE_SYNC_VERSION {
        Ok(())
    } else {
        bail!("unsupported ActiveSync protocol version")
    }
}
