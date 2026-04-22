use anyhow::{bail, Result};
use axum::http::{HeaderMap, HeaderValue};

pub(crate) fn precondition_not_modified(headers: &HeaderMap, current_etag: &str) -> bool {
    match_condition_header(headers.get("if-none-match"), current_etag)
}

pub(crate) fn check_write_preconditions(
    headers: &HeaderMap,
    current_etag: Option<String>,
) -> Result<()> {
    if let Some(if_match) = headers.get("if-match") {
        let Some(current_etag) = current_etag.as_deref() else {
            bail!("precondition failed");
        };
        if !match_condition_header(Some(if_match), current_etag) {
            bail!("precondition failed");
        }
    }
    if let Some(if_none_match) = headers.get("if-none-match") {
        if let Some(current_etag) = current_etag.as_deref() {
            if match_condition_header(Some(if_none_match), current_etag) {
                bail!("precondition failed");
            }
        }
    }
    Ok(())
}

pub(crate) fn check_delete_preconditions(
    headers: &HeaderMap,
    current_etag: Option<String>,
) -> Result<()> {
    let Some(current_etag) = current_etag else {
        bail!("not found");
    };
    if let Some(if_match) = headers.get("if-match") {
        if !match_condition_header(Some(if_match), &current_etag) {
            bail!("precondition failed");
        }
    }
    if let Some(if_none_match) = headers.get("if-none-match") {
        if match_condition_header(Some(if_none_match), &current_etag) {
            bail!("precondition failed");
        }
    }
    Ok(())
}

fn match_condition_header(header_value: Option<&HeaderValue>, current_etag: &str) -> bool {
    let Some(header_value) = header_value.and_then(|value| value.to_str().ok()) else {
        return false;
    };
    header_value
        .split(',')
        .map(str::trim)
        .any(|candidate| candidate == "*" || candidate == current_etag)
}
