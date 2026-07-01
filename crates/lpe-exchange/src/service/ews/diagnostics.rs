use axum::{
    http::{HeaderMap, Uri},
    response::Response,
};
use tracing::{info, warn};

use super::super::*;

pub(in crate::service) fn ews_operation_hint(headers: &HeaderMap, body: &[u8]) -> Option<String> {
    decode_ews_body(headers, body)
        .ok()
        .and_then(|decoded| operation_name(&decoded))
}

pub(in crate::service) fn log_ews_connection(
    uri: &Uri,
    headers: &HeaderMap,
    request_body_bytes: usize,
    operation: &str,
    ews_response_code: Option<&str>,
    response: &Response,
    duration_ms: f64,
    error: Option<&str>,
    debug_detail: Option<&str>,
) {
    let status = response.status().as_u16();
    let trace_id = mapi::safe_header(headers, "x-trace-id").unwrap_or_default();
    let user_agent = mapi::safe_header(headers, "user-agent").unwrap_or_default();
    let client_request_id = mapi::safe_header(headers, "client-request-id").unwrap_or_default();
    let x_request_id = mapi::safe_header(headers, "x-requestid").unwrap_or_default();
    let client_application = mapi::safe_header(headers, "x-clientapplication").unwrap_or_default();
    let message = "rca debug ews connection";

    if status < 400 {
        info!(
            rca_debug = true,
            adapter = "ews",
            path = %uri.path(),
            query = %uri.query().unwrap_or_default(),
            operation = %operation,
            ews_response_code = %ews_response_code.unwrap_or_default(),
            trace_id = %trace_id,
            client_request_id = %client_request_id,
            x_request_id = %x_request_id,
            client_application = %client_application,
            http_status = status,
            request_body_bytes,
            ews_debug_detail = %debug_detail.unwrap_or_default(),
            duration_ms,
            user_agent = %user_agent,
            "{message}"
        );
    } else {
        warn!(
            rca_debug = true,
            adapter = "ews",
            path = %uri.path(),
            query = %uri.query().unwrap_or_default(),
            operation = %operation,
            ews_response_code = %ews_response_code.unwrap_or_default(),
            trace_id = %trace_id,
            client_request_id = %client_request_id,
            x_request_id = %x_request_id,
            client_application = %client_application,
            http_status = status,
            request_body_bytes,
            ews_debug_detail = %debug_detail.unwrap_or_default(),
            duration_ms,
            user_agent = %user_agent,
            error = %error.unwrap_or_default(),
            "{message}"
        );
    }
}

#[derive(Clone, Debug)]
pub(in crate::service) struct EwsResponseDebug {
    pub(in crate::service) response_code: String,
    pub(in crate::service) detail: String,
}

pub(in crate::service) fn ews_response_code(response: &Response) -> Option<&str> {
    response
        .extensions()
        .get::<EwsResponseDebug>()
        .map(|debug| debug.response_code.as_str())
}

pub(in crate::service) fn ews_response_debug_detail(response: &Response) -> Option<&str> {
    response
        .extensions()
        .get::<EwsResponseDebug>()
        .map(|debug| debug.detail.as_str())
        .filter(|detail| !detail.is_empty())
}

pub(in crate::service) fn ews_payload_debug_detail(operation: &str, payload: &str) -> String {
    match operation {
        "CreateItem" => {
            let item_id = attribute_values_for_tag(payload, "ItemId", "Id")
                .into_iter()
                .next()
                .unwrap_or_default();
            let parent_folder_id = attribute_values_for_tag(payload, "ParentFolderId", "Id")
                .into_iter()
                .next()
                .unwrap_or_default();
            if item_id.is_empty() && parent_folder_id.is_empty() {
                String::new()
            } else {
                format!("created_item_id={item_id};parent_folder_id={parent_folder_id}")
            }
        }
        "SyncFolderItems" => {
            let sync_state = element_text(payload, "SyncState").unwrap_or_default();
            let creates = count_tag_occurrences(payload, "<t:Create>");
            let updates = count_tag_occurrences(payload, "<t:Update>");
            let deletes = count_tag_occurrences(payload, "<t:Delete>");
            format!("sync_state={sync_state};creates={creates};updates={updates};deletes={deletes}")
        }
        "GetEvents" => {
            let subscription_id = element_text(payload, "SubscriptionId").unwrap_or_default();
            let created = count_tag_occurrences(payload, "<t:CreatedEvent>");
            let new_mail = count_tag_occurrences(payload, "<t:NewMailEvent>");
            let deleted = count_tag_occurrences(payload, "<t:DeletedEvent>");
            let status = count_tag_occurrences(payload, "<t:StatusEvent>");
            format!("subscription_id={subscription_id};created={created};new_mail={new_mail};deleted={deleted};status={status}")
        }
        _ => String::new(),
    }
}
