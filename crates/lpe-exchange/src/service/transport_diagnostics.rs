use super::*;
use tracing::{info, warn};

pub(super) fn log_mapi_transport_connection(
    endpoint: MapiEndpoint,
    uri: &Uri,
    headers: &HeaderMap,
    request_body: &[u8],
    response: &Response,
    duration_ms: f64,
    error: Option<&str>,
) {
    let endpoint = match endpoint {
        MapiEndpoint::Emsmdb => "emsmdb",
        MapiEndpoint::Nspi => "nspi",
    };
    let status = response.status().as_u16();
    let mapi_response_code = response_header(response, "x-responsecode").unwrap_or_default();
    let mapi_request_id = response_header(response, "x-requestid")
        .or_else(|| mapi::safe_header(headers, "x-requestid"))
        .unwrap_or_default();
    let request_type = response_header(response, "x-requesttype")
        .or_else(|| mapi::safe_header(headers, "x-requesttype"))
        .unwrap_or_default();
    let mailbox_id = query_parameter(uri.query().unwrap_or_default(), "mailboxId");
    let client_request_id = mapi::safe_header(headers, "client-request-id").unwrap_or_default();
    let trace_id = mapi::safe_header(headers, "x-trace-id").unwrap_or_default();
    let user_agent = mapi::safe_header(headers, "user-agent").unwrap_or_default();
    let client_application = mapi::safe_header(headers, "x-clientapplication").unwrap_or_default();
    let client_info = mapi::safe_header(headers, "x-clientinfo").unwrap_or_default();
    let (request_guid, request_counter) = mapi::guid_counter_debug(&mapi_request_id);
    let (client_info_guid, client_info_counter) = mapi::guid_counter_debug(&client_info);
    let client_flow_key = mapi::client_flow_key(&client_info);
    let x_mapi_http_capability =
        mapi::safe_header(headers, "x-mapihttpcapability").unwrap_or_default();
    let request_content_type = mapi::safe_header(headers, "content-type").unwrap_or_default();
    let request_host = mapi::safe_header(headers, "host").unwrap_or_default();
    let response_payload_bytes = mapi::mapi_response_payload_bytes(response).unwrap_or(0);
    let request_body_bytes = request_body.len();
    let response_content_type = response_header(response, "content-type").unwrap_or_default();
    let response_www_authenticate =
        response_header(response, "www-authenticate").unwrap_or_default();
    let response_x_request_type = response_header(response, "x-requesttype").unwrap_or_default();
    let response_x_request_id = response_header(response, "x-requestid").unwrap_or_default();
    let response_x_expiration_info =
        response_header(response, "x-expirationinfo").unwrap_or_default();
    let response_x_pending_period =
        response_header(response, "x-pendingperiod").unwrap_or_default();
    let response_set_cookie_names = response_set_cookie_names(response);
    let cookie_debug = mapi::request_cookie_transport_debug(
        match endpoint {
            "emsmdb" => MapiEndpoint::Emsmdb,
            _ => MapiEndpoint::Nspi,
        },
        headers,
    );
    let message = "rca debug mapi transport connection";

    if status < 400 && mapi_response_code == "0" {
        tracing::debug!(
            rca_debug = true,
            adapter = "mapi",
            endpoint = endpoint,
            path = %uri.path(),
            query = %uri.query().unwrap_or_default(),
            mailbox_id = %mailbox_id.unwrap_or_default(),
            request_type = %request_type,
            mapi_request_id = %mapi_request_id,
            client_request_id = %client_request_id,
            trace_id = %trace_id,
            user_agent = %user_agent,
            client_application = %client_application,
            client_info = %client_info,
            client_flow_key = %client_flow_key,
            request_guid = %request_guid,
            request_counter = %request_counter,
            client_info_guid = %client_info_guid,
            client_info_counter = %client_info_counter,
            package_name = build_info::PACKAGE_NAME,
            package_version = build_info::PACKAGE_VERSION,
            git_commit = build_info::GIT_COMMIT,
            git_commit_full = build_info::GIT_COMMIT_FULL,
            git_commit_time = build_info::GIT_COMMIT_TIME,
            git_dirty = build_info::GIT_DIRTY,
            build_unix_time = build_info::BUILD_UNIX_TIME,
            target = build_info::TARGET,
            profile = build_info::PROFILE,
            x_mapi_http_capability = %x_mapi_http_capability,
            request_content_type = %request_content_type,
            request_host = %request_host,
            http_status = status,
            mapi_response_code = %mapi_response_code,
            request_body_bytes,
            response_payload_bytes,
            response_content_type = %response_content_type,
            response_www_authenticate = %response_www_authenticate,
            response_x_request_type = %response_x_request_type,
            response_x_request_id = %response_x_request_id,
            response_x_expiration_info = %response_x_expiration_info,
            response_x_pending_period = %response_x_pending_period,
            response_set_cookie_names = %response_set_cookie_names,
            cookie_header_count = cookie_debug.cookie_header_count,
            mapi_context_candidate_count = cookie_debug.context_candidate_count,
            mapi_sequence_candidate_count = cookie_debug.sequence_candidate_count,
            selected_context_suffix = %cookie_debug.selected_context_suffix,
            selected_context_hash = %cookie_debug.selected_context_hash,
            selected_sequence_suffix = %cookie_debug.selected_sequence_suffix,
            selected_sequence_hash = %cookie_debug.selected_sequence_hash,
            duration_ms,
            "{message}"
        );
    } else {
        warn!(
            rca_debug = true,
            adapter = "mapi",
            endpoint = endpoint,
            path = %uri.path(),
            query = %uri.query().unwrap_or_default(),
            mailbox_id = %mailbox_id.unwrap_or_default(),
            request_type = %request_type,
            mapi_request_id = %mapi_request_id,
            client_request_id = %client_request_id,
            trace_id = %trace_id,
            user_agent = %user_agent,
            client_application = %client_application,
            client_info = %client_info,
            client_flow_key = %client_flow_key,
            request_guid = %request_guid,
            request_counter = %request_counter,
            client_info_guid = %client_info_guid,
            client_info_counter = %client_info_counter,
            package_name = build_info::PACKAGE_NAME,
            package_version = build_info::PACKAGE_VERSION,
            git_commit = build_info::GIT_COMMIT,
            git_commit_full = build_info::GIT_COMMIT_FULL,
            git_commit_time = build_info::GIT_COMMIT_TIME,
            git_dirty = build_info::GIT_DIRTY,
            build_unix_time = build_info::BUILD_UNIX_TIME,
            target = build_info::TARGET,
            profile = build_info::PROFILE,
            x_mapi_http_capability = %x_mapi_http_capability,
            request_content_type = %request_content_type,
            request_host = %request_host,
            http_status = status,
            mapi_response_code = %mapi_response_code,
            request_body_bytes,
            response_payload_bytes,
            response_content_type = %response_content_type,
            response_www_authenticate = %response_www_authenticate,
            response_x_request_type = %response_x_request_type,
            response_x_request_id = %response_x_request_id,
            response_x_expiration_info = %response_x_expiration_info,
            response_x_pending_period = %response_x_pending_period,
            response_set_cookie_names = %response_set_cookie_names,
            cookie_header_count = cookie_debug.cookie_header_count,
            mapi_context_candidate_count = cookie_debug.context_candidate_count,
            mapi_sequence_candidate_count = cookie_debug.sequence_candidate_count,
            selected_context_suffix = %cookie_debug.selected_context_suffix,
            selected_context_hash = %cookie_debug.selected_context_hash,
            selected_sequence_suffix = %cookie_debug.selected_sequence_suffix,
            selected_sequence_hash = %cookie_debug.selected_sequence_hash,
            duration_ms,
            error = %error.unwrap_or_default(),
            "{message}"
        );
    }
}

pub(super) fn log_rpc_proxy_connection(
    method: &Method,
    uri: &Uri,
    headers: &HeaderMap,
    request_body: &[u8],
    response: &Response,
    duration_ms: f64,
) {
    let status = response.status().as_u16();
    let trace_id = mapi::safe_header(headers, "x-trace-id").unwrap_or_default();
    let user_agent = mapi::safe_header(headers, "user-agent").unwrap_or_default();
    let client_request_id = mapi::safe_header(headers, "client-request-id").unwrap_or_default();
    let x_request_id = mapi::safe_header(headers, "x-requestid").unwrap_or_default();
    let response_kind = response_header(response, RPC_PROXY_COMPAT_STATUS)
        .unwrap_or_else(|| "auth-challenge".into());
    let response_payload_bytes = rpc_proxy_response_payload_bytes(response).unwrap_or(0);
    let request_body_preview_hex = mapi::debug_payload_preview_hex(request_body);
    let response_payload_preview_hex =
        rpc_proxy_response_payload_preview_hex(response).unwrap_or_default();
    let message = "rca debug rpc proxy connection";

    if status < 400 {
        info!(
            rca_debug = true,
            adapter = "rpcproxy",
            method = %method,
            path = %uri.path(),
            query = %uri.query().unwrap_or_default(),
            response_kind = %response_kind,
            trace_id = %trace_id,
            client_request_id = %client_request_id,
            x_request_id = %x_request_id,
            http_status = status,
            request_body_bytes = request_body.len(),
            response_payload_bytes,
            request_body_preview_hex = %request_body_preview_hex,
            response_payload_preview_hex = %response_payload_preview_hex,
            duration_ms,
            user_agent = %user_agent,
            message
        );
    } else {
        warn!(
            rca_debug = true,
            adapter = "rpcproxy",
            method = %method,
            path = %uri.path(),
            query = %uri.query().unwrap_or_default(),
            response_kind = %response_kind,
            trace_id = %trace_id,
            client_request_id = %client_request_id,
            x_request_id = %x_request_id,
            http_status = status,
            request_body_bytes = request_body.len(),
            response_payload_bytes,
            request_body_preview_hex = %request_body_preview_hex,
            response_payload_preview_hex = %response_payload_preview_hex,
            duration_ms,
            user_agent = %user_agent,
            message
        );
    }
}

#[derive(Clone, Copy, Debug)]
pub(super) struct RpcProxyResponseDebug {
    pub(super) payload_bytes: usize,
}

#[derive(Clone, Debug)]
pub(super) struct RpcProxyResponsePayloadPreview {
    pub(super) hex: String,
}

fn rpc_proxy_response_payload_bytes(response: &Response) -> Option<usize> {
    response
        .extensions()
        .get::<RpcProxyResponseDebug>()
        .map(|debug| debug.payload_bytes)
}

fn rpc_proxy_response_payload_preview_hex(response: &Response) -> Option<&str> {
    response
        .extensions()
        .get::<RpcProxyResponsePayloadPreview>()
        .map(|preview| preview.hex.as_str())
}
