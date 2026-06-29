use super::*;

pub(in crate::mapi::dispatch) fn log_execute_dispatch_start_debug(
    endpoint: MapiEndpoint,
    principal: &AccountPrincipal,
    _headers: &HeaderMap,
    request_id: &str,
    mailbox_count: usize,
    email_count: usize,
) {
    let endpoint = match endpoint {
        MapiEndpoint::Emsmdb => "emsmdb",
        MapiEndpoint::Nspi => "nspi",
    };
    let message = "rca debug mapi execute dispatch start";

    tracing::debug!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = endpoint,
        tenant_id = %principal.tenant_id,
        account_id = %principal.account_id,
        mailbox = %principal.email,
        request_type = "Execute",
        mapi_request_id = request_id,
        mailbox_count = mailbox_count,
        email_count = email_count,
        message = message,
    );
}

pub(in crate::mapi::dispatch) fn log_execute_parse_failure_debug(
    endpoint: MapiEndpoint,
    principal: &AccountPrincipal,
    headers: &HeaderMap,
    request_id: &str,
    body: &[u8],
    error: &anyhow::Error,
) {
    let client_request_id = safe_header(headers, "client-request-id").unwrap_or_default();
    let client_application = safe_header(headers, "x-clientapplication").unwrap_or_default();
    let client_info = safe_header(headers, "x-clientinfo").unwrap_or_default();
    let trace_id = safe_header(headers, "x-trace-id").unwrap_or_default();
    let endpoint = match endpoint {
        MapiEndpoint::Emsmdb => "emsmdb",
        MapiEndpoint::Nspi => "nspi",
    };
    let flags = read_le_u32_at(body, 0);
    let rop_buffer_size = read_le_u32_at(body, 4);
    let rop_buffer_end = rop_buffer_size.and_then(|size| 8usize.checked_add(size as usize));
    let max_rop_out = rop_buffer_end.and_then(|offset| read_le_u32_at(body, offset));
    let auxiliary_buffer_size =
        rop_buffer_end.and_then(|offset| read_le_u32_at(body, offset.saturating_add(4)));
    let expected_body_bytes = match (rop_buffer_end, auxiliary_buffer_size) {
        (Some(offset), Some(auxiliary_buffer_size)) => offset
            .checked_add(8)
            .and_then(|offset| offset.checked_add(auxiliary_buffer_size as usize)),
        _ => None,
    };
    tracing::warn!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = endpoint,
        tenant_id = %principal.tenant_id,
        account_id = %principal.account_id,
        mailbox = %principal.email,
        request_type = "Execute",
        mapi_request_id = request_id,
        client_request_id = %client_request_id,
        client_application = %client_application,
        client_info = %client_info,
        trace_id = %trace_id,
        request_body_bytes = body.len(),
        execute_flags = flags.map(format_hex_u32).unwrap_or_default(),
        declared_rop_buffer_size = rop_buffer_size
            .map(|value| value.to_string())
            .unwrap_or_default(),
        max_rop_out = max_rop_out.map(|value| value.to_string()).unwrap_or_default(),
        declared_auxiliary_buffer_size = auxiliary_buffer_size
            .map(|value| value.to_string())
            .unwrap_or_default(),
        expected_body_bytes = expected_body_bytes
            .map(|value| value.to_string())
            .unwrap_or_default(),
        body_preview_hex = %debug_payload_preview_hex(body),
        parse_error = %error,
        "rca debug mapi execute parse failure"
    );
}

fn read_le_u32_at(bytes: &[u8], offset: usize) -> Option<u32> {
    let value = bytes.get(offset..offset.checked_add(4)?)?;
    Some(u32::from_le_bytes(value.try_into().ok()?))
}

fn format_hex_u32(value: u32) -> String {
    format!("0x{value:08x}")
}
