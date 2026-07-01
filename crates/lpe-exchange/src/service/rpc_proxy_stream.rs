use std::time::{Duration, Instant};

use anyhow::Result;
use axum::{
    body::{Body, Bytes},
    http::{
        header::{CONNECTION, CONTENT_LENGTH, CONTENT_TYPE},
        HeaderMap, HeaderValue, Method, StatusCode, Uri,
    },
    response::{IntoResponse, Response},
};
use lpe_magika::{Detector, Validator};
use lpe_mail_auth::AccountPrincipal;
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tracing::{info, warn};

use crate::{mapi, store::ExchangeStore};

use super::rpc_proxy_channels::*;
use super::rpc_proxy_codec::read_le_u32;
use super::rpc_proxy_dce::*;
use super::rpc_proxy_endpoints::*;
use super::rpc_proxy_requests::is_rpc_proxy_endpoint_query;
use super::rpc_proxy_rts::*;
use super::transport_diagnostics::{RpcProxyResponseDebug, RpcProxyResponsePayloadPreview};
use super::{RPC_PROXY_COMPAT_STATUS, RPC_PROXY_RECEIVE_WINDOW_SIZE};

const RPC_PROXY_ECHO_STATUS: &str = "echo";
const RPC_PROXY_IN_CHANNEL_STATUS: &str = "in-channel-open";
const RPC_PROXY_RTS_CONNECT_STATUS: &str = "rts-connect";
const RPC_PROXY_ENDPOINT_PING_STATUS: &str = "endpoint-ping";
const RPC_PROXY_OUT_CHANNEL_CONTENT_LENGTH: u32 = 0x0002_0000;
const RPC_PROXY_ECHO_BODY: [u8; 20] = [
    0x05, 0x00, 0x14, 0x03, 0x10, 0x00, 0x00, 0x00, 0x14, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x40, 0x00, 0x00, 0x00,
];
pub(super) fn rpc_proxy_rts_connect_response(client_receive_window_size: u32) -> Response {
    rpc_proxy_binary_response(
        rpc_proxy_rts_connect_body(client_receive_window_size),
        RPC_PROXY_RTS_CONNECT_STATUS,
    )
}

pub(super) fn rpc_proxy_mailstore_ping_response_for_connect(
    uri: &Uri,
    connect: RpcProxyOutDataConnect,
) -> Response {
    rpc_proxy_mailstore_held_open_response(
        uri,
        rpc_proxy_endpoint_connect_body(),
        Some(connect.virtual_connection_cookie),
    )
}

pub(super) fn rpc_proxy_echo_response() -> Response {
    rpc_proxy_binary_response(RPC_PROXY_ECHO_BODY.to_vec(), RPC_PROXY_ECHO_STATUS)
}

pub(super) fn rpc_proxy_in_channel_response(uri: &Uri) -> Response {
    if should_hold_rpc_proxy_in_channel(uri) {
        return rpc_proxy_held_open_binary_response(
            Vec::new(),
            RPC_PROXY_IN_CHANNEL_STATUS,
            rpc_proxy_channel_hold_ms(),
            false,
            true,
        );
    }

    let mut response = StatusCode::OK.into_response();
    response
        .headers_mut()
        .insert(CONTENT_LENGTH, HeaderValue::from_static("0"));
    decorate_rpc_proxy_binary_response(
        &mut response,
        0,
        String::new(),
        RPC_PROXY_IN_CHANNEL_STATUS,
    );
    response
}

fn rpc_proxy_mailstore_held_open_response(
    uri: &Uri,
    body: Vec<u8>,
    virtual_connection_cookie: Option<[u8; 16]>,
) -> Response {
    let Some(query) = uri.query() else {
        return rpc_proxy_binary_response(body, RPC_PROXY_ENDPOINT_PING_STATUS);
    };
    let hold_open_ms = rpc_proxy_channel_hold_ms();
    if hold_open_ms == 0 {
        return rpc_proxy_binary_response(body, RPC_PROXY_ENDPOINT_PING_STATUS);
    }

    let mut body = body;
    let query = query.to_string();
    let (sender, receiver) = tokio::sync::mpsc::unbounded_channel::<Bytes>();
    register_rpc_proxy_out_channel(&query, virtual_connection_cookie, sender);

    let pending =
        consume_pending_rpc_proxy_out_channel_responses(&query, virtual_connection_cookie);
    let has_pending = !pending.is_empty();
    body.extend(pending);
    if !has_pending && rpc_proxy_should_send_synthetic_rts_connect(&query) {
        body.extend_from_slice(&rpc_proxy_connection_established_pdu(
            RPC_PROXY_RECEIVE_WINDOW_SIZE,
        ));
        mark_rpc_proxy_out_endpoint_rts_connect(&query);
    }
    if has_pending && query.contains(":6001") {
        body.extend_from_slice(&rpc_proxy_dce_bind_ack_body_with_result_count(1, 1));
        mark_rpc_proxy_out_endpoint_bind_ack(&query);
    }

    let payload_bytes = body.len();
    let payload_preview_hex = mapi::debug_payload_preview_hex(&body);

    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(hold_open_ms)).await;
        remove_rpc_proxy_out_channel(&query, virtual_connection_cookie);
    });

    let initial = Some(Ok::<Bytes, std::io::Error>(Bytes::from(body)));
    let followups = tokio_stream::wrappers::UnboundedReceiverStream::new(receiver).map(Ok);
    let stream = tokio_stream::iter(initial).chain(followups);
    let mut response = Response::new(Body::from_stream(stream));
    decorate_rpc_proxy_binary_response(
        &mut response,
        payload_bytes,
        payload_preview_hex,
        RPC_PROXY_ENDPOINT_PING_STATUS,
    );
    response.headers_mut().insert(
        CONTENT_LENGTH,
        HeaderValue::from_str(&RPC_PROXY_OUT_CHANNEL_CONTENT_LENGTH.to_string())
            .unwrap_or_else(|_| HeaderValue::from_static("131072")),
    );
    response
}

fn should_hold_rpc_proxy_in_channel(uri: &Uri) -> bool {
    let Some(_) = uri
        .query()
        .filter(|query| is_rpc_proxy_endpoint_query(query))
    else {
        return false;
    };
    let hold_open_ms = rpc_proxy_channel_hold_ms();
    if hold_open_ms == 0 {
        return false;
    }
    true
}

fn rpc_proxy_binary_response(body: Vec<u8>, status: &'static str) -> Response {
    if (status == RPC_PROXY_RTS_CONNECT_STATUS || status == RPC_PROXY_ENDPOINT_PING_STATUS)
        && rpc_proxy_channel_hold_ms() > 0
    {
        return rpc_proxy_held_open_binary_response(
            body,
            status,
            rpc_proxy_channel_hold_ms(),
            true,
            true,
        );
    }

    let payload_bytes = body.len();
    let payload_preview_hex = mapi::debug_payload_preview_hex(&body);
    let mut response = (StatusCode::OK, body).into_response();
    decorate_rpc_proxy_binary_response(&mut response, payload_bytes, payload_preview_hex, status);
    response
}

fn rpc_proxy_held_open_binary_response(
    body: Vec<u8>,
    status: &'static str,
    hold_open_ms: u64,
    send_initial_body: bool,
    include_content_length: bool,
) -> Response {
    let payload_bytes = body.len();
    let payload_preview_hex = mapi::debug_payload_preview_hex(&body);
    let (sender, receiver) = tokio::sync::mpsc::channel::<Result<Bytes, std::io::Error>>(1);
    tokio::spawn(async move {
        if send_initial_body {
            let _ = sender.send(Ok(Bytes::from(body))).await;
        }
        tokio::time::sleep(Duration::from_millis(hold_open_ms)).await;
    });

    let mut response = Response::new(Body::from_stream(ReceiverStream::new(receiver)));
    decorate_rpc_proxy_binary_response(&mut response, payload_bytes, payload_preview_hex, status);
    if include_content_length {
        response.headers_mut().insert(
            CONTENT_LENGTH,
            HeaderValue::from_str(&RPC_PROXY_OUT_CHANNEL_CONTENT_LENGTH.to_string())
                .unwrap_or_else(|_| HeaderValue::from_static("131072")),
        );
    }
    response
}

fn decorate_rpc_proxy_binary_response(
    response: &mut Response,
    payload_bytes: usize,
    payload_preview_hex: String,
    status: &'static str,
) {
    response
        .extensions_mut()
        .insert(RpcProxyResponseDebug { payload_bytes });
    if !payload_preview_hex.is_empty() {
        response
            .extensions_mut()
            .insert(RpcProxyResponsePayloadPreview {
                hex: payload_preview_hex,
            });
    }
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("application/rpc"));
    response
        .headers_mut()
        .insert(CONNECTION, HeaderValue::from_static("Keep-Alive"));
    response
        .headers_mut()
        .insert(RPC_PROXY_COMPAT_STATUS, HeaderValue::from_static(status));
}

pub(super) fn spawn_rpc_proxy_in_data_drain<S, V>(
    store: S,
    validator: Validator<V>,
    principal: AccountPrincipal,
    method: &Method,
    uri: &Uri,
    headers: &HeaderMap,
    body: Body,
) where
    S: ExchangeStore + Send + Sync + 'static,
    V: Detector + Send + Sync + 'static,
{
    let method = method.to_string();
    let path = uri.path().to_string();
    let query = uri.query().unwrap_or_default().to_string();
    let trace_id = mapi::safe_header(headers, "x-trace-id").unwrap_or_default();
    let client_request_id = mapi::safe_header(headers, "client-request-id").unwrap_or_default();
    let x_request_id = mapi::safe_header(headers, "x-requestid").unwrap_or_default();
    let user_agent = mapi::safe_header(headers, "user-agent").unwrap_or_default();

    tokio::spawn(async move {
        let started_at = Instant::now();
        info!(
            rca_debug = true,
            adapter = "rpcproxy",
            method = %method,
            path = %path,
            query = %query,
            response_kind = "in-channel-open",
            trace_id = %trace_id,
            client_request_id = %client_request_id,
            x_request_id = %x_request_id,
            http_status = 200u16,
            request_body_bytes = 0usize,
            response_payload_bytes = 0usize,
            request_body_preview_hex = "",
            response_payload_preview_hex = "",
            duration_ms = 0.0f64,
            user_agent = %user_agent,
            message = "rca debug rpc proxy in data stream opened"
        );

        let mut stream = body.into_data_stream();
        let mut pdu_buffer = Vec::new();
        let mut total_body_bytes = 0usize;
        let mut virtual_connection_cookie = None;
        while let Some(chunk) = stream.next().await {
            match chunk {
                Ok(bytes) => {
                    total_body_bytes += bytes.len();
                    let request_body_preview_hex = mapi::debug_payload_preview_hex(bytes.as_ref());
                    pdu_buffer.extend_from_slice(bytes.as_ref());
                    while let Some(response) =
                        rpc_proxy_in_channel_response_for_endpoint_query_with_store_response(
                            &store,
                            &validator,
                            &principal,
                            &query,
                            &mut pdu_buffer,
                        )
                        .await
                    {
                        log_and_forward_rpc_proxy_in_channel_response(
                            &method,
                            &path,
                            &query,
                            &trace_id,
                            &client_request_id,
                            &x_request_id,
                            &user_agent,
                            started_at,
                            &mut virtual_connection_cookie,
                            response,
                        );
                    }
                    info!(
                        rca_debug = true,
                        adapter = "rpcproxy",
                        method = %method,
                        path = %path,
                        query = %query,
                        response_kind = "in-channel-data",
                        trace_id = %trace_id,
                        client_request_id = %client_request_id,
                        x_request_id = %x_request_id,
                        http_status = 200u16,
                        request_body_bytes = bytes.len(),
                        total_request_body_bytes = total_body_bytes,
                        request_body_preview_hex = %request_body_preview_hex,
                        duration_ms = started_at.elapsed().as_secs_f64() * 1000.0,
                        user_agent = %user_agent,
                        message = "rca debug rpc proxy in data chunk"
                    );
                }
                Err(error) => {
                    while let Some(response) =
                        rpc_proxy_in_channel_response_for_endpoint_query_with_store_response(
                            &store,
                            &validator,
                            &principal,
                            &query,
                            &mut pdu_buffer,
                        )
                        .await
                    {
                        log_and_forward_rpc_proxy_in_channel_response(
                            &method,
                            &path,
                            &query,
                            &trace_id,
                            &client_request_id,
                            &x_request_id,
                            &user_agent,
                            started_at,
                            &mut virtual_connection_cookie,
                            response,
                        );
                    }
                    let pending_request_body_bytes = pdu_buffer.len();
                    let pending_request_body_preview_hex =
                        mapi::debug_payload_preview_hex(&pdu_buffer);
                    warn!(
                        rca_debug = true,
                        adapter = "rpcproxy",
                        method = %method,
                        path = %path,
                        query = %query,
                        response_kind = "in-channel-error",
                        trace_id = %trace_id,
                        client_request_id = %client_request_id,
                        x_request_id = %x_request_id,
                        http_status = 200u16,
                        total_request_body_bytes = total_body_bytes,
                        pending_request_body_bytes = pending_request_body_bytes,
                        pending_request_body_preview_hex = %pending_request_body_preview_hex,
                        duration_ms = started_at.elapsed().as_secs_f64() * 1000.0,
                        user_agent = %user_agent,
                        error = %error,
                        message = "rca debug rpc proxy in data stream error"
                    );
                    return;
                }
            }
        }

        info!(
            rca_debug = true,
            adapter = "rpcproxy",
            method = %method,
            path = %path,
            query = %query,
            response_kind = "in-channel-finished",
            trace_id = %trace_id,
            client_request_id = %client_request_id,
            x_request_id = %x_request_id,
            http_status = 200u16,
            total_request_body_bytes = total_body_bytes,
            duration_ms = started_at.elapsed().as_secs_f64() * 1000.0,
            user_agent = %user_agent,
            message = "rca debug rpc proxy in data stream finished"
        );
    });
}

#[allow(clippy::too_many_arguments)]
fn log_and_forward_rpc_proxy_in_channel_response(
    method: &str,
    path: &str,
    query: &str,
    trace_id: &str,
    client_request_id: &str,
    x_request_id: &str,
    user_agent: &str,
    started_at: Instant,
    virtual_connection_cookie: &mut Option<[u8; 16]>,
    response: RpcProxyInChannelResponse,
) {
    let response_payload_bytes = response.bytes.len();
    let response_payload_preview_hex = mapi::debug_payload_preview_hex(&response.bytes);
    if response.virtual_connection_cookie.is_some() {
        *virtual_connection_cookie = response.virtual_connection_cookie;
    }
    let target_virtual_connection_cookie = response
        .virtual_connection_cookie
        .or(*virtual_connection_cookie);
    let forwarded = send_rpc_proxy_out_channel(
        query,
        target_virtual_connection_cookie,
        response.bytes.clone(),
    );
    if !forwarded {
        if let Some(cookie) = target_virtual_connection_cookie {
            queue_pending_rpc_proxy_out_channel_response(query, cookie, response.bytes);
        }
    }
    info!(
        rca_debug = true,
        adapter = "rpcproxy",
        method = %method,
        path = %path,
        query = %query,
        response_kind = if forwarded {
            "out-channel-forwarded"
        } else {
            "out-channel-missing"
        },
        trace_id = %trace_id,
        client_request_id = %client_request_id,
        x_request_id = %x_request_id,
        http_status = 200u16,
        response_payload_bytes = response_payload_bytes,
        response_payload_preview_hex = %response_payload_preview_hex,
        duration_ms = started_at.elapsed().as_secs_f64() * 1000.0,
        user_agent = %user_agent,
        message = "rca debug rpc proxy forwarded response from in data stream"
    );
}

#[cfg(test)]
pub(crate) fn rpc_proxy_in_channel_response_for_buffer(buffer: &mut Vec<u8>) -> Option<Vec<u8>> {
    rpc_proxy_in_channel_response_for_endpoint_query("", buffer)
}

#[cfg(test)]
pub(crate) fn rpc_proxy_in_channel_response_for_endpoint_query(
    endpoint_query: &str,
    buffer: &mut Vec<u8>,
) -> Option<Vec<u8>> {
    let mut offset = 0usize;
    while offset + 16 <= buffer.len() {
        if buffer.get(offset..offset + 2) != Some(&[0x05, 0x00]) {
            offset += 1;
            continue;
        }

        let fragment_length = u16::from_le_bytes([buffer[offset + 8], buffer[offset + 9]]) as usize;
        if fragment_length < 16 {
            offset += 1;
            continue;
        }

        let fragment_end = offset + fragment_length;
        if fragment_end > buffer.len() {
            if offset > 0 {
                buffer.drain(..offset);
            }
            return None;
        }

        let fragment = &buffer[offset..fragment_end];
        if let Some(response) = rpc_proxy_conn_b1_response_body(fragment) {
            buffer.drain(..fragment_end);
            return Some(response.bytes);
        }
        let response = rpc_proxy_endpoint_response_for_fragment(endpoint_query, fragment);
        if let Some(response) = response {
            buffer.drain(..fragment_end);
            return Some(response);
        }

        offset = fragment_end;
    }
    if offset > 0 {
        buffer.drain(..offset);
    }
    None
}

async fn rpc_proxy_address_book_check_name_fallback<S>(
    store: &S,
    endpoint_query: &str,
    buffer: &[u8],
    principal: &AccountPrincipal,
) -> Option<RpcProxyInChannelResponse>
where
    S: ExchangeStore,
{
    if !endpoint_query.contains(":6004") || rpc_proxy_nspi_lookup_values(buffer).is_empty() {
        return None;
    }
    let call_id = rpc_proxy_last_dce_request_call_id(buffer)?;
    let bytes =
        rpc_proxy_nspi_resolve_names_response_for_principal(store, call_id, buffer, principal)
            .await;
    Some(RpcProxyInChannelResponse {
        bytes,
        virtual_connection_cookie: None,
    })
}

fn rpc_proxy_last_dce_request_call_id(buffer: &[u8]) -> Option<u32> {
    let mut offset = 0usize;
    let mut call_id = None;
    while offset + 16 <= buffer.len() {
        if buffer.get(offset..offset + 3) == Some(&[0x05, 0x00, 0x00]) {
            let fragment_length =
                u16::from_le_bytes([buffer[offset + 8], buffer[offset + 9]]) as usize;
            if fragment_length >= 24 && offset + fragment_length <= buffer.len() {
                call_id = read_le_u32(buffer, offset + 12);
                offset += fragment_length;
                continue;
            }
        }
        offset += 1;
    }
    call_id
}

#[cfg(test)]
pub(crate) async fn rpc_proxy_in_channel_response_for_endpoint_query_with_store<S, V>(
    store: &S,
    validator: &Validator<V>,
    principal: &AccountPrincipal,
    endpoint_query: &str,
    buffer: &mut Vec<u8>,
) -> Option<Vec<u8>>
where
    S: ExchangeStore,
    V: Detector,
{
    rpc_proxy_in_channel_response_for_endpoint_query_with_store_response(
        store,
        validator,
        principal,
        endpoint_query,
        buffer,
    )
    .await
    .map(|response| response.bytes)
}

async fn rpc_proxy_in_channel_response_for_endpoint_query_with_store_response<S, V>(
    store: &S,
    validator: &Validator<V>,
    principal: &AccountPrincipal,
    endpoint_query: &str,
    buffer: &mut Vec<u8>,
) -> Option<RpcProxyInChannelResponse>
where
    S: ExchangeStore,
    V: Detector,
{
    let mut offset = 0usize;
    while offset + 16 <= buffer.len() {
        if buffer.get(offset..offset + 2) != Some(&[0x05, 0x00]) {
            offset += 1;
            continue;
        }

        let fragment_length = u16::from_le_bytes([buffer[offset + 8], buffer[offset + 9]]) as usize;
        if fragment_length < 16 {
            offset += 1;
            continue;
        }

        let fragment_end = offset + fragment_length;
        if fragment_end > buffer.len() {
            if offset > 0 {
                buffer.drain(..offset);
            }
            return None;
        }

        let fragment = &buffer[offset..fragment_end];
        let response = if let Some(response) = rpc_proxy_conn_b1_response_body(fragment) {
            if consume_rpc_proxy_out_endpoint_rts_connect(endpoint_query) {
                None
            } else {
                Some(response)
            }
        } else {
            rpc_proxy_endpoint_response_for_fragment_with_store(
                store,
                validator,
                principal,
                endpoint_query,
                fragment,
            )
            .await
            .map(|bytes| RpcProxyInChannelResponse {
                bytes,
                virtual_connection_cookie: None,
            })
        };
        if let Some(response) = response {
            buffer.drain(..fragment_end);
            return Some(response);
        }

        offset = fragment_end;
    }
    if let Some(response) =
        rpc_proxy_address_book_check_name_fallback(store, endpoint_query, buffer, principal).await
    {
        buffer.clear();
        return Some(response);
    }
    if offset > 0 {
        buffer.drain(..offset);
    }
    None
}

#[cfg(test)]
fn rpc_proxy_endpoint_response_for_fragment(endpoint_query: &str, bytes: &[u8]) -> Option<Vec<u8>> {
    if bytes.get(0..2) != Some(&[0x05, 0x00]) {
        return None;
    }
    let fragment_length = u16::from_le_bytes([*bytes.get(8)?, *bytes.get(9)?]) as usize;
    if fragment_length > bytes.len() || fragment_length < 16 {
        return None;
    }
    let call_id = read_le_u32(bytes, 12)?;
    match bytes.get(2).copied()? {
        0x0b => {
            rpc_proxy_remember_dce_bind_contexts(endpoint_query, bytes);
            if consume_rpc_proxy_out_endpoint_bind_ack(endpoint_query) {
                return None;
            }
            return Some(rpc_proxy_dce_bind_ack_body(call_id, bytes));
        }
        0x0e => return Some(rpc_proxy_dce_alter_context_response_body(call_id, bytes)),
        0x00 => {}
        _ => return None,
    }
    if fragment_length < 24 {
        return None;
    }
    let alloc_hint = read_le_u32(bytes, 16)?;
    let context_id = u16::from_le_bytes([*bytes.get(20)?, *bytes.get(21)?]);
    let opnum = u16::from_le_bytes([*bytes.get(22)?, *bytes.get(23)?]);
    let bound_interface = rpc_proxy_bound_dce_context_interface(endpoint_query, context_id);
    if matches!(bound_interface, Some(RpcProxyDceBoundInterface::Management)) {
        match opnum {
            1 if alloc_hint == 4 => {
                let requested_stats = read_le_u32(bytes, 24)?;
                return Some(rpc_proxy_dce_response_with_request_auth(
                    rpc_proxy_mgmt_inq_stats_response(call_id, requested_stats),
                    bytes,
                ));
            }
            _ => {}
        }
    }
    if endpoint_query.contains(":6002") {
        match opnum {
            0 if alloc_hint >= 4 => {
                return Some(rpc_proxy_dce_response_with_request_auth(
                    rpc_proxy_rfri_get_new_dsa_response(call_id, endpoint_query),
                    bytes,
                ));
            }
            1 if alloc_hint >= 4 => {
                return Some(rpc_proxy_dce_response_with_request_auth(
                    rpc_proxy_rfri_get_fqdn_response(call_id, endpoint_query),
                    bytes,
                ));
            }
            _ => {}
        }
    }
    if endpoint_query.contains(":6001") {
        match opnum {
            1 if alloc_hint >= 20 => {
                return Some(rpc_proxy_dce_response_with_request_auth(
                    rpc_proxy_emsmdb_disconnect_response(call_id),
                    bytes,
                ));
            }
            10 if alloc_hint >= 20 => {
                return Some(rpc_proxy_dce_response_with_request_auth(
                    rpc_proxy_emsmdb_connect_ex_response(call_id),
                    bytes,
                ));
            }
            11 if alloc_hint >= 20 => {
                return Some(rpc_proxy_dce_response_with_request_auth(
                    rpc_proxy_emsmdb_rpc_ext2_response(call_id),
                    bytes,
                ));
            }
            _ => {}
        }
    }
    match (context_id, opnum) {
        (0, 1) if alloc_hint == 4 && !endpoint_query.contains(":6002") => {
            let requested_stats = read_le_u32(bytes, 24)?;
            return Some(rpc_proxy_dce_response_with_request_auth(
                rpc_proxy_mgmt_inq_stats_response(call_id, requested_stats),
                bytes,
            ));
        }
        _ => {}
    }
    if endpoint_query.contains(":6004") || context_id == 2 {
        return rpc_proxy_nspi_response_for_opnum(call_id, opnum, alloc_hint, bytes)
            .map(|response| rpc_proxy_dce_response_with_request_auth(response, bytes));
    }
    None
}

async fn rpc_proxy_endpoint_response_for_fragment_with_store<S, V>(
    store: &S,
    validator: &Validator<V>,
    principal: &AccountPrincipal,
    endpoint_query: &str,
    bytes: &[u8],
) -> Option<Vec<u8>>
where
    S: ExchangeStore,
    V: Detector,
{
    if bytes.get(0..2) != Some(&[0x05, 0x00]) {
        return None;
    }
    let fragment_length = u16::from_le_bytes([*bytes.get(8)?, *bytes.get(9)?]) as usize;
    if fragment_length > bytes.len() || fragment_length < 16 {
        return None;
    }
    let call_id = read_le_u32(bytes, 12)?;
    match bytes.get(2).copied()? {
        0x0b => {
            rpc_proxy_remember_dce_bind_contexts(endpoint_query, bytes);
            if consume_rpc_proxy_out_endpoint_bind_ack(endpoint_query) {
                return None;
            }
            return Some(rpc_proxy_dce_bind_ack_body(call_id, bytes));
        }
        0x0e => return Some(rpc_proxy_dce_alter_context_response_body(call_id, bytes)),
        0x00 => {}
        _ => return None,
    }
    if fragment_length < 24 {
        return None;
    }
    let alloc_hint = read_le_u32(bytes, 16)?;
    let context_id = u16::from_le_bytes([*bytes.get(20)?, *bytes.get(21)?]);
    let opnum = u16::from_le_bytes([*bytes.get(22)?, *bytes.get(23)?]);
    let bound_interface = rpc_proxy_bound_dce_context_interface(endpoint_query, context_id);
    if matches!(bound_interface, Some(RpcProxyDceBoundInterface::Management)) {
        match opnum {
            1 if alloc_hint == 4 => {
                let requested_stats = read_le_u32(bytes, 24)?;
                return Some(rpc_proxy_dce_response_with_request_auth(
                    rpc_proxy_mgmt_inq_stats_response(call_id, requested_stats),
                    bytes,
                ));
            }
            _ => {}
        }
    }
    if endpoint_query.contains(":6002") {
        match opnum {
            0 if alloc_hint >= 4 => {
                return Some(rpc_proxy_dce_response_with_request_auth(
                    rpc_proxy_rfri_get_new_dsa_response_for_principal(
                        call_id,
                        endpoint_query,
                        principal,
                    ),
                    bytes,
                ));
            }
            1 if alloc_hint >= 4 => {
                return Some(rpc_proxy_dce_response_with_request_auth(
                    rpc_proxy_rfri_get_fqdn_response_for_principal(
                        call_id,
                        endpoint_query,
                        principal,
                    ),
                    bytes,
                ));
            }
            _ => {}
        }
    }
    if endpoint_query.contains(":6001") {
        match opnum {
            1 if alloc_hint >= 20 => {
                return Some(rpc_proxy_dce_response_with_request_auth(
                    rpc_proxy_emsmdb_disconnect_response(call_id),
                    bytes,
                ));
            }
            10 if alloc_hint >= 20 => {
                return Some(rpc_proxy_dce_response_with_request_auth(
                    rpc_proxy_emsmdb_connect_ex_response_for_principal(call_id, principal),
                    bytes,
                ));
            }
            11 if alloc_hint >= 20 => {
                return Some(rpc_proxy_dce_response_with_request_auth(
                    rpc_proxy_emsmdb_rpc_ext2_response_for_principal(
                        store, validator, principal, call_id, bytes,
                    )
                    .await,
                    bytes,
                ));
            }
            _ => {}
        }
    }
    match (context_id, opnum) {
        (0, 1) if alloc_hint == 4 && !endpoint_query.contains(":6002") => {
            let requested_stats = read_le_u32(bytes, 24)?;
            return Some(rpc_proxy_dce_response_with_request_auth(
                rpc_proxy_mgmt_inq_stats_response(call_id, requested_stats),
                bytes,
            ));
        }
        _ => {}
    }
    if endpoint_query.contains(":6004") || context_id == 2 {
        return rpc_proxy_nspi_response_for_opnum_with_store(
            store, call_id, opnum, alloc_hint, bytes, principal,
        )
        .await
        .map(|response| rpc_proxy_dce_response_with_request_auth(response, bytes));
    }
    None
}
