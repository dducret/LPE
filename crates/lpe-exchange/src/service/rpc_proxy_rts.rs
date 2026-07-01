use axum::http::{HeaderMap, Method};

use super::rpc_proxy_codec::read_le_u32;
use super::rpc_proxy_requests::is_rpc_proxy_msrpc_request;
use super::{RPC_PROXY_CONNECTION_TIMEOUT_MS, RPC_PROXY_RECEIVE_WINDOW_SIZE};

#[derive(Debug)]
pub(super) struct RpcProxyInChannelResponse {
    pub(super) bytes: Vec<u8>,
    pub(super) virtual_connection_cookie: Option<[u8; 16]>,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct RpcProxyOutDataConnect {
    pub(super) receive_window_size: u32,
    pub(super) virtual_connection_cookie: [u8; 16],
}

pub(super) fn parse_rpc_proxy_out_data_connect_request(
    method: &Method,
    headers: &HeaderMap,
    request_body: &[u8],
) -> Option<RpcProxyOutDataConnect> {
    if method.as_str() != "RPC_OUT_DATA"
        || request_body.is_empty()
        || !is_rpc_proxy_msrpc_request(headers)
    {
        return None;
    }
    parse_rpc_proxy_conn_a1_rts_pdu(request_body)
}

pub(super) fn rpc_proxy_rts_connect_body(client_receive_window_size: u32) -> Vec<u8> {
    let receive_window_size = client_receive_window_size.clamp(1, RPC_PROXY_RECEIVE_WINDOW_SIZE);
    let mut body = rpc_proxy_connection_timeout_pdu();
    body.extend_from_slice(&rpc_proxy_connection_established_pdu(receive_window_size));
    body
}

pub(super) fn rpc_proxy_endpoint_connect_body() -> Vec<u8> {
    rpc_proxy_connection_timeout_pdu()
}

fn rpc_proxy_connection_timeout_pdu() -> Vec<u8> {
    let mut body = rpc_proxy_rts_header(0, 1, 28);
    body.extend_from_slice(&2u32.to_le_bytes());
    body.extend_from_slice(&RPC_PROXY_CONNECTION_TIMEOUT_MS.to_le_bytes());
    body
}

pub(super) fn rpc_proxy_connection_established_pdu(receive_window_size: u32) -> Vec<u8> {
    let mut body = rpc_proxy_rts_header(0, 3, 44);
    body.extend_from_slice(&6u32.to_le_bytes());
    body.extend_from_slice(&1u32.to_le_bytes());
    body.extend_from_slice(&0u32.to_le_bytes());
    body.extend_from_slice(&receive_window_size.to_le_bytes());
    body.extend_from_slice(&2u32.to_le_bytes());
    body.extend_from_slice(&RPC_PROXY_CONNECTION_TIMEOUT_MS.to_le_bytes());
    body
}

pub(super) fn rpc_proxy_conn_b1_response_body(request: &[u8]) -> Option<RpcProxyInChannelResponse> {
    let virtual_connection_cookie = rpc_proxy_conn_b1_virtual_connection_cookie(request)?;
    Some(RpcProxyInChannelResponse {
        bytes: rpc_proxy_connection_established_pdu(RPC_PROXY_RECEIVE_WINDOW_SIZE),
        virtual_connection_cookie: Some(virtual_connection_cookie),
    })
}

fn parse_rpc_proxy_conn_a1_rts_pdu(body: &[u8]) -> Option<RpcProxyOutDataConnect> {
    if body.len() < 20 || body.get(0..4) != Some(&[0x05, 0x00, 0x14, 0x03]) {
        return None;
    }
    let fragment_length = u16::from_le_bytes([body[8], body[9]]) as usize;
    let flags = u16::from_le_bytes([body[16], body[17]]);
    let command_count = u16::from_le_bytes([body[18], body[19]]);
    if fragment_length != body.len() || flags != 0 || command_count != 4 {
        return None;
    }

    let mut offset = 20;
    let version = parse_rpc_rts_u32_command(body, &mut offset, 6)?;
    if version == 0 {
        return None;
    }
    let virtual_connection_cookie = parse_rpc_rts_cookie_command(body, &mut offset, 3)?;
    parse_rpc_rts_cookie_command(body, &mut offset, 3)?;
    let receive_window_size = parse_rpc_rts_u32_command(body, &mut offset, 0)?;
    (offset == body.len()).then_some(RpcProxyOutDataConnect {
        receive_window_size,
        virtual_connection_cookie,
    })
}

fn rpc_proxy_conn_b1_virtual_connection_cookie(body: &[u8]) -> Option<[u8; 16]> {
    if body.len() < 104 || body.get(0..4) != Some(&[0x05, 0x00, 0x14, 0x03]) {
        return None;
    }
    let fragment_length = u16::from_le_bytes([body[8], body[9]]) as usize;
    let flags = u16::from_le_bytes([body[16], body[17]]);
    let command_count = u16::from_le_bytes([body[18], body[19]]);
    if fragment_length != body.len() || flags != 0 || command_count != 6 {
        return None;
    }

    let mut offset = 20;
    if parse_rpc_rts_u32_command(body, &mut offset, 6) != Some(1) {
        return None;
    }
    let virtual_connection_cookie = parse_rpc_rts_cookie_command(body, &mut offset, 3)?;
    if parse_rpc_rts_cookie_command(body, &mut offset, 3).is_none() {
        return None;
    }
    if parse_rpc_rts_u32_command(body, &mut offset, 4).is_none() {
        return None;
    }
    if parse_rpc_rts_u32_command(body, &mut offset, 5).is_none() {
        return None;
    }
    if parse_rpc_rts_cookie_command(body, &mut offset, 12).is_none() {
        return None;
    }
    (offset == body.len()).then_some(virtual_connection_cookie)
}

fn rpc_proxy_rts_header(flags: u16, command_count: u16, fragment_length: u16) -> Vec<u8> {
    let mut body = Vec::with_capacity(fragment_length as usize);
    body.extend_from_slice(&[0x05, 0x00, 0x14, 0x03, 0x10, 0x00, 0x00, 0x00]);
    body.extend_from_slice(&fragment_length.to_le_bytes());
    body.extend_from_slice(&0u16.to_le_bytes());
    body.extend_from_slice(&0u32.to_le_bytes());
    body.extend_from_slice(&flags.to_le_bytes());
    body.extend_from_slice(&command_count.to_le_bytes());
    body
}

pub(super) fn parse_rpc_rts_u32_command(
    body: &[u8],
    offset: &mut usize,
    expected_command: u32,
) -> Option<u32> {
    let command = read_le_u32(body, *offset)?;
    let value = read_le_u32(body, *offset + 4)?;
    if command != expected_command {
        return None;
    }
    *offset += 8;
    Some(value)
}

pub(super) fn parse_rpc_rts_cookie_command(
    body: &[u8],
    offset: &mut usize,
    expected_command: u32,
) -> Option<[u8; 16]> {
    let command = read_le_u32(body, *offset)?;
    let cookie = body.get(*offset + 4..*offset + 20)?;
    if command != expected_command {
        return None;
    }
    let mut result = [0u8; 16];
    result.copy_from_slice(cookie);
    *offset += 20;
    Some(result)
}
