use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

use super::rpc_proxy_codec::read_le_u32;
use crate::ntlm;

pub(super) const RPC_PROXY_DCE_FAULT_PROTOCOL_ERROR: u32 = 0x0000_0005;

const RPC_PROXY_DCE_NDR_TRANSFER_SYNTAX: [u8; 20] = [
    0x04, 0x5d, 0x88, 0x8a, 0xeb, 0x1c, 0xc9, 0x11, 0x9f, 0xe8, 0x08, 0x00, 0x2b, 0x10, 0x48, 0x60,
    0x02, 0x00, 0x00, 0x00,
];
const RPC_PROXY_DCE_MGMT_INTERFACE_SYNTAX: [u8; 20] = [
    0x80, 0xbd, 0xa8, 0xaf, 0x8a, 0x7d, 0xc9, 0x11, 0xbe, 0xf4, 0x08, 0x00, 0x2b, 0x10, 0x29, 0x89,
    0x01, 0x00, 0x00, 0x00,
];
const RPC_PROXY_RFRI_INTERFACE_SYNTAX: [u8; 20] = [
    0xe0, 0xf5, 0x44, 0x15, 0x3c, 0x61, 0xd1, 0x11, 0x93, 0xdf, 0x00, 0xc0, 0x4f, 0xd7, 0xbd, 0x09,
    0x01, 0x00, 0x00, 0x00,
];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum RpcProxyDceBoundInterface {
    Management,
    Rfri,
}

#[derive(Clone, Copy)]
struct RpcProxyDceContextResult {
    result: u16,
    reason: u16,
    transfer_syntax: [u8; 20],
}

#[derive(Clone, Copy)]
struct RpcProxyDceRequestAuth {
    auth_type: u8,
    auth_level: u8,
    context_id: u32,
}

pub(super) fn rpc_proxy_dce_bind_ack_body(call_id: u32, request: &[u8]) -> Vec<u8> {
    let results = rpc_proxy_dce_bind_context_results(request).unwrap_or_else(|| {
        rpc_proxy_dce_default_context_results(
            rpc_proxy_dce_bind_context_count(request).unwrap_or(1),
        )
    });
    rpc_proxy_dce_bind_ack_body_with_results(call_id, &results)
}

pub(super) fn rpc_proxy_dce_bind_ack_body_with_result_count(
    call_id: u32,
    result_count: u8,
) -> Vec<u8> {
    let results = rpc_proxy_dce_default_context_results(result_count);
    rpc_proxy_dce_bind_ack_body_with_results(call_id, &results)
}

fn rpc_proxy_dce_bind_ack_body_with_results(
    call_id: u32,
    results: &[RpcProxyDceContextResult],
) -> Vec<u8> {
    const DCE_RPC_BIND_ACK: u8 = 0x0c;
    rpc_proxy_dce_context_ack_body(call_id, DCE_RPC_BIND_ACK, results)
}

pub(super) fn rpc_proxy_dce_alter_context_response_body(call_id: u32, request: &[u8]) -> Vec<u8> {
    const DCE_RPC_ALTER_CONTEXT_RESPONSE: u8 = 0x0f;
    let results = rpc_proxy_dce_bind_context_results(request).unwrap_or_else(|| {
        rpc_proxy_dce_default_context_results(
            rpc_proxy_dce_bind_context_count(request).unwrap_or(1),
        )
    });
    rpc_proxy_dce_context_ack_body(call_id, DCE_RPC_ALTER_CONTEXT_RESPONSE, &results)
}

pub(super) fn rpc_proxy_bound_dce_context_interface(
    endpoint_query: &str,
    context_id: u16,
) -> Option<RpcProxyDceBoundInterface> {
    let contexts = rpc_proxy_bound_dce_contexts()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    contexts
        .get(endpoint_query)
        .and_then(|endpoint_contexts| endpoint_contexts.get(&context_id).copied())
}

pub(super) fn rpc_proxy_remember_dce_bind_contexts(endpoint_query: &str, request: &[u8]) {
    let Some(count) = rpc_proxy_dce_bind_context_count(request) else {
        return;
    };
    let mut offset = 28usize;
    let mut endpoint_contexts = HashMap::new();
    for _ in 0..count {
        let Some(context_id) = request
            .get(offset..offset + 2)
            .map(|bytes| u16::from_le_bytes([bytes[0], bytes[1]]))
        else {
            return;
        };
        let Some(transfer_count) = request.get(offset + 2).copied().map(usize::from) else {
            return;
        };
        let Some(abstract_syntax) = request.get(offset + 4..offset + 24) else {
            return;
        };
        offset += 24;
        let mut has_ndr_transfer_syntax = false;
        for _ in 0..transfer_count {
            let Some(transfer_syntax) = request.get(offset..offset + 20) else {
                return;
            };
            has_ndr_transfer_syntax |= transfer_syntax == RPC_PROXY_DCE_NDR_TRANSFER_SYNTAX;
            offset += 20;
        }
        if !has_ndr_transfer_syntax {
            continue;
        }
        if let Some(interface) = rpc_proxy_dce_interface_for_abstract_syntax(abstract_syntax) {
            endpoint_contexts.insert(context_id, interface);
        }
    }
    if endpoint_contexts.is_empty() {
        return;
    }
    let mut contexts = rpc_proxy_bound_dce_contexts()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    contexts.insert(endpoint_query.to_string(), endpoint_contexts);
}

pub(super) fn rpc_proxy_dce_fault_response(call_id: u32, status: u32) -> Vec<u8> {
    const DCE_RPC_FAULT: u8 = 0x03;
    const DCE_RPC_FIRST_FRAG: u8 = 0x01;
    const DCE_RPC_LAST_FRAG: u8 = 0x02;
    const FRAGMENT_LENGTH: u16 = 32;

    let mut packet = Vec::with_capacity(FRAGMENT_LENGTH as usize);
    packet.extend_from_slice(&[
        0x05,
        0x00,
        DCE_RPC_FAULT,
        DCE_RPC_FIRST_FRAG | DCE_RPC_LAST_FRAG,
        0x10,
        0x00,
        0x00,
        0x00,
    ]);
    packet.extend_from_slice(&FRAGMENT_LENGTH.to_le_bytes());
    packet.extend_from_slice(&0u16.to_le_bytes());
    packet.extend_from_slice(&call_id.to_le_bytes());
    packet.extend_from_slice(&0u32.to_le_bytes());
    packet.extend_from_slice(&0u16.to_le_bytes());
    packet.push(0);
    packet.push(0);
    packet.extend_from_slice(&status.to_le_bytes());
    packet.extend_from_slice(&0u32.to_le_bytes());
    packet
}

pub(super) fn rpc_proxy_dce_response_with_request_auth(
    mut response: Vec<u8>,
    request: &[u8],
) -> Vec<u8> {
    let Some(auth) = rpc_proxy_dce_request_auth(request) else {
        return response;
    };
    let auth_pad_length = (4 - (response.len() % 4)) % 4;
    response.extend(std::iter::repeat_n(0, auth_pad_length));
    response.push(auth.auth_type);
    response.push(auth.auth_level);
    response.push(auth_pad_length as u8);
    response.push(0);
    response.extend_from_slice(&auth.context_id.to_le_bytes());
    response.extend_from_slice(&[0u8; 16]);
    let fragment_length = response.len() as u16;
    response[8..10].copy_from_slice(&fragment_length.to_le_bytes());
    response[10..12].copy_from_slice(&16u16.to_le_bytes());
    response
}

pub(super) fn rpc_proxy_dce_response(call_id: u32, stub: &[u8]) -> Vec<u8> {
    const RESPONSE_BODY_HEADER_LENGTH: usize = 8;
    let fragment_length = 16 + RESPONSE_BODY_HEADER_LENGTH + stub.len();
    let mut packet = Vec::with_capacity(fragment_length);
    packet.extend_from_slice(&[0x05, 0x00, 0x02, 0x03, 0x10, 0x00, 0x00, 0x00]);
    packet.extend_from_slice(&(fragment_length as u16).to_le_bytes());
    packet.extend_from_slice(&0u16.to_le_bytes());
    packet.extend_from_slice(&call_id.to_le_bytes());
    packet.extend_from_slice(&(stub.len() as u32).to_le_bytes());
    packet.extend_from_slice(&0u16.to_le_bytes());
    packet.push(0);
    packet.push(0);
    packet.extend_from_slice(stub);
    packet
}

fn rpc_proxy_dce_bind_context_count(request: &[u8]) -> Option<u8> {
    let count = *request.get(24)?;
    (count > 0).then_some(count)
}

fn rpc_proxy_bound_dce_contexts(
) -> &'static Mutex<HashMap<String, HashMap<u16, RpcProxyDceBoundInterface>>> {
    static CONTEXTS: OnceLock<Mutex<HashMap<String, HashMap<u16, RpcProxyDceBoundInterface>>>> =
        OnceLock::new();
    CONTEXTS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn rpc_proxy_dce_interface_for_abstract_syntax(
    abstract_syntax: &[u8],
) -> Option<RpcProxyDceBoundInterface> {
    if abstract_syntax == RPC_PROXY_DCE_MGMT_INTERFACE_SYNTAX {
        return Some(RpcProxyDceBoundInterface::Management);
    }
    if abstract_syntax == RPC_PROXY_RFRI_INTERFACE_SYNTAX {
        return Some(RpcProxyDceBoundInterface::Rfri);
    }
    None
}

fn rpc_proxy_dce_default_context_results(result_count: u8) -> Vec<RpcProxyDceContextResult> {
    (0..result_count)
        .map(|result_index| {
            if result_index == 0 {
                rpc_proxy_dce_context_accept_result(RPC_PROXY_DCE_NDR_TRANSFER_SYNTAX)
            } else {
                rpc_proxy_dce_context_provider_rejection_result()
            }
        })
        .collect()
}

fn rpc_proxy_dce_bind_context_results(request: &[u8]) -> Option<Vec<RpcProxyDceContextResult>> {
    let count = rpc_proxy_dce_bind_context_count(request)? as usize;
    let mut offset = 28usize;
    let mut results = Vec::with_capacity(count);
    for _ in 0..count {
        let transfer_count = *request.get(offset + 2)? as usize;
        offset += 24;
        let mut result = rpc_proxy_dce_context_provider_rejection_result();
        for _ in 0..transfer_count {
            let transfer_syntax = request.get(offset..offset + 20)?;
            if transfer_syntax == RPC_PROXY_DCE_NDR_TRANSFER_SYNTAX {
                result = rpc_proxy_dce_context_accept_result(RPC_PROXY_DCE_NDR_TRANSFER_SYNTAX);
            } else if rpc_proxy_is_bind_time_feature_negotiation_syntax(transfer_syntax) {
                result = rpc_proxy_dce_bind_time_feature_negotiation_result();
            }
            offset += 20;
        }
        results.push(result);
    }
    Some(results)
}

fn rpc_proxy_dce_context_accept_result(transfer_syntax: [u8; 20]) -> RpcProxyDceContextResult {
    RpcProxyDceContextResult {
        result: 0,
        reason: 0,
        transfer_syntax,
    }
}

fn rpc_proxy_dce_context_provider_rejection_result() -> RpcProxyDceContextResult {
    RpcProxyDceContextResult {
        result: 2,
        reason: 2,
        transfer_syntax: [0u8; 20],
    }
}

fn rpc_proxy_dce_bind_time_feature_negotiation_result() -> RpcProxyDceContextResult {
    RpcProxyDceContextResult {
        result: 3,
        reason: 0,
        transfer_syntax: [0u8; 20],
    }
}

fn rpc_proxy_is_bind_time_feature_negotiation_syntax(transfer_syntax: &[u8]) -> bool {
    transfer_syntax.len() == 20
        && transfer_syntax[0..8] == [0x2c, 0x1c, 0xb7, 0x6c, 0x12, 0x98, 0x40, 0x45]
        && transfer_syntax[16..20] == [0x01, 0x00, 0x00, 0x00]
}

fn rpc_proxy_dce_request_auth_trailer_offset(
    request: &[u8],
    fragment_length: usize,
    auth_length: usize,
) -> Option<usize> {
    let token_base = fragment_length.checked_sub(auth_length + 8)?;
    if rpc_proxy_dce_auth_trailer_candidate(request, token_base, 0) {
        return Some(token_base);
    }
    for auth_pad_length in 1..=15usize {
        let Some(offset) = token_base.checked_sub(auth_pad_length) else {
            break;
        };
        if rpc_proxy_dce_auth_trailer_candidate(request, offset, auth_pad_length) {
            return Some(offset);
        }
    }
    None
}

fn rpc_proxy_dce_auth_trailer_candidate(
    request: &[u8],
    offset: usize,
    auth_pad_length: usize,
) -> bool {
    let Some(auth_type) = request.get(offset) else {
        return false;
    };
    let Some(auth_level) = request.get(offset + 1) else {
        return false;
    };
    let Some(candidate_pad_length) = request.get(offset + 2) else {
        return false;
    };
    if usize::from(*candidate_pad_length) != auth_pad_length {
        return false;
    }
    // NTLM over RPC/HTTP is the Outlook Anywhere path RCA uses for these probes.
    if *auth_type != 0x0a {
        return false;
    }
    matches!(*auth_level, 1..=6)
}

fn rpc_proxy_dce_request_auth(request: &[u8]) -> Option<RpcProxyDceRequestAuth> {
    let fragment_length = u16::from_le_bytes([*request.get(8)?, *request.get(9)?]) as usize;
    let auth_length = u16::from_le_bytes([*request.get(10)?, *request.get(11)?]) as usize;
    if auth_length == 0 || fragment_length > request.len() || fragment_length < auth_length + 8 {
        return None;
    }
    let trailer_offset =
        rpc_proxy_dce_request_auth_trailer_offset(request, fragment_length, auth_length)?;
    Some(RpcProxyDceRequestAuth {
        auth_type: *request.get(trailer_offset)?,
        auth_level: *request.get(trailer_offset + 1)?,
        context_id: read_le_u32(request, trailer_offset + 4)?,
    })
}

fn rpc_proxy_dce_context_ack_body(
    call_id: u32,
    packet_type: u8,
    results: &[RpcProxyDceContextResult],
) -> Vec<u8> {
    const DCE_RPC_FIRST_FRAG: u8 = 0x01;
    const DCE_RPC_LAST_FRAG: u8 = 0x02;
    const DCE_RPC_MAX_FRAG: u16 = 5840;
    let mut body = Vec::new();
    body.extend_from_slice(&DCE_RPC_MAX_FRAG.to_le_bytes());
    body.extend_from_slice(&DCE_RPC_MAX_FRAG.to_le_bytes());
    body.extend_from_slice(&1u32.to_le_bytes());
    body.extend_from_slice(&1u16.to_le_bytes());
    body.push(0);
    body.push(0);
    body.push(results.len() as u8);
    body.push(0);
    body.extend_from_slice(&0u16.to_le_bytes());
    for result in results {
        body.extend_from_slice(&result.result.to_le_bytes());
        body.extend_from_slice(&result.reason.to_le_bytes());
        body.extend_from_slice(&result.transfer_syntax);
    }

    let verifier = ntlm::connect_level_challenge_verifier();
    body.push(verifier.auth_type);
    body.push(verifier.auth_level);
    body.push(0);
    body.push(0);
    body.extend_from_slice(&verifier.context_id.to_le_bytes());
    body.extend_from_slice(&verifier.value);

    let fragment_length = (16 + body.len()) as u16;
    let mut packet = Vec::with_capacity(fragment_length as usize);
    packet.extend_from_slice(&[
        0x05,
        0x00,
        packet_type,
        DCE_RPC_FIRST_FRAG | DCE_RPC_LAST_FRAG,
        0x10,
        0x00,
        0x00,
        0x00,
    ]);
    packet.extend_from_slice(&fragment_length.to_le_bytes());
    packet.extend_from_slice(&(verifier.value.len() as u16).to_le_bytes());
    packet.extend_from_slice(&call_id.to_le_bytes());
    packet.extend_from_slice(&body);
    packet
}
