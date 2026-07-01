use anyhow::{anyhow, Result};
use lpe_domain::normalization;
use lpe_magika::{Detector, Validator};
use lpe_mail_auth::AccountPrincipal;
use tracing::warn;
#[cfg(test)]
use uuid::Uuid;

use crate::{
    mapi,
    store::{
        ExchangeAddressBookDirectoryKind, ExchangeAddressBookEntry,
        ExchangeAddressBookEntryDetails, ExchangeAddressBookEntryKind, ExchangeStore,
    },
};

use super::rpc_proxy_codec::{
    push_le_u32, read_le_u32, rpc_proxy_push_ndr_ascii_string, rpc_proxy_push_ndr_byte_array,
    rpc_proxy_push_ndr_utf16_string,
};
use super::rpc_proxy_dce::{
    rpc_proxy_dce_fault_response, rpc_proxy_dce_response, RPC_PROXY_DCE_FAULT_PROTOCOL_ERROR,
};
#[cfg(test)]
pub(super) fn rpc_proxy_nspi_response_for_opnum(
    call_id: u32,
    opnum: u16,
    alloc_hint: u32,
    bytes: &[u8],
) -> Option<Vec<u8>> {
    match opnum {
        0 if alloc_hint >= 44 => Some(rpc_proxy_nspi_bind_response(call_id)),
        1 if alloc_hint >= 4 => Some(rpc_proxy_nspi_unbind_response(call_id)),
        2 if alloc_hint >= 20 => Some(rpc_proxy_nspi_update_stat_response(call_id)),
        3 if alloc_hint >= 20 => Some(rpc_proxy_nspi_query_rows_response(call_id, bytes)),
        4 if alloc_hint >= 20 => Some(rpc_proxy_nspi_query_rows_response(call_id, bytes)),
        5 if alloc_hint >= 20 => Some(rpc_proxy_nspi_get_matches_response(call_id, bytes)),
        6 if alloc_hint >= 20 => Some(rpc_proxy_nspi_resort_restriction_response(call_id)),
        7 if alloc_hint >= 20 => Some(rpc_proxy_nspi_minimal_ids_response(call_id)),
        8 if alloc_hint >= 16 => Some(rpc_proxy_nspi_property_tags_response(call_id)),
        9 if alloc_hint >= 20 => Some(rpc_proxy_nspi_get_props_response(call_id, bytes)),
        10 if alloc_hint >= 20 => Some(rpc_proxy_nspi_compare_mids_response(call_id)),
        12 if alloc_hint >= 20 => Some(rpc_proxy_nspi_get_special_table_response(call_id)),
        13 if alloc_hint >= 20 => Some(rpc_proxy_nspi_get_props_response(call_id, bytes)),
        16 if alloc_hint >= 12 => Some(rpc_proxy_nspi_property_tags_response(call_id)),
        17 if alloc_hint >= 12 => Some(rpc_proxy_nspi_get_names_from_ids_response(call_id, bytes)),
        18 if alloc_hint >= 20 => Some(rpc_proxy_nspi_property_tags_response(call_id)),
        19 if alloc_hint >= 24 => Some(rpc_proxy_nspi_resolve_names_response(call_id, bytes)),
        20 if alloc_hint >= 24 => Some(rpc_proxy_nspi_resolve_names_response(call_id, bytes)),
        _ => None,
    }
}

pub(super) async fn rpc_proxy_nspi_response_for_opnum_with_store<S>(
    store: &S,
    call_id: u32,
    opnum: u16,
    alloc_hint: u32,
    bytes: &[u8],
    principal: &AccountPrincipal,
) -> Option<Vec<u8>>
where
    S: ExchangeStore,
{
    match opnum {
        0 if alloc_hint >= 44 => Some(rpc_proxy_nspi_bind_response(call_id)),
        1 if alloc_hint >= 4 => Some(rpc_proxy_nspi_unbind_response(call_id)),
        2 if alloc_hint >= 20 => Some(rpc_proxy_nspi_update_stat_response(call_id)),
        3 if alloc_hint >= 20 => Some(
            rpc_proxy_nspi_query_rows_response_for_principal(store, call_id, bytes, principal)
                .await,
        ),
        4 if alloc_hint >= 20 => Some(
            rpc_proxy_nspi_query_rows_response_for_principal(store, call_id, bytes, principal)
                .await,
        ),
        5 if alloc_hint >= 20 => Some(
            rpc_proxy_nspi_get_matches_response_for_principal(store, call_id, bytes, principal)
                .await,
        ),
        6 if alloc_hint >= 20 => Some(rpc_proxy_nspi_resort_restriction_response(call_id)),
        7 if alloc_hint >= 20 => Some(rpc_proxy_nspi_minimal_ids_response(call_id)),
        8 if alloc_hint >= 16 => Some(rpc_proxy_nspi_property_tags_response(call_id)),
        9 if alloc_hint >= 20 => Some(
            rpc_proxy_nspi_get_props_response_for_principal(store, call_id, bytes, principal).await,
        ),
        10 if alloc_hint >= 20 => Some(rpc_proxy_nspi_compare_mids_response(call_id)),
        12 if alloc_hint >= 20 => Some(rpc_proxy_nspi_get_special_table_response(call_id)),
        13 if alloc_hint >= 20 => Some(
            rpc_proxy_nspi_get_props_response_for_principal(store, call_id, bytes, principal).await,
        ),
        16 if alloc_hint >= 12 => Some(rpc_proxy_nspi_property_tags_response(call_id)),
        17 if alloc_hint >= 12 => Some(rpc_proxy_nspi_get_names_from_ids_response(call_id, bytes)),
        18 if alloc_hint >= 20 => Some(rpc_proxy_nspi_property_tags_response(call_id)),
        19 if alloc_hint >= 24 => Some(
            rpc_proxy_nspi_resolve_names_response_for_principal(store, call_id, bytes, principal)
                .await,
        ),
        20 if alloc_hint >= 24 => Some(
            rpc_proxy_nspi_resolve_names_response_for_principal(store, call_id, bytes, principal)
                .await,
        ),
        _ => None,
    }
}

pub(super) fn rpc_proxy_mgmt_inq_stats_response(call_id: u32, requested_stats: u32) -> Vec<u8> {
    let stat_count = requested_stats.min(4);
    let stats = [1u32, 0u32, 1u32, 1u32];
    let mut stub = Vec::with_capacity(8 + (stat_count as usize * 4) + 4);
    stub.extend_from_slice(&stat_count.to_le_bytes());
    stub.extend_from_slice(&stat_count.to_le_bytes());
    for value in stats.iter().take(stat_count as usize) {
        stub.extend_from_slice(&value.to_le_bytes());
    }
    stub.extend_from_slice(&0u32.to_le_bytes());

    rpc_proxy_dce_response(call_id, &stub)
}

#[cfg(test)]
pub(super) fn rpc_proxy_emsmdb_connect_ex_response(call_id: u32) -> Vec<u8> {
    let mut context = [0u8; 20];
    context[4..20].copy_from_slice(Uuid::nil().as_bytes());
    rpc_proxy_emsmdb_connect_ex_response_with_context(call_id, &context)
}

pub(super) fn rpc_proxy_emsmdb_connect_ex_response_for_principal(
    call_id: u32,
    principal: &AccountPrincipal,
) -> Vec<u8> {
    let context = mapi::create_rpc_emsmdb_context(principal);
    rpc_proxy_emsmdb_connect_ex_response_with_context(call_id, &context)
}

fn rpc_proxy_emsmdb_connect_ex_response_with_context(call_id: u32, context: &[u8; 20]) -> Vec<u8> {
    let mut stub = Vec::new();
    rpc_proxy_push_emsmdb_context_handle(&mut stub, context);
    push_le_u32(&mut stub, 60_000);
    push_le_u32(&mut stub, 6);
    push_le_u32(&mut stub, 10_000);
    stub.extend_from_slice(&0x0304u16.to_le_bytes());
    while stub.len() % 4 != 0 {
        stub.push(0);
    }
    push_le_u32(&mut stub, 0);
    push_le_u32(&mut stub, 0);
    for value in [15u16, 0x263c, 0] {
        stub.extend_from_slice(&value.to_le_bytes());
    }
    for value in [12u16, 0x183e, 1000] {
        stub.extend_from_slice(&value.to_le_bytes());
    }
    while stub.len() % 4 != 0 {
        stub.push(0);
    }
    push_le_u32(&mut stub, 1);
    rpc_proxy_push_ndr_byte_array(&mut stub, &[]);
    push_le_u32(&mut stub, 0);
    push_le_u32(&mut stub, 0);

    rpc_proxy_dce_response(call_id, &stub)
}

#[cfg(test)]
pub(super) fn rpc_proxy_emsmdb_rpc_ext2_response(call_id: u32) -> Vec<u8> {
    let mut context = [0u8; 20];
    context[4..20].copy_from_slice(Uuid::nil().as_bytes());
    rpc_proxy_emsmdb_rpc_ext2_response_with_rop_buffer(call_id, &context, Vec::new())
}

pub(super) async fn rpc_proxy_emsmdb_rpc_ext2_response_for_principal<S, V>(
    store: &S,
    validator: &Validator<V>,
    principal: &AccountPrincipal,
    call_id: u32,
    request: &[u8],
) -> Vec<u8>
where
    S: ExchangeStore,
    V: Detector,
{
    let (context, rop_buffer) = match rpc_proxy_emsmdb_rpc_ext2_request(request) {
        Ok(request) => request,
        Err(error) => {
            warn!(
                rca_debug = true,
                adapter = "rpcproxy",
                mailbox = %principal.email,
                error = %error,
                message = "rpc proxy emsmdb request parsing failed"
            );
            return rpc_proxy_dce_fault_response(call_id, RPC_PROXY_DCE_FAULT_PROTOCOL_ERROR);
        }
    };
    let rop_buffer =
        match mapi::execute_rpc_emsmdb_rops(store, validator, principal, &context, &rop_buffer)
            .await
        {
            Ok(rop_buffer) => rop_buffer,
            Err(error) => {
                warn!(
                    rca_debug = true,
                    adapter = "rpcproxy",
                    mailbox = %principal.email,
                    error = %error,
                    message = "rpc proxy emsmdb execution failed"
                );
                return rpc_proxy_dce_fault_response(call_id, RPC_PROXY_DCE_FAULT_PROTOCOL_ERROR);
            }
        };
    rpc_proxy_emsmdb_rpc_ext2_response_with_rop_buffer(call_id, &context, rop_buffer)
}

fn rpc_proxy_emsmdb_rpc_ext2_response_with_rop_buffer(
    call_id: u32,
    context: &[u8; 20],
    rop_buffer: Vec<u8>,
) -> Vec<u8> {
    let rgb_out = if rop_buffer.is_empty() {
        rpc_proxy_rpc_header_ext_payload(&[])
    } else {
        rop_buffer
    };
    let mut stub = Vec::new();
    rpc_proxy_push_emsmdb_context_handle(&mut stub, context);
    push_le_u32(&mut stub, 0);
    rpc_proxy_push_ndr_byte_array(&mut stub, &rgb_out);
    push_le_u32(&mut stub, rgb_out.len() as u32);
    rpc_proxy_push_ndr_byte_array(&mut stub, &[]);
    push_le_u32(&mut stub, 0);
    push_le_u32(&mut stub, 1);
    push_le_u32(&mut stub, 0);

    rpc_proxy_dce_response(call_id, &stub)
}

pub(super) fn rpc_proxy_emsmdb_disconnect_response(call_id: u32) -> Vec<u8> {
    let mut stub = Vec::new();
    stub.extend_from_slice(&[0; 20]);
    push_le_u32(&mut stub, 0);

    rpc_proxy_dce_response(call_id, &stub)
}

fn rpc_proxy_push_emsmdb_context_handle(stub: &mut Vec<u8>, context: &[u8; 20]) {
    stub.extend_from_slice(context);
}

fn rpc_proxy_rpc_header_ext_payload(payload: &[u8]) -> Vec<u8> {
    let size = payload.len().min(u16::MAX as usize) as u16;
    let mut buffer = Vec::with_capacity(8 + payload.len());
    buffer.extend_from_slice(&0u16.to_le_bytes());
    buffer.extend_from_slice(&0x0004u16.to_le_bytes());
    buffer.extend_from_slice(&size.to_le_bytes());
    buffer.extend_from_slice(&size.to_le_bytes());
    buffer.extend_from_slice(payload);
    buffer
}

fn rpc_proxy_emsmdb_rpc_ext2_request(request: &[u8]) -> Result<([u8; 20], Vec<u8>)> {
    let fragment_length = request
        .get(8..10)
        .map(|bytes| u16::from_le_bytes([bytes[0], bytes[1]]) as usize)
        .ok_or_else(|| anyhow!("truncated DCE/RPC request header"))?;
    let stub = request
        .get(24..fragment_length)
        .ok_or_else(|| anyhow!("truncated EcDoRpcExt2 request stub"))?;
    let context: [u8; 20] = stub
        .get(0..20)
        .ok_or_else(|| anyhow!("missing EcDoRpcExt2 context handle"))?
        .try_into()
        .map_err(|_| anyhow!("invalid EcDoRpcExt2 context handle"))?;
    for offset in 20..stub.len().saturating_sub(8) {
        let candidate = &stub[offset..];
        if candidate.get(0..2) != Some(&[0, 0]) {
            continue;
        }
        let flags = u16::from_le_bytes([candidate[2], candidate[3]]);
        let size = u16::from_le_bytes([candidate[4], candidate[5]]) as usize;
        let size_actual = u16::from_le_bytes([candidate[6], candidate[7]]) as usize;
        if flags & !0x0004 != 0 || size == 0 || size > size_actual {
            continue;
        }
        let end = 8 + size;
        let Some(payload) = candidate.get(8..end) else {
            continue;
        };
        let Some(rop_size_bytes) = payload.get(0..2) else {
            continue;
        };
        let rop_size = u16::from_le_bytes(
            rop_size_bytes
                .try_into()
                .map_err(|_| anyhow!("invalid ROP buffer size"))?,
        ) as usize;
        if rop_size >= 2 && payload.len() >= rop_size {
            return Ok((context, candidate[..end].to_vec()));
        }
    }
    Err(anyhow!(
        "missing valid EcDoRpcExt2 RPC_HEADER_EXT ROP payload"
    ))
}

#[cfg(test)]
pub(super) fn rpc_proxy_rfri_get_new_dsa_response(call_id: u32, endpoint_query: &str) -> Vec<u8> {
    let server = rpc_proxy_referral_server_name(endpoint_query);
    let mut stub = Vec::with_capacity(40 + server.len());
    push_le_u32(&mut stub, 0);
    push_le_u32(&mut stub, 0x0002_0000);
    push_le_u32(&mut stub, 0x0002_0004);
    rpc_proxy_push_ndr_ascii_string(&mut stub, &server);
    push_le_u32(&mut stub, 0);

    rpc_proxy_dce_response(call_id, &stub)
}

pub(super) fn rpc_proxy_rfri_get_new_dsa_response_for_principal(
    call_id: u32,
    endpoint_query: &str,
    principal: &AccountPrincipal,
) -> Vec<u8> {
    let server = rpc_proxy_referral_server_name_for_principal(endpoint_query, principal);
    let mut stub = Vec::with_capacity(40 + server.len());
    push_le_u32(&mut stub, 0);
    push_le_u32(&mut stub, 0x0002_0000);
    push_le_u32(&mut stub, 0x0002_0004);
    rpc_proxy_push_ndr_ascii_string(&mut stub, &server);
    push_le_u32(&mut stub, 0);

    rpc_proxy_dce_response(call_id, &stub)
}

#[cfg(test)]
pub(super) fn rpc_proxy_rfri_get_fqdn_response(call_id: u32, endpoint_query: &str) -> Vec<u8> {
    let server = rpc_proxy_referral_server_name(endpoint_query);
    let mut stub = Vec::with_capacity(32 + server.len());
    push_le_u32(&mut stub, 0x0002_0000);
    rpc_proxy_push_ndr_ascii_string(&mut stub, &server);
    push_le_u32(&mut stub, 0);

    rpc_proxy_dce_response(call_id, &stub)
}

pub(super) fn rpc_proxy_rfri_get_fqdn_response_for_principal(
    call_id: u32,
    endpoint_query: &str,
    principal: &AccountPrincipal,
) -> Vec<u8> {
    let server = rpc_proxy_referral_server_name_for_principal(endpoint_query, principal);
    let mut stub = Vec::with_capacity(32 + server.len());
    push_le_u32(&mut stub, 0x0002_0000);
    rpc_proxy_push_ndr_ascii_string(&mut stub, &server);
    push_le_u32(&mut stub, 0);

    rpc_proxy_dce_response(call_id, &stub)
}

#[cfg(test)]
fn rpc_proxy_referral_server_name(endpoint_query: &str) -> String {
    endpoint_query
        .split_once(':')
        .map(|(host, _)| host)
        .filter(|host| !host.is_empty())
        .unwrap_or("localhost")
        .to_ascii_lowercase()
}

fn rpc_proxy_referral_server_name_for_principal(
    endpoint_query: &str,
    principal: &AccountPrincipal,
) -> String {
    endpoint_query
        .split_once(':')
        .map(|(host, _)| host)
        .filter(|host| !host.is_empty())
        .map(str::to_ascii_lowercase)
        .unwrap_or_else(|| {
            let domain = principal
                .email
                .split_once('@')
                .map(|(_, domain)| domain)
                .filter(|domain| !domain.is_empty())
                .unwrap_or("localhost");
            format!("mail.{domain}").to_ascii_lowercase()
        })
}

fn rpc_proxy_nspi_bind_response(call_id: u32) -> Vec<u8> {
    let mut stub = Vec::with_capacity(28);
    stub.extend_from_slice(&0u32.to_le_bytes());
    stub.extend_from_slice(&0u32.to_le_bytes());
    stub.extend_from_slice(&[
        0x4c, 0x50, 0x45, 0x00, 0x4e, 0x53, 0x50, 0x49, 0x43, 0x54, 0x58, 0x00, 0x00, 0x00, 0x00,
        0x01,
    ]);
    stub.extend_from_slice(&0u32.to_le_bytes());

    rpc_proxy_dce_response(call_id, &stub)
}

fn rpc_proxy_nspi_unbind_response(call_id: u32) -> Vec<u8> {
    let mut stub = Vec::with_capacity(24);
    for _ in 0..5 {
        push_le_u32(&mut stub, 0);
    }
    push_le_u32(&mut stub, 0);

    rpc_proxy_dce_response(call_id, &stub)
}

fn rpc_proxy_nspi_update_stat_response(call_id: u32) -> Vec<u8> {
    let mut stub = Vec::with_capacity(44);
    push_le_u32(&mut stub, 0);
    push_le_u32(&mut stub, 0);
    push_le_u32(&mut stub, 2);
    push_le_u32(&mut stub, 0);
    push_le_u32(&mut stub, 1);
    push_le_u32(&mut stub, 1);
    push_le_u32(&mut stub, 0x04e4);
    push_le_u32(&mut stub, 0x0409);
    push_le_u32(&mut stub, 0x0409);
    push_le_u32(&mut stub, 0);
    push_le_u32(&mut stub, 0);

    rpc_proxy_dce_response(call_id, &stub)
}

#[cfg(test)]
fn rpc_proxy_nspi_query_rows_response(call_id: u32, request: &[u8]) -> Vec<u8> {
    let tags = rpc_proxy_nspi_requested_property_tags(request);
    let row_values = rpc_proxy_nspi_row_values(request, &tags);
    let mut stub = Vec::with_capacity(256);
    rpc_proxy_push_stat(&mut stub);
    rpc_proxy_push_rowset_pointer(&mut stub, &[row_values]);
    push_le_u32(&mut stub, 0);

    rpc_proxy_dce_response(call_id, &stub)
}

async fn rpc_proxy_nspi_query_rows_response_for_principal<S>(
    store: &S,
    call_id: u32,
    request: &[u8],
    principal: &AccountPrincipal,
) -> Vec<u8>
where
    S: ExchangeStore,
{
    let tags = rpc_proxy_nspi_requested_property_tags(request);
    let entries = rpc_proxy_address_book_entries(store, principal).await;
    let rows = rpc_proxy_filter_nspi_entries(&entries, request)
        .into_iter()
        .map(|entry| rpc_proxy_nspi_row_values_for_entry(&tags, entry))
        .collect::<Vec<_>>();
    let mut stub = Vec::with_capacity(256);
    rpc_proxy_push_stat(&mut stub);
    rpc_proxy_push_rowset_pointer(&mut stub, &rows);
    push_le_u32(&mut stub, 0);

    rpc_proxy_dce_response(call_id, &stub)
}

#[cfg(test)]
fn rpc_proxy_nspi_get_matches_response(call_id: u32, request: &[u8]) -> Vec<u8> {
    let tags = rpc_proxy_nspi_requested_property_tags(request);
    let row_values = rpc_proxy_nspi_row_values(request, &tags);
    let mut stub = Vec::with_capacity(280);
    rpc_proxy_push_stat(&mut stub);
    push_le_u32(&mut stub, 0x0002_0000);
    rpc_proxy_push_property_tag_array(&mut stub, &[2]);
    rpc_proxy_push_rowset_pointer(&mut stub, &[row_values]);
    push_le_u32(&mut stub, 0);

    rpc_proxy_dce_response(call_id, &stub)
}

async fn rpc_proxy_nspi_get_matches_response_for_principal<S>(
    store: &S,
    call_id: u32,
    request: &[u8],
    principal: &AccountPrincipal,
) -> Vec<u8>
where
    S: ExchangeStore,
{
    let tags = rpc_proxy_nspi_requested_property_tags(request);
    let entries = rpc_proxy_address_book_entries(store, principal).await;
    let matched = rpc_proxy_filter_nspi_entries(&entries, request);
    let rows = matched
        .iter()
        .map(|entry| rpc_proxy_nspi_row_values_for_entry(&tags, entry))
        .collect::<Vec<_>>();
    let mids = matched
        .iter()
        .map(|entry| rpc_proxy_nspi_entry_id(entry))
        .collect::<Vec<_>>();
    let mut stub = Vec::with_capacity(280);
    rpc_proxy_push_stat(&mut stub);
    push_le_u32(&mut stub, 0x0002_0000);
    rpc_proxy_push_property_tag_array(&mut stub, &mids);
    rpc_proxy_push_rowset_pointer(&mut stub, &rows);
    push_le_u32(&mut stub, 0);

    rpc_proxy_dce_response(call_id, &stub)
}
fn rpc_proxy_nspi_resort_restriction_response(call_id: u32) -> Vec<u8> {
    let mut stub = Vec::with_capacity(68);
    rpc_proxy_push_stat(&mut stub);
    push_le_u32(&mut stub, 0x0002_0000);
    rpc_proxy_push_property_tag_array(&mut stub, &[2]);
    push_le_u32(&mut stub, 0);

    rpc_proxy_dce_response(call_id, &stub)
}

fn rpc_proxy_nspi_minimal_ids_response(call_id: u32) -> Vec<u8> {
    let mut stub = Vec::with_capacity(32);
    push_le_u32(&mut stub, 0x0002_0000);
    rpc_proxy_push_property_tag_array(&mut stub, &[2]);
    push_le_u32(&mut stub, 0);

    rpc_proxy_dce_response(call_id, &stub)
}

fn rpc_proxy_nspi_property_tags_response(call_id: u32) -> Vec<u8> {
    let mut stub = Vec::with_capacity(80);
    push_le_u32(&mut stub, 0x0002_0000);
    rpc_proxy_push_property_tag_array(&mut stub, RPC_PROXY_NSPI_BOOTSTRAP_PROPERTY_TAGS);
    push_le_u32(&mut stub, 0);

    rpc_proxy_dce_response(call_id, &stub)
}

fn rpc_proxy_nspi_get_names_from_ids_response(call_id: u32, request: &[u8]) -> Vec<u8> {
    let tags = rpc_proxy_nspi_known_property_tags(request);
    let mut stub = Vec::with_capacity(24 + tags.len() * 12);
    push_le_u32(&mut stub, 0);
    push_le_u32(&mut stub, 0x0002_0000);
    push_le_u32(&mut stub, tags.len() as u32);
    push_le_u32(&mut stub, tags.len() as u32);
    for tag in tags {
        push_le_u32(&mut stub, 0);
        push_le_u32(&mut stub, 0);
        push_le_u32(&mut stub, tag);
    }
    push_le_u32(&mut stub, 0);

    rpc_proxy_dce_response(call_id, &stub)
}

#[cfg(test)]
fn rpc_proxy_nspi_get_props_response(call_id: u32, request: &[u8]) -> Vec<u8> {
    let tags = rpc_proxy_nspi_requested_property_tags(request);
    let row_values = rpc_proxy_nspi_row_values(request, &tags);
    let mut stub = Vec::with_capacity(192);
    push_le_u32(&mut stub, 0x0002_0000);
    rpc_proxy_push_property_row(&mut stub, &row_values);
    push_le_u32(&mut stub, 0);

    rpc_proxy_dce_response(call_id, &stub)
}

async fn rpc_proxy_nspi_get_props_response_for_principal<S>(
    store: &S,
    call_id: u32,
    request: &[u8],
    principal: &AccountPrincipal,
) -> Vec<u8>
where
    S: ExchangeStore,
{
    let tags = rpc_proxy_nspi_requested_property_tags(request);
    let entries = rpc_proxy_address_book_entries(store, principal).await;
    let row_values = rpc_proxy_requested_nspi_entry(&entries, request)
        .or_else(|| {
            entries
                .iter()
                .find(|entry| rpc_proxy_nspi_entry_is_principal(entry, principal))
        })
        .map(|entry| rpc_proxy_nspi_row_values_for_entry(&tags, entry))
        .unwrap_or_default();
    let mut stub = Vec::with_capacity(192);
    push_le_u32(&mut stub, 0x0002_0000);
    rpc_proxy_push_property_row(&mut stub, &row_values);
    push_le_u32(&mut stub, 0);

    rpc_proxy_dce_response(call_id, &stub)
}

fn rpc_proxy_nspi_compare_mids_response(call_id: u32) -> Vec<u8> {
    let mut stub = Vec::with_capacity(8);
    push_le_u32(&mut stub, 0);
    push_le_u32(&mut stub, 0);

    rpc_proxy_dce_response(call_id, &stub)
}

fn rpc_proxy_nspi_get_special_table_response(call_id: u32) -> Vec<u8> {
    let mut stub = Vec::with_capacity(220);
    push_le_u32(&mut stub, 1);
    let row = vec![
        (0x3001_001f, RpcProxyNspiValue::String("Global Address List".to_string())),
        (0x0ffe_0003, RpcProxyNspiValue::U32(2)),
        (0x3000_0003, RpcProxyNspiValue::U32(1)),
        (
            0x3002_001f,
            RpcProxyNspiValue::String(
                "/o=LPE/ou=Exchange Administrative Group/cn=Configuration/cn=Address Lists/cn=Global Address List".to_string(),
            ),
        ),
    ];
    rpc_proxy_push_rowset_pointer(&mut stub, &[row]);
    push_le_u32(&mut stub, 0);

    rpc_proxy_dce_response(call_id, &stub)
}

#[cfg(test)]
fn rpc_proxy_nspi_resolve_names_response(call_id: u32, request: &[u8]) -> Vec<u8> {
    const MID_RESOLVED: u32 = 2;
    const PR_DISPLAY_NAME_A: u32 = 0x3001_001e;
    const PR_EMAIL_ADDRESS_A: u32 = 0x3003_001e;

    let smtp_address = rpc_proxy_nspi_requested_smtp_address(request)
        .unwrap_or_else(|| "unknown@localhost".to_string());
    let display_name = rpc_proxy_display_name_for_smtp_address(&smtp_address);
    let property_tags = rpc_proxy_nspi_requested_resolve_property_tags(request);
    let row_values: Vec<(u32, String)> = property_tags
        .into_iter()
        .filter_map(|tag| match tag {
            PR_EMAIL_ADDRESS_A => Some((tag, smtp_address.clone())),
            PR_DISPLAY_NAME_A => Some((tag, display_name.clone())),
            _ => None,
        })
        .collect();
    let row_values = if row_values.is_empty() {
        vec![
            (PR_EMAIL_ADDRESS_A, smtp_address),
            (PR_DISPLAY_NAME_A, display_name),
        ]
    } else {
        row_values
    };

    let mut stub = Vec::with_capacity(192);
    let mut deferred_strings = Vec::new();

    push_le_u32(&mut stub, 0x0002_0000);
    rpc_proxy_push_property_tag_array(&mut stub, &[MID_RESOLVED]);

    push_le_u32(&mut stub, 0x0002_0004);
    push_le_u32(&mut stub, 1);
    push_le_u32(&mut stub, 1);
    push_le_u32(&mut stub, 0);
    push_le_u32(&mut stub, row_values.len() as u32);
    push_le_u32(&mut stub, 0x0002_0008);
    push_le_u32(&mut stub, row_values.len() as u32);

    for (index, (property_tag, value)) in row_values.iter().enumerate() {
        push_le_u32(&mut stub, *property_tag);
        push_le_u32(&mut stub, 0);
        push_le_u32(&mut stub, property_tag & 0xffff);
        push_le_u32(&mut stub, 0x0002_000c + (index as u32 * 4));
        rpc_proxy_push_ndr_ascii_string(&mut deferred_strings, value);
    }
    stub.extend_from_slice(&deferred_strings);
    push_le_u32(&mut stub, 0);

    rpc_proxy_dce_response(call_id, &stub)
}

pub(super) async fn rpc_proxy_nspi_resolve_names_response_for_principal<S>(
    store: &S,
    call_id: u32,
    request: &[u8],
    principal: &AccountPrincipal,
) -> Vec<u8>
where
    S: ExchangeStore,
{
    const MID_RESOLVED: u32 = 2;
    const PR_DISPLAY_NAME_A: u32 = 0x3001_001e;
    const PR_EMAIL_ADDRESS_A: u32 = 0x3003_001e;

    let entries = rpc_proxy_address_book_entries(store, principal).await;
    let principal_entry = rpc_proxy_principal_address_book_entry(principal);
    let lookup_values = rpc_proxy_nspi_lookup_values(request);
    let matched = lookup_values
        .first()
        .and_then(|value| rpc_proxy_match_nspi_entry(&entries, value))
        .or_else(|| {
            lookup_values
                .iter()
                .any(|value| rpc_proxy_nspi_principal_matches(value, principal))
                .then_some(&principal_entry)
        });
    let Some(entry) = matched else {
        let mut stub = Vec::with_capacity(64);
        push_le_u32(&mut stub, 0x0002_0000);
        rpc_proxy_push_property_tag_array(&mut stub, &[0]);
        push_le_u32(&mut stub, 0);
        push_le_u32(&mut stub, 0);
        return rpc_proxy_dce_response(call_id, &stub);
    };
    let smtp_address = entry.email.to_ascii_lowercase();
    let display_name = entry.display_name.clone();
    let property_tags = rpc_proxy_nspi_requested_resolve_property_tags(request);
    let row_values: Vec<(u32, String)> = property_tags
        .into_iter()
        .filter_map(|tag| match tag {
            PR_EMAIL_ADDRESS_A => Some((tag, smtp_address.clone())),
            PR_DISPLAY_NAME_A => Some((tag, display_name.clone())),
            _ => None,
        })
        .collect();
    let row_values = if row_values.is_empty() {
        vec![
            (PR_EMAIL_ADDRESS_A, smtp_address),
            (PR_DISPLAY_NAME_A, display_name),
        ]
    } else {
        row_values
    };

    let mut stub = Vec::with_capacity(192);
    let mut deferred_strings = Vec::new();

    push_le_u32(&mut stub, 0x0002_0000);
    rpc_proxy_push_property_tag_array(&mut stub, &[MID_RESOLVED]);

    push_le_u32(&mut stub, 0x0002_0004);
    push_le_u32(&mut stub, 1);
    push_le_u32(&mut stub, 1);
    push_le_u32(&mut stub, 0);
    push_le_u32(&mut stub, row_values.len() as u32);
    push_le_u32(&mut stub, 0x0002_0008);
    push_le_u32(&mut stub, row_values.len() as u32);

    for (index, (property_tag, value)) in row_values.iter().enumerate() {
        push_le_u32(&mut stub, *property_tag);
        push_le_u32(&mut stub, 0);
        push_le_u32(&mut stub, property_tag & 0xffff);
        push_le_u32(&mut stub, 0x0002_000c + (index as u32 * 4));
        rpc_proxy_push_ndr_ascii_string(&mut deferred_strings, value);
    }
    stub.extend_from_slice(&deferred_strings);
    push_le_u32(&mut stub, 0);

    rpc_proxy_dce_response(call_id, &stub)
}

const RPC_PROXY_NSPI_BOOTSTRAP_PROPERTY_TAGS: &[u32] = &[
    0x3001_001f,
    0x39fe_001f,
    0x3003_001f,
    0x3a00_001f,
    0x0ffe_0003,
    0x3000_0003,
    0x3004_001f,
    0x3002_001f,
    0x3005_001f,
];

enum RpcProxyNspiValue {
    String(String),
    U32(u32),
}

fn rpc_proxy_nspi_requested_property_tags(request: &[u8]) -> Vec<u32> {
    let mut tags = rpc_proxy_nspi_known_property_tags(request);
    if tags.is_empty() {
        tags.extend_from_slice(RPC_PROXY_NSPI_BOOTSTRAP_PROPERTY_TAGS);
    }
    tags
}

fn rpc_proxy_nspi_known_property_tags(request: &[u8]) -> Vec<u32> {
    let mut tags = Vec::new();
    let mut offset = 24usize;
    while offset + 4 <= request.len() {
        let Some(tag) = read_le_u32(request, offset) else {
            break;
        };
        if RPC_PROXY_NSPI_BOOTSTRAP_PROPERTY_TAGS.contains(&tag) && !tags.contains(&tag) {
            tags.push(tag);
        }
        offset += 4;
    }
    tags
}

fn rpc_proxy_nspi_requested_resolve_property_tags(request: &[u8]) -> Vec<u32> {
    let mut tags = Vec::new();
    let mut offset = 24usize;
    while offset + 4 <= request.len() {
        let Some(tag) = read_le_u32(request, offset) else {
            break;
        };
        if matches!(tag, 0x3001_001e | 0x3003_001e) && !tags.contains(&tag) {
            tags.push(tag);
        }
        offset += 4;
    }
    tags
}

fn rpc_proxy_nspi_requested_smtp_address(request: &[u8]) -> Option<String> {
    const SMTP_PREFIX_UTF16LE: &[u8] = b"=\0S\0M\0T\0P\0:\0";
    const SMTP_PREFIX_ASCII: &[u8] = b"=SMTP:";

    if let Some(start) = request.windows(SMTP_PREFIX_ASCII.len()).position(|window| {
        window
            .iter()
            .zip(SMTP_PREFIX_ASCII)
            .all(|(actual, expected)| actual.eq_ignore_ascii_case(expected))
    }) {
        let mut end = start + SMTP_PREFIX_ASCII.len();
        while end < request.len() && request[end] != 0 {
            end += 1;
        }
        if let Ok(value) = std::str::from_utf8(&request[start + SMTP_PREFIX_ASCII.len()..end]) {
            let value = normalization::normalize_trimmed_lowercase(value);
            if value.contains('@') {
                return Some(value);
            }
        }
    }

    let start = request
        .windows(SMTP_PREFIX_UTF16LE.len())
        .position(|window| {
            window
                .chunks_exact(2)
                .zip(SMTP_PREFIX_UTF16LE.chunks_exact(2))
                .all(|(actual, expected)| {
                    actual[0].eq_ignore_ascii_case(&expected[0]) && actual[1] == expected[1]
                })
        })?;
    let mut units = Vec::new();
    let mut offset = start + SMTP_PREFIX_UTF16LE.len();
    while offset + 1 < request.len() {
        let unit = u16::from_le_bytes([request[offset], request[offset + 1]]);
        if unit == 0 {
            break;
        }
        units.push(unit);
        offset += 2;
    }
    String::from_utf16(&units)
        .ok()
        .map(|value| normalization::normalize_trimmed_lowercase(&value))
        .filter(|value| value.contains('@'))
}

#[cfg(test)]
fn rpc_proxy_display_name_for_smtp_address(address: &str) -> String {
    let local_part = address.split('@').next().unwrap_or(address).trim();
    let mut chars = local_part.chars();
    let Some(first) = chars.next() else {
        return address.to_string();
    };
    let mut display_name = first.to_uppercase().collect::<String>();
    display_name.push_str(chars.as_str());
    display_name
}

fn rpc_proxy_push_property_tag_array(buffer: &mut Vec<u8>, values: &[u32]) {
    push_le_u32(buffer, values.len() as u32 + 1);
    push_le_u32(buffer, values.len() as u32);
    push_le_u32(buffer, 0);
    push_le_u32(buffer, values.len() as u32);
    for value in values {
        push_le_u32(buffer, *value);
    }
}

fn rpc_proxy_push_stat(buffer: &mut Vec<u8>) {
    push_le_u32(buffer, 0);
    push_le_u32(buffer, 0);
    push_le_u32(buffer, 2);
    push_le_u32(buffer, 0);
    push_le_u32(buffer, 1);
    push_le_u32(buffer, 1);
    push_le_u32(buffer, 0x04e4);
    push_le_u32(buffer, 0x0409);
    push_le_u32(buffer, 0x0409);
}

#[cfg(test)]
fn rpc_proxy_nspi_row_values(request: &[u8], tags: &[u32]) -> Vec<(u32, RpcProxyNspiValue)> {
    let smtp_address = rpc_proxy_nspi_requested_smtp_address(request)
        .unwrap_or_else(|| "unknown@localhost".to_string());
    let display_name = rpc_proxy_display_name_for_smtp_address(&smtp_address);
    tags.iter()
        .map(|tag| {
            let value = match *tag {
                0x3001_001f | 0x3a00_001f => RpcProxyNspiValue::String(display_name.clone()),
                0x39fe_001f | 0x3003_001f | 0x3004_001f => {
                    RpcProxyNspiValue::String(smtp_address.clone())
                }
                0x3002_001f => RpcProxyNspiValue::String("SMTP".to_string()),
                0x3005_001f => RpcProxyNspiValue::String(format!(
                    "/o=LPE/ou=Exchange Administrative Group/cn=Recipients/cn={}",
                    smtp_address.replace('@', "-").replace('.', "-")
                )),
                0x0ffe_0003 => RpcProxyNspiValue::U32(6),
                0x3000_0003 => RpcProxyNspiValue::U32(2),
                _ if *tag & 0xffff == 0x0003 => RpcProxyNspiValue::U32(0),
                _ => RpcProxyNspiValue::String(String::new()),
            };
            (*tag, value)
        })
        .collect()
}

async fn rpc_proxy_address_book_entries<S>(
    store: &S,
    principal: &AccountPrincipal,
) -> Vec<ExchangeAddressBookEntry>
where
    S: ExchangeStore,
{
    match store.fetch_address_book_entries(principal).await {
        Ok(entries) => entries,
        Err(_) => Vec::new(),
    }
}

fn rpc_proxy_principal_address_book_entry(
    principal: &AccountPrincipal,
) -> ExchangeAddressBookEntry {
    ExchangeAddressBookEntry {
        id: principal.account_id,
        display_name: principal.display_name.clone(),
        email: principal.email.clone(),
        entry_kind: ExchangeAddressBookEntryKind::Account,
        directory_kind: ExchangeAddressBookDirectoryKind::Person,
        member_emails: Vec::new(),
        details: ExchangeAddressBookEntryDetails::default(),
    }
}

fn rpc_proxy_nspi_row_values_for_entry(
    tags: &[u32],
    entry: &ExchangeAddressBookEntry,
) -> Vec<(u32, RpcProxyNspiValue)> {
    let smtp_address = entry.email.to_ascii_lowercase();
    let display_name = entry.display_name.clone();
    tags.iter()
        .map(|tag| {
            let value = match *tag {
                0x3001_001f | 0x3a00_001f => RpcProxyNspiValue::String(display_name.clone()),
                0x39fe_001f | 0x3003_001f | 0x3004_001f => {
                    RpcProxyNspiValue::String(smtp_address.clone())
                }
                0x3002_001f => RpcProxyNspiValue::String("SMTP".to_string()),
                0x3005_001f => RpcProxyNspiValue::String(format!(
                    "/o=LPE/ou=Exchange Administrative Group/cn=Recipients/cn={}",
                    rpc_proxy_nspi_entry_legacy_name(entry)
                )),
                0x0ffe_0003 => RpcProxyNspiValue::U32(6),
                0x3000_0003 => RpcProxyNspiValue::U32(rpc_proxy_nspi_entry_id(entry)),
                _ if *tag & 0xffff == 0x0003 => RpcProxyNspiValue::U32(0),
                _ => RpcProxyNspiValue::String(String::new()),
            };
            (*tag, value)
        })
        .collect()
}

fn rpc_proxy_nspi_entry_id(entry: &ExchangeAddressBookEntry) -> u32 {
    let bytes = entry.id.as_bytes();
    let value = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
    match entry.entry_kind {
        ExchangeAddressBookEntryKind::Account => value | 0x8000_0000,
        ExchangeAddressBookEntryKind::Contact | ExchangeAddressBookEntryKind::DistributionList => {
            value | 0x4000_0000
        }
    }
    .max(2)
}

fn rpc_proxy_nspi_entry_legacy_name(entry: &ExchangeAddressBookEntry) -> String {
    let prefix = match entry.entry_kind {
        ExchangeAddressBookEntryKind::Account => "acct",
        ExchangeAddressBookEntryKind::Contact => "contact",
        ExchangeAddressBookEntryKind::DistributionList => "group",
    };
    let source = if entry.email.trim().is_empty() {
        entry.id.to_string()
    } else {
        entry.email.clone()
    };
    let legacy_user = source
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect::<String>();
    format!("{prefix}-{legacy_user}")
}

fn rpc_proxy_filter_nspi_entries<'a>(
    entries: &'a [ExchangeAddressBookEntry],
    request: &[u8],
) -> Vec<&'a ExchangeAddressBookEntry> {
    let values = rpc_proxy_nspi_lookup_values(request);
    if values.is_empty() {
        return entries.iter().collect();
    }
    entries
        .iter()
        .filter(|entry| {
            values
                .iter()
                .any(|value| rpc_proxy_nspi_entry_matches(entry, value))
        })
        .collect()
}

fn rpc_proxy_requested_nspi_entry<'a>(
    entries: &'a [ExchangeAddressBookEntry],
    request: &[u8],
) -> Option<&'a ExchangeAddressBookEntry> {
    rpc_proxy_nspi_requested_mids(request)
        .iter()
        .find_map(|mid| {
            entries
                .iter()
                .find(|entry| rpc_proxy_nspi_entry_id(entry) == *mid)
        })
        .or_else(|| {
            rpc_proxy_nspi_lookup_values(request)
                .iter()
                .find_map(|value| rpc_proxy_match_nspi_entry(entries, value))
        })
}

fn rpc_proxy_match_nspi_entry<'a>(
    entries: &'a [ExchangeAddressBookEntry],
    value: &str,
) -> Option<&'a ExchangeAddressBookEntry> {
    entries
        .iter()
        .find(|entry| {
            rpc_proxy_nspi_entry_matches(entry, value)
                && rpc_proxy_nspi_entry_exact_match(entry, value)
        })
        .or_else(|| {
            entries
                .iter()
                .find(|entry| rpc_proxy_nspi_entry_matches(entry, value))
        })
}

fn rpc_proxy_nspi_entry_is_principal(
    entry: &ExchangeAddressBookEntry,
    principal: &AccountPrincipal,
) -> bool {
    entry.entry_kind == ExchangeAddressBookEntryKind::Account && entry.id == principal.account_id
}

fn rpc_proxy_nspi_principal_matches(value: &str, principal: &AccountPrincipal) -> bool {
    let value = rpc_proxy_normalize_nspi_lookup_value(value);
    let email = principal.email.to_ascii_lowercase();
    let display_name = principal.display_name.to_ascii_lowercase();
    value == email
        || value == display_name
        || value == format!("smtp:{email}")
        || value == format!("=smtp:{email}")
        || email.contains(value.as_str())
}

fn rpc_proxy_nspi_entry_exact_match(entry: &ExchangeAddressBookEntry, value: &str) -> bool {
    let value = rpc_proxy_normalize_nspi_lookup_value(value);
    let email = entry.email.to_ascii_lowercase();
    value == email
        || value == entry.display_name.to_ascii_lowercase()
        || value
            == format!(
                "/o=lpe/ou=exchange administrative group/cn=recipients/cn={}",
                rpc_proxy_nspi_entry_legacy_name(entry)
            )
}

fn rpc_proxy_nspi_entry_matches(entry: &ExchangeAddressBookEntry, value: &str) -> bool {
    let value = rpc_proxy_normalize_nspi_lookup_value(value);
    if value.is_empty() {
        return false;
    }
    rpc_proxy_nspi_entry_exact_match(entry, &value)
        || entry.email.to_ascii_lowercase().contains(value.as_str())
        || entry
            .display_name
            .to_ascii_lowercase()
            .contains(value.as_str())
}

fn rpc_proxy_nspi_requested_mids(request: &[u8]) -> Vec<u32> {
    let mut mids = Vec::new();
    let mut offset = 0usize;
    while offset + 4 <= request.len() {
        if let Some(value) = read_le_u32(request, offset) {
            if value >= 2 && !mids.contains(&value) {
                mids.push(value);
            }
        }
        offset += 4;
    }
    mids
}

pub(super) fn rpc_proxy_nspi_lookup_values(request: &[u8]) -> Vec<String> {
    let mut values = Vec::new();
    if let Some(address) = rpc_proxy_nspi_requested_smtp_address(request) {
        values.push(address);
    }
    values.extend(rpc_proxy_nspi_ascii_lookup_values(request));
    values.extend(rpc_proxy_nspi_utf16_lookup_values(request));
    values.sort();
    values.dedup();
    values
}

fn rpc_proxy_nspi_ascii_lookup_values(request: &[u8]) -> Vec<String> {
    request
        .split(|byte| *byte == 0)
        .filter_map(|bytes| {
            if bytes.len() < 3 {
                return None;
            }
            let value = String::from_utf8_lossy(bytes);
            let value = rpc_proxy_normalize_nspi_lookup_value(&value);
            (!value.is_empty() && (value.contains('@') || value.contains("/cn="))).then_some(value)
        })
        .collect()
}

fn rpc_proxy_nspi_utf16_lookup_values(request: &[u8]) -> Vec<String> {
    let mut values = Vec::new();
    let mut start = 0usize;
    while start + 3 < request.len() {
        let mut units = Vec::new();
        let mut offset = start;
        while offset + 1 < request.len() {
            let unit = u16::from_le_bytes([request[offset], request[offset + 1]]);
            if unit == 0 {
                break;
            }
            if unit < 0x20 && !matches!(unit, 0x09 | 0x0a | 0x0d) {
                units.clear();
                break;
            }
            units.push(unit);
            offset += 2;
        }
        if units.len() >= 3 {
            if let Ok(value) = String::from_utf16(&units) {
                let value = rpc_proxy_normalize_nspi_lookup_value(&value);
                if !value.is_empty() && (value.contains('@') || value.contains("/cn=")) {
                    values.push(value);
                }
            }
        }
        start += 1;
    }
    values
}

fn rpc_proxy_normalize_nspi_lookup_value(value: &str) -> String {
    normalization::normalize_smtp_lookup_value(value)
}

fn rpc_proxy_push_rowset_pointer(buffer: &mut Vec<u8>, rows: &[Vec<(u32, RpcProxyNspiValue)>]) {
    push_le_u32(buffer, 0x0002_0004);
    push_le_u32(buffer, rows.len() as u32);
    push_le_u32(buffer, rows.len() as u32);
    for row in rows {
        rpc_proxy_push_property_row(buffer, row);
    }
}

fn rpc_proxy_push_property_row(buffer: &mut Vec<u8>, row_values: &[(u32, RpcProxyNspiValue)]) {
    let mut deferred = Vec::new();
    push_le_u32(buffer, 0);
    push_le_u32(buffer, row_values.len() as u32);
    push_le_u32(buffer, 0x0002_0008);
    push_le_u32(buffer, row_values.len() as u32);
    for (index, (property_tag, value)) in row_values.iter().enumerate() {
        push_le_u32(buffer, *property_tag);
        push_le_u32(buffer, 0);
        push_le_u32(buffer, property_tag & 0xffff);
        match value {
            RpcProxyNspiValue::U32(value) => push_le_u32(buffer, *value),
            RpcProxyNspiValue::String(value) if property_tag & 0xffff == 0x001f => {
                push_le_u32(buffer, 0x0002_000c + (index as u32 * 4));
                rpc_proxy_push_ndr_utf16_string(&mut deferred, value);
            }
            RpcProxyNspiValue::String(value) => {
                push_le_u32(buffer, 0x0002_000c + (index as u32 * 4));
                rpc_proxy_push_ndr_ascii_string(&mut deferred, value);
            }
        }
    }
    buffer.extend_from_slice(&deferred);
}
