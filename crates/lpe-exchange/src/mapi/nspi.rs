use super::properties::{
    write_ascii_z, write_multi_string, write_multi_string8, NSPI_PERMANENT_ENTRY_ID_PROVIDER_UID,
};
use super::rop::*;
use super::session::*;
use super::transport::*;
use super::wire::MapiHttpRequestType as MapiRequestType;
use super::*;
use crate::store::ExchangeAddressBookEntryDetails;
use lpe_domain::normalization;
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

mod diagnostics;
mod property_values;
mod special_tables;

#[cfg(test)]
use diagnostics::{
    format_nspi_duplicate_entry_keys_for_debug, format_nspi_entry_summaries_for_debug,
};
use diagnostics::{
    log_nspi_dn_to_mid_debug, log_nspi_get_props_debug, log_nspi_response_contract,
    log_nspi_rowset_debug, nspi_raw_property_tag_candidates,
};
use property_values::{
    allocate_nspi_entry_identities, allocate_principal_nspi_identity, nspi_get_props_property_tags,
    nspi_property_tags_response, nspi_resolved_entry_row, NSPI_BOOTSTRAP_PROPERTY_TAGS,
};
pub(in crate::mapi) use property_values::{
    nspi_entry_display_type, nspi_entry_id, nspi_entry_property_value_list,
    nspi_known_unsupported_property_tag_name, nspi_property_tag_is_supported,
    nspi_requested_property_tags, principal_address_book_entry, principal_minimal_entry_id,
    write_address_book_tagged_property_value, write_large_property_tag_array, NspiValue,
};
#[cfg(test)]
use property_values::{
    nspi_entry_value, NSPI_ADDITIONAL_REQUESTED_PROPERTY_TAGS, NSPI_SUPPORTED_REQUEST_TYPES,
};
#[cfg(test)]
use special_tables::NSPI_UNICODE_STRINGS_FLAG;
use special_tables::{nspi_hierarchy_info_response, nspi_special_table_response};

const NSPI_ROWSET_DEBUG_SCHEMA: &str = "nspi-rowset-explicit-table-v2";

static NSPI_OBJECT_IDS: OnceLock<Mutex<HashMap<(Uuid, u8, Uuid), u64>>> = OnceLock::new();

pub(in crate::mapi) async fn handle_nspi_request<S>(
    store: &S,
    principal: &AccountPrincipal,
    headers: &HeaderMap,
    request: &[u8],
    request_type: MapiRequestType,
    request_id: &str,
) -> Response
where
    S: ExchangeStore,
{
    match request_type {
        MapiRequestType::Bind => bind_response(MapiEndpoint::Nspi, principal, headers, request_id),
        MapiRequestType::Unbind => {
            disconnect_response(MapiEndpoint::Nspi, principal, headers, request_id, "Unbind")
        }
        MapiRequestType::CompareMids => nspi_u32_result_response("CompareMIds", request_id, 0),
        MapiRequestType::DnToEph => {
            nspi_dn_to_mid_response(store, principal, request, "DNToEPH", request_id).await
        }
        MapiRequestType::DnToMid => {
            nspi_dn_to_mid_response(store, principal, request, "DNToMId", request_id).await
        }
        MapiRequestType::GetMatches => {
            nspi_matches_response(store, principal, request, request_id).await
        }
        MapiRequestType::GetPropList => nspi_property_tags_response("GetPropList", request_id),
        MapiRequestType::GetProps => {
            nspi_props_response(store, principal, request, "GetProps", request_id).await
        }
        MapiRequestType::GetHierarchyInfo => {
            nspi_hierarchy_info_response(principal, request, request_id)
        }
        MapiRequestType::GetSpecialTable => {
            nspi_special_table_response(principal, request, request_id)
        }
        MapiRequestType::GetTemplateInfo => {
            nspi_template_info_response(store, principal, request_id).await
        }
        MapiRequestType::ModLinkAtt => nspi_disabled_mutation_response(
            "ModLinkAtt",
            request_id,
            "NSPI link-attribute mutation is disabled; LPE address-book data is projected from canonical accounts, contacts, and group aliases.",
        ),
        MapiRequestType::ModProps => nspi_disabled_mutation_response(
            "ModProps",
            request_id,
            "NSPI property mutation is disabled; LPE address-book data is projected from canonical accounts, contacts, and group aliases.",
        ),
        MapiRequestType::GetAddressBookUrl => {
            endpoint_url_response("GetAddressBookUrl", request_id, headers, "/mapi/nspi/")
        }
        MapiRequestType::GetMailboxUrl => {
            endpoint_url_response("GetMailboxUrl", request_id, headers, "/mapi/emsmdb/")
        }
        MapiRequestType::QueryColumns => nspi_property_tags_response("QueryColumns", request_id),
        MapiRequestType::QueryRows => {
            nspi_rowset_response(store, principal, request, "QueryRows", request_id).await
        }
        MapiRequestType::ResolveNames => {
            resolve_names_response(store, principal, request, request_id).await
        }
        MapiRequestType::ResortRestriction => {
            nspi_minimal_ids_response("ResortRestriction", store, principal, request_id).await
        }
        MapiRequestType::SeekEntries => {
            nspi_rowset_response(store, principal, request, "SeekEntries", request_id).await
        }
        MapiRequestType::UpdateStat => nspi_update_stat_response(request_id),
        other => mapi_diagnostic_response(
            other.header_value(),
            request_id,
            5,
            "request type is not valid for the NSPI endpoint",
        ),
    }
}

pub(in crate::mapi) fn bind_response(
    endpoint: MapiEndpoint,
    principal: &AccountPrincipal,
    headers: &HeaderMap,
    request_id: &str,
) -> Response {
    let (session_id, reconnected) =
        match reconnect_session(endpoint, principal, headers, "Bind", request_id) {
            Ok(Some(session_id)) => (session_id, true),
            Ok(None) => (
                create_session(endpoint, principal, "Bind", request_id),
                false,
            ),
            Err(response) => return response,
        };
    log_mapi_session_establish(
        endpoint,
        principal,
        headers,
        "Bind",
        request_id,
        &session_id,
        reconnected,
    );
    let cookies = session_context_cookies(endpoint, &session_id, false);
    let mut body = Vec::new();
    write_u32(&mut body, 0);
    write_u32(&mut body, 0);
    body.extend_from_slice(&NSPI_SERVER_GUID);
    write_u32(&mut body, 0);
    mapi_response_with_cookies("Bind", request_id, 0, body, cookies)
}

pub(in crate::mapi) fn endpoint_url_response(
    request_type: &str,
    request_id: &str,
    headers: &HeaderMap,
    path: &str,
) -> Response {
    let url = public_endpoint_url(headers, path);
    tracing::info!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = "nspi",
        request_type = request_type,
        mapi_request_id = request_id,
        endpoint_url_path = path,
        endpoint_url = %url,
        host = %safe_header(headers, "host").unwrap_or_default(),
        client_application = %safe_header(headers, "x-clientapplication").unwrap_or_default(),
        client_info = %safe_header(headers, "x-clientinfo").unwrap_or_default(),
        client_flow_key = %client_flow_key(&safe_header(headers, "x-clientinfo").unwrap_or_default()),
        client_request_id = %safe_header(headers, "client-request-id").unwrap_or_default(),
        trace_id = %safe_header(headers, "x-trace-id").unwrap_or_default(),
        message = "rca debug nspi endpoint url"
    );
    let mut body = Vec::new();
    write_u32(&mut body, 0);
    write_u32(&mut body, 0);
    write_utf16z(&mut body, &url);
    write_u32(&mut body, 0);
    mapi_response(request_type, request_id, 0, body, None)
}

pub(in crate::mapi) fn nspi_disabled_mutation_response(
    request_type: &str,
    request_id: &str,
    message: &str,
) -> Response {
    mapi_diagnostic_response(request_type, request_id, 16, message)
}

pub(in crate::mapi) async fn resolve_names_response<S>(
    store: &S,
    principal: &AccountPrincipal,
    request: &[u8],
    request_id: &str,
) -> Response
where
    S: ExchangeStore,
{
    let columns = resolve_names_columns(request);
    let requested_names = resolve_names_requested_values(request);
    let entries = match store.fetch_address_book_entries(principal).await {
        Ok(entries) => entries,
        Err(error) => {
            return mapi_diagnostic_response(
                "ResolveNames",
                request_id,
                4,
                &format!("failed to load address book entries: {error}"),
            );
        }
    };
    if let Err(error) = allocate_nspi_entry_identities(store, principal, &entries).await {
        return mapi_diagnostic_response(
            "ResolveNames",
            request_id,
            4,
            &format!("failed to project address book identifiers: {error}"),
        );
    }
    if let Err(error) = allocate_principal_nspi_identity(store, principal).await {
        return mapi_diagnostic_response(
            "ResolveNames",
            request_id,
            4,
            &format!("failed to project authenticated address book identifier: {error}"),
        );
    }
    let principal_entry = principal_address_book_entry(principal);
    let matched = requested_names
        .first()
        .and_then(|name| nspi_match_entry(principal.account_id, &entries, name))
        .or_else(|| {
            requested_names
                .iter()
                .any(|name| nspi_lookup_matches_principal(name, principal))
                .then_some(&principal_entry)
        })
        .or_else(|| {
            requested_names
                .is_empty()
                .then(|| {
                    entries
                        .iter()
                        .find(|entry| nspi_entry_is_principal(entry, principal))
                })
                .flatten()
        });

    let mut body = Vec::new();
    write_u32(&mut body, 0);
    write_u32(&mut body, 0);
    write_u32(&mut body, NSPI_UNICODE_CODEPAGE);
    body.push(1);
    write_u32(&mut body, 1);
    write_u32(
        &mut body,
        if matched.is_some() {
            NSPI_MID_RESOLVED
        } else {
            0
        },
    );
    if let Some(entry) = matched {
        body.push(1);
        write_large_property_tag_array(&mut body, &columns);
        write_u32(&mut body, 1);
        body.extend_from_slice(&nspi_resolved_entry_row(
            principal.account_id,
            entry,
            &columns,
            &entries,
        ));
    } else {
        body.push(0);
    }
    write_u32(&mut body, 0);
    mapi_response("ResolveNames", request_id, 0, body, None)
}

pub(in crate::mapi) fn resolve_names_columns(request: &[u8]) -> Vec<u32> {
    parse_resolve_names_columns(request)
        .filter(|columns| !columns.is_empty())
        .unwrap_or_else(|| NSPI_BOOTSTRAP_PROPERTY_TAGS.to_vec())
}

pub(in crate::mapi) fn parse_resolve_names_columns(request: &[u8]) -> Option<Vec<u32>> {
    let mut cursor = Cursor::new(request);
    let _reserved = cursor.read_u32().ok()?;
    let has_state = cursor.read_u8().ok()? != 0;
    if has_state {
        cursor.read_bytes(36).ok()?;
    }
    let has_property_tags = cursor.read_u8().ok()? != 0;
    if !has_property_tags {
        return None;
    }
    let count = cursor.read_u32().ok()? as usize;
    if count == 0 || count > 128 {
        return None;
    }
    let mut columns = Vec::with_capacity(count);
    for _ in 0..count {
        columns.push(cursor.read_u32().ok()?);
    }
    Some(columns)
}

pub(in crate::mapi) fn resolve_names_requested_values(request: &[u8]) -> Vec<String> {
    parse_resolve_names_values(request)
        .filter(|values| !values.is_empty())
        .unwrap_or_else(|| scan_address_book_lookup_values(request))
}

pub(in crate::mapi) fn parse_resolve_names_values(request: &[u8]) -> Option<Vec<String>> {
    let mut cursor = Cursor::new(request);
    let _reserved = cursor.read_u32().ok()?;
    if cursor.read_u8().ok()? != 0 {
        cursor.read_bytes(36).ok()?;
    }
    if cursor.read_u8().ok()? != 0 {
        let count = cursor.read_u32().ok()? as usize;
        if count > 128 {
            return None;
        }
        cursor.read_bytes(count.checked_mul(4)?).ok()?;
    }
    if cursor.read_u8().ok()? == 0 {
        return Some(Vec::new());
    }
    let count = cursor.read_u32().ok()? as usize;
    if count > 128 {
        return None;
    }
    let mut values = Vec::new();
    for _ in 0..count {
        let size = cursor.read_u16().ok()? as usize;
        let bytes = cursor.read_bytes(size).ok()?;
        if let Some(value) = decode_utf16le_string(bytes) {
            let value = normalize_nspi_lookup_value(&value);
            if !value.is_empty() {
                values.push(value);
            }
        }
    }
    Some(values)
}

pub(in crate::mapi) fn nspi_u32_result_response(
    request_type: &str,
    request_id: &str,
    value: u32,
) -> Response {
    let mut body = Vec::new();
    write_u32(&mut body, 0);
    write_u32(&mut body, 0);
    write_u32(&mut body, value);
    write_u32(&mut body, 0);
    mapi_response(request_type, request_id, 0, body, None)
}

pub(in crate::mapi) async fn nspi_dn_to_mid_response<S>(
    store: &S,
    principal: &AccountPrincipal,
    request: &[u8],
    request_type: &str,
    request_id: &str,
) -> Response
where
    S: ExchangeStore,
{
    let entries = match store.fetch_address_book_entries(principal).await {
        Ok(entries) => entries,
        Err(error) => {
            return mapi_diagnostic_response(
                request_type,
                request_id,
                4,
                &format!("failed to load address book entries: {error}"),
            );
        }
    };
    if let Err(error) = allocate_nspi_entry_identities(store, principal, &entries).await {
        return mapi_diagnostic_response(
            request_type,
            request_id,
            4,
            &format!("failed to project address book identifiers: {error}"),
        );
    }
    if let Err(error) = allocate_principal_nspi_identity(store, principal).await {
        return mapi_diagnostic_response(
            request_type,
            request_id,
            4,
            &format!("failed to project authenticated address book identifier: {error}"),
        );
    }
    let values = resolve_names_requested_values(request);
    let matched = nspi_dn_to_mid_match(principal, &entries, &values);
    log_nspi_dn_to_mid_debug(
        principal,
        request_type,
        request_id,
        request,
        &values,
        &matched,
    );
    let mut body = Vec::new();
    write_u32(&mut body, 0);
    write_u32(&mut body, 0);
    body.push(1);
    write_u32(&mut body, 1);
    write_u32(&mut body, matched.mid.unwrap_or(0));
    write_u32(&mut body, 0);
    mapi_response(request_type, request_id, 0, body, None)
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct NspiDnToMidMatch {
    mid: Option<u32>,
    source: &'static str,
}

fn nspi_dn_to_mid_match(
    principal: &AccountPrincipal,
    entries: &[ExchangeAddressBookEntry],
    values: &[String],
) -> NspiDnToMidMatch {
    if let Some(entry) = values
        .first()
        .and_then(|value| nspi_match_entry(principal.account_id, entries, value))
    {
        return NspiDnToMidMatch {
            mid: Some(nspi_entry_id(principal.account_id, entry)),
            source: "address_book_entry",
        };
    }
    if values
        .iter()
        .any(|value| nspi_lookup_matches_principal(value, principal))
    {
        return NspiDnToMidMatch {
            mid: Some(principal_minimal_entry_id(principal)),
            source: "principal_alias",
        };
    }
    if let Some(entry) = values
        .is_empty()
        .then(|| {
            entries
                .iter()
                .find(|entry| nspi_entry_is_principal(entry, principal))
        })
        .flatten()
    {
        return NspiDnToMidMatch {
            mid: Some(nspi_entry_id(principal.account_id, entry)),
            source: "principal_default",
        };
    }
    NspiDnToMidMatch {
        mid: None,
        source: "none",
    }
}

pub(in crate::mapi) async fn nspi_props_response<S>(
    store: &S,
    principal: &AccountPrincipal,
    request: &[u8],
    request_type: &str,
    request_id: &str,
) -> Response
where
    S: ExchangeStore,
{
    let entries = match store.fetch_address_book_entries(principal).await {
        Ok(entries) => entries,
        Err(error) => {
            return mapi_diagnostic_response(
                request_type,
                request_id,
                4,
                &format!("failed to load address book entries: {error}"),
            );
        }
    };
    if let Err(error) = allocate_nspi_entry_identities(store, principal, &entries).await {
        return mapi_diagnostic_response(
            request_type,
            request_id,
            4,
            &format!("failed to project address book identifiers: {error}"),
        );
    }
    if let Err(error) = allocate_principal_nspi_identity(store, principal).await {
        return mapi_diagnostic_response(
            request_type,
            request_id,
            4,
            &format!("failed to project authenticated address book identifier: {error}"),
        );
    }
    let tags = nspi_get_props_property_tags(request);
    let raw_tag_candidates = nspi_raw_property_tag_candidates(request);
    let dropped_tags = raw_tag_candidates
        .iter()
        .copied()
        .filter(|tag| !tags.contains(tag))
        .collect::<Vec<_>>();
    let principal_entry = principal_address_book_entry(principal);
    let principal_id = nspi_entry_id(principal.account_id, &principal_entry);
    let lookup_values = resolve_names_requested_values(request);
    let entry = nspi_stat_current_rec(request)
        .and_then(|current_rec| {
            entries
                .iter()
                .find(|entry| nspi_entry_id(principal.account_id, entry) == current_rec)
                .cloned()
                .or_else(|| (current_rec == principal_id).then_some(principal_entry.clone()))
        })
        .or_else(|| nspi_requested_entry(principal.account_id, request, &entries).cloned())
        .or_else(|| {
            nspi_requested_entry_ids(request)
                .contains(&principal_id)
                .then_some(principal_entry.clone())
        })
        .or_else(|| {
            lookup_values
                .iter()
                .any(|value| nspi_lookup_matches_principal(value, principal))
                .then_some(principal_entry.clone())
        })
        .or_else(|| {
            (lookup_values.is_empty() && !nspi_request_has_entry_selector(request)).then(|| {
                entries
                    .iter()
                    .find(|entry| nspi_entry_is_principal(entry, principal))
                    .cloned()
                    .unwrap_or_else(|| principal_entry.clone())
            })
        });
    let mut body = Vec::new();
    write_u32(&mut body, 0);
    write_u32(&mut body, 0);
    write_u32(&mut body, NSPI_UNICODE_CODEPAGE);
    if let Some(entry) = entry.as_ref() {
        body.push(1);
        body.extend_from_slice(&nspi_entry_property_value_list(
            principal.account_id,
            entry,
            &tags,
            &entries,
        ));
    } else {
        body.push(0);
    }
    log_nspi_get_props_debug(
        principal,
        request,
        request_type,
        &raw_tag_candidates,
        &tags,
        &dropped_tags,
        entry.as_ref(),
    );
    write_u32(&mut body, 0);
    mapi_response(request_type, request_id, 0, body, None)
}

pub(in crate::mapi) async fn nspi_rowset_response<S>(
    store: &S,
    principal: &AccountPrincipal,
    request: &[u8],
    request_type: &str,
    request_id: &str,
) -> Response
where
    S: ExchangeStore,
{
    let entries = match store.fetch_address_book_entries(principal).await {
        Ok(entries) => entries,
        Err(error) => {
            return mapi_diagnostic_response(
                request_type,
                request_id,
                4,
                &format!("failed to load address book entries: {error}"),
            );
        }
    };
    let available_entry_count = entries.len();
    let lookup_values = scan_address_book_lookup_values(request);
    let explicit_entry_ids = nspi_query_rows_explicit_entry_ids(request_type, request);
    let entries = if explicit_entry_ids.is_empty() {
        nspi_filter_entries_for_request(principal.account_id, entries, request)
    } else {
        nspi_filter_explicit_table_entries(principal.account_id, entries, &explicit_entry_ids)
    };
    if let Err(error) = allocate_nspi_entry_identities(store, principal, &entries).await {
        return mapi_diagnostic_response(
            request_type,
            request_id,
            4,
            &format!("failed to project address book identifiers: {error}"),
        );
    }
    let row_limit = nspi_query_rows_count(request_type, request);
    let entries = if let Some(limit) = row_limit {
        entries.into_iter().take(limit).collect::<Vec<_>>()
    } else {
        entries
    };
    let tags = nspi_requested_property_tags(request);
    log_nspi_rowset_debug(
        principal,
        request,
        request_type,
        available_entry_count,
        &lookup_values,
        &tags,
        &entries,
        row_limit,
    );
    let mut body = Vec::new();
    write_u32(&mut body, 0);
    write_u32(&mut body, 0);
    body.push(0);
    body.push((!entries.is_empty()) as u8);
    if !entries.is_empty() {
        write_large_property_tag_array(&mut body, &tags);
        write_u32(&mut body, entries.len().min(u32::MAX as usize) as u32);
        for entry in &entries {
            body.extend_from_slice(&nspi_resolved_entry_row(
                principal.account_id,
                entry,
                &tags,
                &entries,
            ));
        }
    }
    write_u32(&mut body, 0);
    log_nspi_response_contract(
        principal,
        request_type,
        request_id,
        0,
        &body,
        !entries.is_empty(),
        entries.len(),
        &tags,
        "rowset",
    );
    mapi_response(request_type, request_id, 0, body, None)
}

pub(in crate::mapi) fn nspi_query_rows_count(request_type: &str, request: &[u8]) -> Option<usize> {
    nspi_query_rows_count_details(request_type, request).map(|details| details.count)
}

pub(in crate::mapi) fn nspi_query_rows_explicit_entry_ids(
    request_type: &str,
    request: &[u8],
) -> Vec<u32> {
    let Some(details) = nspi_query_rows_count_details(request_type, request) else {
        return Vec::new();
    };
    (0..details.explicit_table_count)
        .filter_map(|index| {
            let offset = details.table_offset + index * 4;
            let bytes = request.get(offset..offset + 4)?;
            let value = u32::from_le_bytes(bytes.try_into().ok()?);
            nspi_word_looks_like_entry_id(value).then_some(value)
        })
        .collect()
}

struct NspiQueryRowsCountDetails {
    count: usize,
    explicit_table_count: usize,
    table_offset: usize,
    count_offset: usize,
}

fn nspi_query_rows_count_details(
    request_type: &str,
    request: &[u8],
) -> Option<NspiQueryRowsCountDetails> {
    if !nspi_request_type_is_query_rows(request_type) && !nspi_body_looks_like_query_rows(request) {
        return None;
    }
    nspi_query_rows_layout_from_body(request)
}

fn nspi_request_type_is_query_rows(request_type: &str) -> bool {
    request_type
        .trim_matches(|ch: char| ch.is_control() || ch.is_whitespace())
        .eq_ignore_ascii_case("QueryRows")
}

fn nspi_body_looks_like_query_rows(request: &[u8]) -> bool {
    nspi_query_rows_layout_from_body(request).is_some()
}

fn nspi_query_rows_layout_from_body(request: &[u8]) -> Option<NspiQueryRowsCountDetails> {
    const FLAGS_BYTES: usize = 4;
    const STAT_BYTES: usize = 36;
    let documented_offset = FLAGS_BYTES + STAT_BYTES;
    nspi_query_rows_layout_at_offset(request, documented_offset).or_else(|| {
        (FLAGS_BYTES + 32..=FLAGS_BYTES + 44)
            .filter(|offset| *offset != documented_offset)
            .find_map(|offset| nspi_query_rows_layout_at_offset(request, offset))
    })
}

fn nspi_query_rows_layout_at_offset(
    request: &[u8],
    etable_count_offset: usize,
) -> Option<NspiQueryRowsCountDetails> {
    const ETABLE_COUNT_BYTES: usize = 4;
    let etable_count_bytes = request.get(etable_count_offset..etable_count_offset + 4)?;
    let etable_count = u32::from_le_bytes(etable_count_bytes.try_into().ok()?) as usize;
    if etable_count > 1024 {
        return None;
    }
    let etable_bytes = etable_count.checked_mul(4)?;
    let count_offset = etable_count_offset
        .checked_add(ETABLE_COUNT_BYTES)?
        .checked_add(etable_bytes)?;
    let count_bytes = request.get(count_offset..count_offset + 4)?;
    let count = u32::from_le_bytes(count_bytes.try_into().ok()?) as usize;
    if count > 100_000 {
        return None;
    }
    if etable_count > 0 {
        let table_offset = etable_count_offset.checked_add(ETABLE_COUNT_BYTES)?;
        for index in 0..etable_count {
            let offset = table_offset.checked_add(index.checked_mul(4)?)?;
            let bytes = request.get(offset..offset + 4)?;
            let value = u32::from_le_bytes(bytes.try_into().ok()?);
            if !nspi_word_looks_like_entry_id(value) {
                return None;
            }
        }
    }
    Some(NspiQueryRowsCountDetails {
        count,
        explicit_table_count: etable_count,
        table_offset: etable_count_offset + ETABLE_COUNT_BYTES,
        count_offset,
    })
}

pub(in crate::mapi) async fn nspi_matches_response<S>(
    store: &S,
    principal: &AccountPrincipal,
    request: &[u8],
    request_id: &str,
) -> Response
where
    S: ExchangeStore,
{
    let entries = match store.fetch_address_book_entries(principal).await {
        Ok(entries) => entries,
        Err(error) => {
            return mapi_diagnostic_response(
                "GetMatches",
                request_id,
                4,
                &format!("failed to load address book entries: {error}"),
            );
        }
    };
    let available_entry_count = entries.len();
    let lookup_values = scan_address_book_lookup_values(request);
    let entries = nspi_filter_entries_for_request(principal.account_id, entries, request);
    if let Err(error) = allocate_nspi_entry_identities(store, principal, &entries).await {
        return mapi_diagnostic_response(
            "GetMatches",
            request_id,
            4,
            &format!("failed to project address book identifiers: {error}"),
        );
    }
    let tags = nspi_requested_property_tags(request);
    log_nspi_rowset_debug(
        principal,
        request,
        "GetMatches",
        available_entry_count,
        &lookup_values,
        &tags,
        &entries,
        None,
    );
    let mut body = Vec::new();
    write_u32(&mut body, 0);
    write_u32(&mut body, 0);
    body.push(0);
    body.push((!entries.is_empty()) as u8);
    write_u32(&mut body, entries.len().min(u32::MAX as usize) as u32);
    for entry in &entries {
        write_u32(&mut body, nspi_entry_id(principal.account_id, entry));
    }
    body.push((!entries.is_empty()) as u8);
    if !entries.is_empty() {
        write_large_property_tag_array(&mut body, &tags);
        write_u32(&mut body, entries.len().min(u32::MAX as usize) as u32);
        for entry in &entries {
            body.extend_from_slice(&nspi_resolved_entry_row(
                principal.account_id,
                entry,
                &tags,
                &entries,
            ));
        }
    }
    write_u32(&mut body, 0);
    log_nspi_response_contract(
        principal,
        "GetMatches",
        request_id,
        0,
        &body,
        !entries.is_empty(),
        entries.len(),
        &tags,
        "matches",
    );
    mapi_response("GetMatches", request_id, 0, body, None)
}

pub(in crate::mapi) async fn nspi_minimal_ids_response<S>(
    request_type: &str,
    store: &S,
    principal: &AccountPrincipal,
    request_id: &str,
) -> Response
where
    S: ExchangeStore,
{
    if let Err(error) = allocate_principal_nspi_identity(store, principal).await {
        return mapi_diagnostic_response(
            request_type,
            request_id,
            4,
            &format!("failed to project authenticated address book identifier: {error}"),
        );
    }
    let mut body = Vec::new();
    write_u32(&mut body, 0);
    write_u32(&mut body, 0);
    body.push(0);
    body.push(1);
    write_u32(&mut body, 1);
    write_u32(&mut body, principal_minimal_entry_id(principal));
    write_u32(&mut body, 0);
    mapi_response(request_type, request_id, 0, body, None)
}

pub(in crate::mapi) async fn nspi_template_info_response<S>(
    store: &S,
    principal: &AccountPrincipal,
    request_id: &str,
) -> Response
where
    S: ExchangeStore,
{
    if let Err(error) = allocate_principal_nspi_identity(store, principal).await {
        return mapi_diagnostic_response(
            "GetTemplateInfo",
            request_id,
            4,
            &format!("failed to project authenticated address book identifier: {error}"),
        );
    }
    let entry = principal_address_book_entry(principal);
    let mut body = Vec::new();
    write_u32(&mut body, 0);
    write_u32(&mut body, 0);
    write_u32(&mut body, NSPI_UNICODE_CODEPAGE);
    body.push(1);
    body.extend_from_slice(&nspi_entry_property_value_list(
        principal.account_id,
        &entry,
        NSPI_BOOTSTRAP_PROPERTY_TAGS,
        &[],
    ));
    write_u32(&mut body, 0);
    mapi_response("GetTemplateInfo", request_id, 0, body, None)
}

pub(in crate::mapi) fn nspi_update_stat_response(request_id: &str) -> Response {
    let mut body = Vec::new();
    write_u32(&mut body, 0);
    write_u32(&mut body, 0);
    body.extend_from_slice(&[0; 36]);
    write_u32(&mut body, 0);
    write_u32(&mut body, 0);
    mapi_response("UpdateStat", request_id, 0, body, None)
}

fn nspi_entry_instance_key(account_id: Uuid, entry: &ExchangeAddressBookEntry) -> Vec<u8> {
    let mut value = Vec::with_capacity(20);
    value.extend_from_slice(&nspi_entry_id(account_id, entry).to_le_bytes());
    value.extend_from_slice(entry.id.as_bytes());
    value
}

fn nspi_entry_record_key(entry: &ExchangeAddressBookEntry) -> Vec<u8> {
    nspi_entry_permanent_entry_id(entry)
}

pub(in crate::mapi) fn nspi_entry_permanent_entry_id(entry: &ExchangeAddressBookEntry) -> Vec<u8> {
    let legacy_dn = nspi_entry_unprefixed_legacy_dn(entry);
    let mut value = Vec::with_capacity(28 + legacy_dn.len() + 1);
    value.extend_from_slice(&[0, 0, 0, 0]);
    value.extend_from_slice(&NSPI_PERMANENT_ENTRY_ID_PROVIDER_UID);
    value.extend_from_slice(&1u32.to_le_bytes());
    value.extend_from_slice(&nspi_entry_display_type(entry).to_le_bytes());
    value.extend_from_slice(legacy_dn.as_bytes());
    value.push(0);
    value
}

fn nspi_entry_search_key(entry: &ExchangeAddressBookEntry) -> Vec<u8> {
    let legacy_dn = nspi_entry_unprefixed_legacy_dn(entry).to_ascii_uppercase();
    let mut value = format!("EX:{legacy_dn}").into_bytes();
    value.push(0);
    value
}

pub(in crate::mapi) fn nspi_entry_legacy_dn(entry: &ExchangeAddressBookEntry) -> String {
    nspi_entry_legacy_dn_with_prefix(entry, true)
}

pub(in crate::mapi) fn nspi_entry_unprefixed_legacy_dn(entry: &ExchangeAddressBookEntry) -> String {
    nspi_entry_legacy_dn_with_prefix(entry, false)
}

pub(in crate::mapi) fn nspi_entry_legacy_dn_with_prefix(
    entry: &ExchangeAddressBookEntry,
    include_kind_prefix: bool,
) -> String {
    let prefix = match entry.entry_kind {
        ExchangeAddressBookEntryKind::Account => "acct",
        ExchangeAddressBookEntryKind::Contact => "contact",
        ExchangeAddressBookEntryKind::DistributionList => "group",
    };
    let source = match entry.entry_kind {
        ExchangeAddressBookEntryKind::Account if entry.email.trim().is_empty() => {
            entry.id.to_string()
        }
        ExchangeAddressBookEntryKind::Account => entry.email.clone(),
        ExchangeAddressBookEntryKind::Contact if entry.email.trim().is_empty() => {
            entry.id.to_string()
        }
        ExchangeAddressBookEntryKind::Contact => format!("{}-{}", entry.email, entry.id),
        ExchangeAddressBookEntryKind::DistributionList if entry.email.trim().is_empty() => {
            entry.id.to_string()
        }
        ExchangeAddressBookEntryKind::DistributionList => entry.email.clone(),
    };
    let legacy_user = nspi_legacy_cn_from_source(&source);
    let legacy_cn = if include_kind_prefix {
        format!("{prefix}-{legacy_user}")
    } else {
        legacy_user
    };
    nspi_legacy_dn_from_cn(&legacy_cn)
}

fn nspi_legacy_cn_from_source(source: &str) -> String {
    source
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect::<String>()
}

fn nspi_legacy_dn_from_cn(cn: &str) -> String {
    format!("/o=LPE/ou=Exchange Administrative Group/cn=Recipients/cn={cn}")
}

fn nspi_entry_alias(entry: &ExchangeAddressBookEntry) -> String {
    entry
        .email
        .split_once('@')
        .map(|(local_part, _)| local_part)
        .filter(|local_part| !local_part.trim().is_empty())
        .unwrap_or(entry.display_name.as_str())
        .to_string()
}

pub(in crate::mapi) fn nspi_entry_is_principal(
    entry: &ExchangeAddressBookEntry,
    principal: &AccountPrincipal,
) -> bool {
    entry.entry_kind == ExchangeAddressBookEntryKind::Account && entry.id == principal.account_id
}

pub(in crate::mapi) fn nspi_lookup_matches_principal(
    value: &str,
    principal: &AccountPrincipal,
) -> bool {
    let value = normalize_nspi_lookup_value(value);
    let email = principal.email.to_ascii_lowercase();
    value == email
        || principal_legacy_dn_aliases(principal)
            .iter()
            .any(|alias| value == alias.to_ascii_lowercase())
}

pub(in crate::mapi) fn principal_legacy_dn_aliases(principal: &AccountPrincipal) -> Vec<String> {
    let principal_entry = principal_address_book_entry(principal);
    let mut aliases = vec![
        nspi_entry_legacy_dn(&principal_entry),
        nspi_entry_unprefixed_legacy_dn(&principal_entry),
    ];
    push_principal_legacy_dn_alias(&mut aliases, &principal.display_name);
    if let Some((local_part, _)) = principal.email.split_once('@') {
        push_principal_legacy_dn_alias(&mut aliases, local_part);
    }
    aliases.sort_by_key(|alias| alias.to_ascii_lowercase());
    aliases.dedup_by(|left, right| left.eq_ignore_ascii_case(right));
    aliases
}

fn push_principal_legacy_dn_alias(aliases: &mut Vec<String>, source: &str) {
    let source = source.trim();
    if source.is_empty() {
        return;
    }
    let cn = nspi_legacy_cn_from_source(source);
    if cn.is_empty() {
        return;
    }
    aliases.push(nspi_legacy_dn_from_cn(&cn));
}

pub(in crate::mapi) fn nspi_requested_entry<'a>(
    account_id: Uuid,
    request: &[u8],
    entries: &'a [ExchangeAddressBookEntry],
) -> Option<&'a ExchangeAddressBookEntry> {
    let ids = nspi_requested_entry_ids(request);
    ids.iter()
        .find_map(|id| {
            entries
                .iter()
                .find(|entry| nspi_entry_id(account_id, entry) == *id)
        })
        .or_else(|| {
            scan_address_book_lookup_values(request)
                .iter()
                .find_map(|value| nspi_match_entry(account_id, entries, value))
        })
}

pub(in crate::mapi) fn nspi_request_has_entry_selector(request: &[u8]) -> bool {
    nspi_stat_current_rec(request).is_some()
        || nspi_direct_entry_id(request).is_some()
        || !scan_address_book_lookup_values(request).is_empty()
}

pub(in crate::mapi) fn nspi_filter_entries_for_request(
    account_id: Uuid,
    entries: Vec<ExchangeAddressBookEntry>,
    request: &[u8],
) -> Vec<ExchangeAddressBookEntry> {
    let values = resolve_names_requested_values(request);
    if values.is_empty() {
        return entries;
    }
    nspi_ranked_matching_entries(account_id, entries, &values)
}

fn nspi_filter_explicit_table_entries(
    account_id: Uuid,
    entries: Vec<ExchangeAddressBookEntry>,
    requested_entry_ids: &[u32],
) -> Vec<ExchangeAddressBookEntry> {
    requested_entry_ids
        .iter()
        .filter_map(|requested_entry_id| {
            entries
                .iter()
                .find(|entry| nspi_entry_id(account_id, entry) == *requested_entry_id)
                .cloned()
        })
        .collect()
}

pub(in crate::mapi) fn nspi_match_entry<'a>(
    account_id: Uuid,
    entries: &'a [ExchangeAddressBookEntry],
    value: &str,
) -> Option<&'a ExchangeAddressBookEntry> {
    entries
        .iter()
        .filter_map(|entry| {
            Some((
                nspi_entry_match_rank(entry, value)?,
                nspi_entry_kind_rank(entry.entry_kind),
                entry.display_name.to_ascii_lowercase(),
                entry.email.to_ascii_lowercase(),
                nspi_entry_id(account_id, entry),
                entry,
            ))
        })
        .min_by(|left, right| {
            left.0
                .cmp(&right.0)
                .then_with(|| left.1.cmp(&right.1))
                .then_with(|| left.2.cmp(&right.2))
                .then_with(|| left.3.cmp(&right.3))
                .then_with(|| left.4.cmp(&right.4))
        })
        .map(|(_, _, _, _, _, entry)| entry)
}

fn nspi_ranked_matching_entries(
    account_id: Uuid,
    entries: Vec<ExchangeAddressBookEntry>,
    values: &[String],
) -> Vec<ExchangeAddressBookEntry> {
    let mut ranked = entries
        .into_iter()
        .filter_map(|entry| {
            let rank = values
                .iter()
                .filter_map(|value| nspi_entry_match_rank(&entry, value))
                .min()?;
            Some((
                rank,
                nspi_entry_kind_rank(entry.entry_kind),
                entry.display_name.to_ascii_lowercase(),
                entry.email.to_ascii_lowercase(),
                nspi_entry_id(account_id, &entry),
                entry,
            ))
        })
        .collect::<Vec<_>>();
    ranked.sort_by(|left, right| {
        left.0
            .cmp(&right.0)
            .then_with(|| left.1.cmp(&right.1))
            .then_with(|| left.2.cmp(&right.2))
            .then_with(|| left.3.cmp(&right.3))
            .then_with(|| left.4.cmp(&right.4))
    });
    ranked
        .into_iter()
        .map(|(_, _, _, _, _, entry)| entry)
        .collect()
}

fn nspi_entry_kind_rank(entry_kind: ExchangeAddressBookEntryKind) -> u8 {
    match entry_kind {
        ExchangeAddressBookEntryKind::Account => 0,
        ExchangeAddressBookEntryKind::DistributionList => 1,
        ExchangeAddressBookEntryKind::Contact => 2,
    }
}

pub(in crate::mapi) fn nspi_entry_match_rank(
    entry: &ExchangeAddressBookEntry,
    value: &str,
) -> Option<u8> {
    let value = normalize_nspi_lookup_value(value);
    if value.is_empty() {
        return None;
    }
    let email = entry.email.to_ascii_lowercase();
    let display_name = entry.display_name.to_ascii_lowercase();
    let legacy_dn = nspi_entry_legacy_dn(entry).to_ascii_lowercase();
    let unprefixed_legacy_dn = nspi_entry_unprefixed_legacy_dn(entry).to_ascii_lowercase();

    if value == email {
        Some(0)
    } else if value == display_name {
        Some(1)
    } else if value == legacy_dn || value == unprefixed_legacy_dn {
        Some(2)
    } else if email.starts_with(value.as_str()) {
        Some(10)
    } else if display_name.starts_with(value.as_str()) {
        Some(11)
    } else if legacy_dn.starts_with(value.as_str())
        || unprefixed_legacy_dn.starts_with(value.as_str())
    {
        Some(12)
    } else if email.contains(value.as_str()) {
        Some(20)
    } else if display_name.contains(value.as_str()) {
        Some(21)
    } else if legacy_dn.contains(value.as_str()) || unprefixed_legacy_dn.contains(value.as_str()) {
        Some(22)
    } else {
        None
    }
}

pub(in crate::mapi) fn nspi_requested_entry_ids(request: &[u8]) -> Vec<u32> {
    let mut ids = Vec::new();
    if let Some(value) = nspi_stat_current_rec(request) {
        push_unique_nspi_entry_id(&mut ids, value);
    }
    if let Some(value) = nspi_direct_entry_id(request) {
        push_unique_nspi_entry_id(&mut ids, value);
    }
    for value in nspi_query_rows_explicit_entry_ids("", request) {
        push_unique_nspi_entry_id(&mut ids, value);
    }
    ids
}

fn push_unique_nspi_entry_id(ids: &mut Vec<u32>, value: u32) {
    if !ids.contains(&value) {
        ids.push(value);
    }
}

fn nspi_stat_current_rec(request: &[u8]) -> Option<u32> {
    const FLAGS_BYTES: usize = 4;
    const STAT_CURRENT_REC_OFFSET: usize = FLAGS_BYTES + 8;
    if request.len() < STAT_CURRENT_REC_OFFSET + 4 {
        return None;
    }
    let value = u32::from_le_bytes([
        request[STAT_CURRENT_REC_OFFSET],
        request[STAT_CURRENT_REC_OFFSET + 1],
        request[STAT_CURRENT_REC_OFFSET + 2],
        request[STAT_CURRENT_REC_OFFSET + 3],
    ]);
    nspi_word_looks_like_entry_id(value).then_some(value)
}

fn nspi_direct_entry_id(request: &[u8]) -> Option<u32> {
    if request.len() < 8 {
        return None;
    }
    let value = u32::from_le_bytes(request[0..4].try_into().ok()?);
    if !nspi_word_looks_like_entry_id(value) {
        return None;
    }
    request[4..]
        .chunks_exact(4)
        .any(|chunk| nspi_property_tag_is_supported(u32::from_le_bytes(chunk.try_into().unwrap())))
        .then_some(value)
}

fn nspi_word_looks_like_entry_id(value: u32) -> bool {
    matches!(value & 0xc000_0000, 0x4000_0000 | 0x8000_0000)
        && !nspi_word_looks_like_property_tag(value)
}

fn nspi_word_looks_like_property_tag(value: u32) -> bool {
    nspi_property_tag_is_supported(value)
        || matches!(
            value & 0xffff,
            0x0002
                | 0x0003
                | 0x000b
                | 0x000d
                | 0x001e
                | 0x001f
                | 0x0040
                | 0x0048
                | 0x0102
                | 0x1003
                | 0x101e
                | 0x101f
                | 0x1102
        )
}

pub(in crate::mapi) fn scan_address_book_lookup_values(request: &[u8]) -> Vec<String> {
    let mut values = Vec::new();
    values.extend(scan_ascii_lookup_values(request));
    values.extend(scan_utf16_lookup_values(request));
    values.sort();
    values.dedup();
    values
}

pub(in crate::mapi) fn scan_ascii_lookup_values(request: &[u8]) -> Vec<String> {
    request
        .split(|byte| *byte == 0)
        .filter_map(|bytes| {
            if bytes.len() < 3 {
                return None;
            }
            let value = String::from_utf8_lossy(bytes);
            let value = normalize_nspi_lookup_value(&value);
            nspi_lookup_value_is_plausible(&value).then_some(value)
        })
        .collect()
}

pub(in crate::mapi) fn scan_utf16_lookup_values(request: &[u8]) -> Vec<String> {
    let mut values = Vec::new();
    let mut start = 0usize;
    while start + 3 < request.len() {
        if !is_utf16_lookup_start(request, start) {
            start += 1;
            continue;
        }
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
                let value = normalize_nspi_lookup_value(&value);
                if nspi_lookup_value_is_plausible(&value) {
                    values.push(value);
                }
            }
        }
        start += 1;
    }
    values
}

pub(in crate::mapi) fn is_utf16_lookup_start(request: &[u8], start: usize) -> bool {
    if start < 2 {
        return true;
    }
    let previous = u16::from_le_bytes([request[start - 2], request[start - 1]]);
    previous == 0 || previous < 0x20 || previous > 0x7e
}

pub(in crate::mapi) fn decode_utf16le_string(bytes: &[u8]) -> Option<String> {
    if bytes.len() % 2 != 0 {
        return None;
    }
    let units = bytes
        .chunks_exact(2)
        .map(|chunk| u16::from_le_bytes([chunk[0], chunk[1]]))
        .take_while(|unit| *unit != 0)
        .collect::<Vec<_>>();
    String::from_utf16(&units).ok()
}

pub(in crate::mapi) fn normalize_nspi_lookup_value(value: &str) -> String {
    normalization::normalize_smtp_lookup_value(value)
}

fn nspi_lookup_value_is_plausible(value: &str) -> bool {
    if value.is_empty() || value.chars().any(|ch| ch.is_control() || !ch.is_ascii()) {
        return false;
    }
    if value.contains("/cn=") {
        return value.starts_with("/o=") || value.starts_with("o=");
    }
    if !value.contains('@') {
        return false;
    }
    let mut parts = value.split('@');
    let Some(local) = parts.next() else {
        return false;
    };
    let Some(domain) = parts.next() else {
        return false;
    };
    parts.next().is_none()
        && !local.is_empty()
        && domain.contains('.')
        && domain
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '.'))
        && local.chars().all(|ch| {
            ch.is_ascii_alphanumeric()
                || matches!(
                    ch,
                    '.' | '_'
                        | '%'
                        | '+'
                        | '-'
                        | '\''
                        | '='
                        | '!'
                        | '#'
                        | '$'
                        | '&'
                        | '*'
                        | '/'
                        | '?'
                        | '^'
                        | '`'
                        | '{'
                        | '|'
                        | '}'
                        | '~'
                )
        })
}

pub(in crate::mapi) fn public_endpoint_url(headers: &HeaderMap, path: &str) -> String {
    let scheme = headers
        .get("x-forwarded-proto")
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("https");
    let host = headers
        .get("x-forwarded-host")
        .or_else(|| headers.get("host"))
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("localhost");
    format!("{scheme}://{host}{path}")
}

#[cfg(test)]
mod tests;
