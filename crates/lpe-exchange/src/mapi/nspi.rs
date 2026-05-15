use super::properties::write_ascii_z;
use super::rop::*;
use super::session::*;
use super::transport::*;
use super::*;

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
        MapiRequestType::DnToMid => {
            nspi_dn_to_mid_response(store, principal, request, request_id).await
        }
        MapiRequestType::GetMatches => {
            nspi_matches_response(store, principal, request, request_id).await
        }
        MapiRequestType::GetPropList => nspi_property_tags_response("GetPropList", request_id),
        MapiRequestType::GetProps => {
            nspi_props_response(store, principal, request, "GetProps", request_id).await
        }
        MapiRequestType::GetSpecialTable => nspi_special_table_response(request_id),
        MapiRequestType::GetTemplateInfo => {
            nspi_template_info_response(store, principal, request_id).await
        }
        MapiRequestType::ModLinkAtt => nspi_disabled_mutation_response(
            "ModLinkAtt",
            request_id,
            "NSPI link-attribute mutation is disabled; LPE address-book data is projected from canonical accounts and contacts.",
        ),
        MapiRequestType::ModProps => nspi_disabled_mutation_response(
            "ModProps",
            request_id,
            "NSPI property mutation is disabled; LPE address-book data is projected from canonical accounts and contacts.",
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
    let session_id = match reconnect_session(endpoint, principal, headers, "Bind", request_id) {
        Ok(Some(session_id)) => session_id,
        Ok(None) => create_session(endpoint, principal),
        Err(response) => return response,
    };
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
    let mut body = Vec::new();
    write_u32(&mut body, 0);
    write_u32(&mut body, 0);
    write_utf16z(&mut body, &public_endpoint_url(headers, path));
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

pub(in crate::mapi) const NSPI_BOOTSTRAP_PROPERTY_TAGS: &[u32] = &[
    0x3001_001F, // PidTagDisplayName
    0x39FE_001F, // PidTagSmtpAddress
    0x3003_001F, // PidTagEmailAddress
    0x3A00_001F, // PidTagAccount
    0x0FFE_0003, // PidTagObjectType
    0x3000_0003, // PidTagRowId
    0x3004_001F, // PidTagComment
    0x3002_001F, // PidTagAddressType / legacy bootstrap metadata
];

const NSPI_ADDITIONAL_REQUESTED_PROPERTY_TAGS: &[u32] = &[
    0x3001_001E, // PidTagDisplayName string8
    0x39FE_001E, // PidTagSmtpAddress string8
    0x3003_001E, // PidTagEmailAddress string8
    0x3A00_001E, // PidTagAccount string8
    0x3004_001E, // PidTagComment string8
    0x3002_001E, // PidTagAddressType string8
    0x3005_001E, // PidTagAddressBookDisplayNamePrintable / legacy DN string8
    0x3005_001F, // PidTagAddressBookDisplayNamePrintable / legacy DN
    0x3900_0003, // PidTagDisplayType
];

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
        .and_then(|name| nspi_match_entry(&entries, name))
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
        body.extend_from_slice(&nspi_resolved_entry_row(entry, &columns));
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
    parse_resolve_names_values(request).unwrap_or_else(|| scan_address_book_lookup_values(request))
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
    request_id: &str,
) -> Response
where
    S: ExchangeStore,
{
    let entries = match store.fetch_address_book_entries(principal).await {
        Ok(entries) => entries,
        Err(error) => {
            return mapi_diagnostic_response(
                "DNToMId",
                request_id,
                4,
                &format!("failed to load address book entries: {error}"),
            );
        }
    };
    if let Err(error) = allocate_nspi_entry_identities(store, principal, &entries).await {
        return mapi_diagnostic_response(
            "DNToMId",
            request_id,
            4,
            &format!("failed to project address book identifiers: {error}"),
        );
    }
    if let Err(error) = allocate_principal_nspi_identity(store, principal).await {
        return mapi_diagnostic_response(
            "DNToMId",
            request_id,
            4,
            &format!("failed to project authenticated address book identifier: {error}"),
        );
    }
    let values = scan_address_book_lookup_values(request);
    let matched_mid = values
        .first()
        .and_then(|value| nspi_match_entry(&entries, value))
        .map(nspi_entry_id)
        .or_else(|| {
            values
                .iter()
                .any(|value| nspi_lookup_matches_principal(value, principal))
                .then(|| principal_minimal_entry_id(principal))
        })
        .or_else(|| {
            values
                .is_empty()
                .then(|| {
                    entries
                        .iter()
                        .find(|entry| nspi_entry_is_principal(entry, principal))
                })
                .flatten()
                .map(nspi_entry_id)
        });
    let mut body = Vec::new();
    write_u32(&mut body, 0);
    write_u32(&mut body, 0);
    body.push(1);
    write_u32(&mut body, 1);
    write_u32(&mut body, matched_mid.unwrap_or(0));
    write_u32(&mut body, 0);
    mapi_response("DNToMId", request_id, 0, body, None)
}

pub(in crate::mapi) fn nspi_property_tags_response(
    request_type: &str,
    request_id: &str,
) -> Response {
    let mut body = Vec::new();
    write_u32(&mut body, 0);
    write_u32(&mut body, 0);
    body.push(1);
    write_large_property_tag_array(&mut body, NSPI_BOOTSTRAP_PROPERTY_TAGS);
    write_u32(&mut body, 0);
    mapi_response(request_type, request_id, 0, body, None)
}

pub(in crate::mapi) fn nspi_requested_property_tags(request: &[u8]) -> Vec<u32> {
    let mut tags = Vec::new();
    let mut offset = 0usize;
    while offset + 4 <= request.len() {
        let tag = u32::from_le_bytes([
            request[offset],
            request[offset + 1],
            request[offset + 2],
            request[offset + 3],
        ]);
        if nspi_property_tag_is_supported(tag) && !tags.contains(&tag) {
            tags.push(tag);
        }
        offset += 4;
    }
    if tags.is_empty() {
        NSPI_BOOTSTRAP_PROPERTY_TAGS.to_vec()
    } else {
        tags
    }
}

pub(in crate::mapi) fn nspi_property_tag_is_supported(tag: u32) -> bool {
    NSPI_BOOTSTRAP_PROPERTY_TAGS.contains(&tag)
        || NSPI_ADDITIONAL_REQUESTED_PROPERTY_TAGS.contains(&tag)
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
    let tags = nspi_requested_property_tags(request);
    let principal_entry = principal_address_book_entry(principal);
    let principal_id = nspi_entry_id(&principal_entry);
    let entry = nspi_requested_entry(request, &entries)
        .cloned()
        .or_else(|| {
            nspi_requested_entry_ids(request)
                .contains(&principal_id)
                .then_some(principal_entry.clone())
        })
        .or_else(|| {
            scan_address_book_lookup_values(request)
                .iter()
                .any(|value| nspi_lookup_matches_principal(value, principal))
                .then_some(principal_entry.clone())
        })
        .or_else(|| {
            (!nspi_request_has_entry_selector(request)).then(|| {
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
        body.extend_from_slice(&nspi_entry_property_value_list(entry, &tags));
    } else {
        body.push(0);
    }
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
        Ok(entries) => nspi_filter_entries_for_request(entries, request),
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
    let tags = nspi_requested_property_tags(request);
    let mut body = Vec::new();
    write_u32(&mut body, 0);
    write_u32(&mut body, 0);
    body.push(0);
    body.push((!entries.is_empty()) as u8);
    if !entries.is_empty() {
        write_large_property_tag_array(&mut body, &tags);
        write_u32(&mut body, entries.len().min(u32::MAX as usize) as u32);
        for entry in &entries {
            body.extend_from_slice(&nspi_resolved_entry_row(entry, &tags));
        }
    }
    write_u32(&mut body, 0);
    mapi_response(request_type, request_id, 0, body, None)
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
        Ok(entries) => nspi_filter_entries_for_request(entries, request),
        Err(error) => {
            return mapi_diagnostic_response(
                "GetMatches",
                request_id,
                4,
                &format!("failed to load address book entries: {error}"),
            );
        }
    };
    if let Err(error) = allocate_nspi_entry_identities(store, principal, &entries).await {
        return mapi_diagnostic_response(
            "GetMatches",
            request_id,
            4,
            &format!("failed to project address book identifiers: {error}"),
        );
    }
    let tags = nspi_requested_property_tags(request);
    let mut body = Vec::new();
    write_u32(&mut body, 0);
    write_u32(&mut body, 0);
    body.push(0);
    body.push((!entries.is_empty()) as u8);
    write_u32(&mut body, entries.len().min(u32::MAX as usize) as u32);
    for entry in &entries {
        write_u32(&mut body, nspi_entry_id(entry));
    }
    body.push((!entries.is_empty()) as u8);
    if !entries.is_empty() {
        write_large_property_tag_array(&mut body, &tags);
        write_u32(&mut body, entries.len().min(u32::MAX as usize) as u32);
        for entry in &entries {
            body.extend_from_slice(&nspi_resolved_entry_row(entry, &tags));
        }
    }
    write_u32(&mut body, 0);
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

pub(in crate::mapi) fn nspi_special_table_response(request_id: &str) -> Response {
    let mut table_row = Vec::new();
    write_u32(&mut table_row, 4);
    write_address_book_tagged_property_value(
        &mut table_row,
        0x3001_001F,
        &NspiValue::String("Global Address List"),
    );
    write_address_book_tagged_property_value(&mut table_row, 0x0FFE_0003, &NspiValue::U32(2));
    write_address_book_tagged_property_value(&mut table_row, 0x3000_0003, &NspiValue::U32(1));
    write_address_book_tagged_property_value(
        &mut table_row,
        0x3002_001F,
        &NspiValue::String(
            "/o=LPE/ou=Exchange Administrative Group/cn=Configuration/cn=Address Lists/cn=Global Address List",
        ),
    );

    let mut body = Vec::new();
    write_u32(&mut body, 0);
    write_u32(&mut body, 0);
    write_u32(&mut body, NSPI_UNICODE_CODEPAGE);
    body.push(1);
    write_u32(&mut body, 1);
    body.push(1);
    write_u32(&mut body, 1);
    body.extend_from_slice(&table_row);
    write_u32(&mut body, 0);
    mapi_response("GetSpecialTable", request_id, 0, body, None)
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
        &entry,
        NSPI_BOOTSTRAP_PROPERTY_TAGS,
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

pub(in crate::mapi) fn nspi_resolved_entry_row(
    entry: &ExchangeAddressBookEntry,
    columns: &[u32],
) -> Vec<u8> {
    let mut row = Vec::new();
    row.push(0);
    for property_tag in columns {
        write_address_book_property_value(
            &mut row,
            *property_tag,
            &nspi_entry_value(entry, *property_tag),
        );
    }
    row
}

pub(in crate::mapi) fn nspi_entry_property_value_list(
    entry: &ExchangeAddressBookEntry,
    tags: &[u32],
) -> Vec<u8> {
    let mut values = Vec::new();
    write_u32(&mut values, tags.len() as u32);
    for property_tag in tags {
        write_address_book_tagged_property_value(
            &mut values,
            *property_tag,
            &nspi_entry_value(entry, *property_tag),
        );
    }
    values
}

pub(in crate::mapi) enum NspiValue<'a> {
    String(&'a str),
    OwnedString(String),
    U32(u32),
}

pub(in crate::mapi) fn nspi_entry_value(
    entry: &ExchangeAddressBookEntry,
    property_tag: u32,
) -> NspiValue<'_> {
    match property_tag {
        0x3001_001F | 0x3001_001E => NspiValue::String(&entry.display_name),
        0x39FE_001F | 0x39FE_001E => NspiValue::String(&entry.email),
        0x3003_001F | 0x3003_001E => NspiValue::String(&entry.email),
        0x3A00_001F | 0x3A00_001E => NspiValue::String(&entry.display_name),
        0x0FFE_0003 => NspiValue::U32(MAPI_MAILUSER_OBJECT_TYPE),
        0x3900_0003 => NspiValue::U32(nspi_entry_display_type(entry)),
        0x3000_0003 => NspiValue::U32(nspi_entry_id(entry)),
        0x3004_001F | 0x3004_001E => NspiValue::String(&entry.email),
        0x3002_001F | 0x3002_001E => NspiValue::String("SMTP"),
        0x3005_001F | 0x3005_001E => NspiValue::OwnedString(nspi_entry_legacy_dn(entry)),
        _ => match property_tag & 0xFFFF {
            0x001F | 0x001E => NspiValue::String(""),
            0x0003 => NspiValue::U32(0),
            _ => NspiValue::U32(0),
        },
    }
}

pub(in crate::mapi) async fn allocate_nspi_entry_identities<S>(
    store: &S,
    principal: &AccountPrincipal,
    entries: &[ExchangeAddressBookEntry],
) -> Result<()>
where
    S: ExchangeStore,
{
    let requests = entries
        .iter()
        .map(nspi_identity_request)
        .collect::<Vec<_>>();
    remember_nspi_identity_records(store, principal, &requests).await
}

pub(in crate::mapi) async fn allocate_principal_nspi_identity<S>(
    store: &S,
    principal: &AccountPrincipal,
) -> Result<()>
where
    S: ExchangeStore,
{
    let entry = principal_address_book_entry(principal);
    let request = nspi_identity_request(&entry);
    remember_nspi_identity_records(store, principal, &[request]).await
}

async fn remember_nspi_identity_records<S>(
    store: &S,
    principal: &AccountPrincipal,
    requests: &[MapiIdentityRequest],
) -> Result<()>
where
    S: ExchangeStore,
{
    if requests.is_empty() {
        return Ok(());
    }
    for record in store
        .fetch_or_allocate_mapi_identities(principal.account_id, requests)
        .await?
    {
        identity::remember_mapi_identity(record.canonical_id, record.object_id);
    }
    Ok(())
}

fn nspi_identity_request(entry: &ExchangeAddressBookEntry) -> MapiIdentityRequest {
    MapiIdentityRequest {
        object_kind: match entry.entry_kind {
            ExchangeAddressBookEntryKind::Account => MapiIdentityObjectKind::Account,
            ExchangeAddressBookEntryKind::Contact => MapiIdentityObjectKind::Contact,
        },
        canonical_id: entry.id,
        reserved_global_counter: None,
    }
}

pub(in crate::mapi) fn nspi_entry_id(entry: &ExchangeAddressBookEntry) -> u32 {
    identity::mapped_mapi_object_id(&entry.id)
        .and_then(|object_id| nspi_minimal_id_from_object_id(object_id, entry.entry_kind))
        .unwrap_or_else(|| legacy_nspi_entry_id(entry))
}

pub(in crate::mapi) fn nspi_minimal_id_from_object_id(
    object_id: u64,
    entry_kind: ExchangeAddressBookEntryKind,
) -> Option<u32> {
    let counter = identity::global_counter_from_store_id(object_id)? as u32;
    let value = (counter & 0x3FFF_FFFF)
        | match entry_kind {
            ExchangeAddressBookEntryKind::Account => 0x8000_0000,
            ExchangeAddressBookEntryKind::Contact => 0x4000_0000,
        };
    (value >= 2).then_some(value)
}

fn legacy_nspi_entry_id(entry: &ExchangeAddressBookEntry) -> u32 {
    let bytes = entry.id.as_bytes();
    let value = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
    match entry.entry_kind {
        ExchangeAddressBookEntryKind::Account => value | 0x8000_0000,
        ExchangeAddressBookEntryKind::Contact => value | 0x4000_0000,
    }
    .max(2)
}

pub(in crate::mapi) fn principal_minimal_entry_id(principal: &AccountPrincipal) -> u32 {
    nspi_entry_id(&principal_address_book_entry(principal))
}

pub(in crate::mapi) fn principal_address_book_entry(
    principal: &AccountPrincipal,
) -> ExchangeAddressBookEntry {
    ExchangeAddressBookEntry {
        id: principal.account_id,
        display_name: principal.display_name.clone(),
        email: principal.email.clone(),
        entry_kind: ExchangeAddressBookEntryKind::Account,
        directory_kind: ExchangeAddressBookDirectoryKind::Person,
    }
}

pub(in crate::mapi) fn nspi_entry_display_type(entry: &ExchangeAddressBookEntry) -> u32 {
    match (entry.entry_kind, entry.directory_kind) {
        (ExchangeAddressBookEntryKind::Contact, _) => 6,
        (ExchangeAddressBookEntryKind::Account, ExchangeAddressBookDirectoryKind::Room) => 7,
        (ExchangeAddressBookEntryKind::Account, ExchangeAddressBookDirectoryKind::Equipment) => 8,
        (ExchangeAddressBookEntryKind::Account, ExchangeAddressBookDirectoryKind::Person) => 0,
    }
}

pub(in crate::mapi) fn write_large_property_tag_array(body: &mut Vec<u8>, tags: &[u32]) {
    write_u32(body, tags.len() as u32);
    for tag in tags {
        write_u32(body, *tag);
    }
}

pub(in crate::mapi) fn write_address_book_tagged_property_value(
    body: &mut Vec<u8>,
    property_tag: u32,
    value: &NspiValue<'_>,
) {
    write_u32(body, property_tag);
    write_address_book_property_value(body, property_tag, value);
}

pub(in crate::mapi) fn write_address_book_property_value(
    body: &mut Vec<u8>,
    property_tag: u32,
    value: &NspiValue<'_>,
) {
    match (property_tag & 0xFFFF, value) {
        (0x001E, NspiValue::String(value)) => {
            body.push(0xFF);
            write_ascii_z(body, value);
        }
        (0x001E, NspiValue::OwnedString(value)) => {
            body.push(0xFF);
            write_ascii_z(body, value);
        }
        (0x001F, NspiValue::String(value)) => {
            body.push(0xFF);
            write_utf16z(body, value);
        }
        (0x001F, NspiValue::OwnedString(value)) => {
            body.push(0xFF);
            write_utf16z(body, value);
        }
        (0x0003, NspiValue::U32(value)) => write_u32(body, *value),
        (0x0003, _) => write_u32(body, 0),
        (_, NspiValue::U32(value)) => write_u32(body, *value),
        (_, NspiValue::String(value)) => {
            body.push(0xFF);
            write_utf16z(body, value);
        }
        (_, NspiValue::OwnedString(value)) => {
            body.push(0xFF);
            write_utf16z(body, value);
        }
    }
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
    let legacy_cn = if include_kind_prefix {
        format!("{prefix}-{legacy_user}")
    } else {
        legacy_user
    };
    format!("/o=LPE/ou=Exchange Administrative Group/cn=Recipients/cn={legacy_cn}")
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
    let display_name = principal.display_name.to_ascii_lowercase();
    let principal_entry = principal_address_book_entry(principal);
    value == email
        || value == display_name
        || email.contains(value.as_str())
        || nspi_entry_matches(&principal_entry, &value)
}

pub(in crate::mapi) fn nspi_requested_entry<'a>(
    request: &[u8],
    entries: &'a [ExchangeAddressBookEntry],
) -> Option<&'a ExchangeAddressBookEntry> {
    let ids = nspi_requested_entry_ids(request);
    ids.iter()
        .find_map(|id| entries.iter().find(|entry| nspi_entry_id(entry) == *id))
        .or_else(|| {
            scan_address_book_lookup_values(request)
                .iter()
                .find_map(|value| nspi_match_entry(entries, value))
        })
}

pub(in crate::mapi) fn nspi_request_has_entry_selector(request: &[u8]) -> bool {
    !nspi_requested_entry_ids(request).is_empty()
        || !scan_address_book_lookup_values(request).is_empty()
}

pub(in crate::mapi) fn nspi_filter_entries_for_request(
    entries: Vec<ExchangeAddressBookEntry>,
    request: &[u8],
) -> Vec<ExchangeAddressBookEntry> {
    let values = scan_address_book_lookup_values(request);
    if values.is_empty() {
        return entries;
    }
    entries
        .into_iter()
        .filter(|entry| values.iter().any(|value| nspi_entry_matches(entry, value)))
        .collect()
}

pub(in crate::mapi) fn nspi_match_entry<'a>(
    entries: &'a [ExchangeAddressBookEntry],
    value: &str,
) -> Option<&'a ExchangeAddressBookEntry> {
    entries
        .iter()
        .filter_map(|entry| {
            Some((
                nspi_entry_match_rank(entry, value)?,
                match entry.entry_kind {
                    ExchangeAddressBookEntryKind::Account => 0u8,
                    ExchangeAddressBookEntryKind::Contact => 1u8,
                },
                entry.display_name.to_ascii_lowercase(),
                entry.email.to_ascii_lowercase(),
                nspi_entry_id(entry),
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

pub(in crate::mapi) fn nspi_entry_matches(entry: &ExchangeAddressBookEntry, value: &str) -> bool {
    nspi_entry_match_rank(entry, value).is_some()
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
    let mut offset = 0usize;
    while offset + 4 <= request.len() {
        let value = u32::from_le_bytes([
            request[offset],
            request[offset + 1],
            request[offset + 2],
            request[offset + 3],
        ]);
        if value >= 2 && !nspi_property_tag_is_supported(value) && !ids.contains(&value) {
            ids.push(value);
        }
        offset += 4;
    }
    ids
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
            (!value.is_empty() && (value.contains('@') || value.contains("/cn="))).then_some(value)
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
                if !value.is_empty() && (value.contains('@') || value.contains("/cn=")) {
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
    let mut value = value.trim().trim_matches('\0').to_ascii_lowercase();
    if let Some(rest) = value.strip_prefix("=smtp:") {
        value = rest.to_string();
    } else if let Some(rest) = value.strip_prefix("smtp:") {
        value = rest.to_string();
    }
    value
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
