use super::rop::*;
use super::session::*;
use super::transport::*;
use super::*;

pub(in crate::mapi) fn bind_response(
    endpoint: MapiEndpoint,
    principal: &AccountPrincipal,
    headers: &HeaderMap,
    request_id: &str,
) -> Response {
    let session_id = reconnect_session(endpoint, principal, headers)
        .unwrap_or_else(|| create_session(endpoint, principal));
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
    let entries = match store.fetch_address_book_entries(principal.account_id).await {
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
    let entries = match store.fetch_address_book_entries(principal.account_id).await {
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
    let entries = match store.fetch_address_book_entries(principal.account_id).await {
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
    let entry = nspi_requested_entry(request, &entries).or_else(|| {
        entries
            .iter()
            .find(|entry| nspi_entry_is_principal(entry, principal))
    });
    let mut body = Vec::new();
    write_u32(&mut body, 0);
    write_u32(&mut body, 0);
    write_u32(&mut body, NSPI_UNICODE_CODEPAGE);
    if let Some(entry) = entry {
        body.push(1);
        body.extend_from_slice(&nspi_entry_property_value_list(
            entry,
            NSPI_BOOTSTRAP_PROPERTY_TAGS,
        ));
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
    let entries = match store.fetch_address_book_entries(principal.account_id).await {
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
    let mut body = Vec::new();
    write_u32(&mut body, 0);
    write_u32(&mut body, 0);
    body.push(0);
    body.push((!entries.is_empty()) as u8);
    if !entries.is_empty() {
        write_large_property_tag_array(&mut body, NSPI_BOOTSTRAP_PROPERTY_TAGS);
        write_u32(&mut body, entries.len().min(u32::MAX as usize) as u32);
        for entry in &entries {
            body.extend_from_slice(&nspi_resolved_entry_row(
                entry,
                NSPI_BOOTSTRAP_PROPERTY_TAGS,
            ));
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
    let entries = match store.fetch_address_book_entries(principal.account_id).await {
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
        write_large_property_tag_array(&mut body, NSPI_BOOTSTRAP_PROPERTY_TAGS);
        write_u32(&mut body, entries.len().min(u32::MAX as usize) as u32);
        for entry in &entries {
            body.extend_from_slice(&nspi_resolved_entry_row(
                entry,
                NSPI_BOOTSTRAP_PROPERTY_TAGS,
            ));
        }
    }
    write_u32(&mut body, 0);
    mapi_response("GetMatches", request_id, 0, body, None)
}

pub(in crate::mapi) fn nspi_minimal_ids_response(
    request_type: &str,
    principal: &AccountPrincipal,
    request_id: &str,
) -> Response {
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

pub(in crate::mapi) fn nspi_template_info_response(
    principal: &AccountPrincipal,
    request_id: &str,
) -> Response {
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
        0x3001_001F => NspiValue::String(&entry.display_name),
        0x39FE_001F => NspiValue::String(&entry.email),
        0x3003_001F => NspiValue::String(&entry.email),
        0x3A00_001F => NspiValue::String(&entry.display_name),
        0x0FFE_0003 => NspiValue::U32(MAPI_MAILUSER_OBJECT_TYPE),
        0x3900_0003 => NspiValue::U32(nspi_entry_display_type(entry)),
        0x3000_0003 => NspiValue::U32(nspi_entry_id(entry)),
        0x3004_001F => NspiValue::String(&entry.email),
        0x3002_001F => NspiValue::String("SMTP"),
        0x3005_001F => NspiValue::OwnedString(nspi_entry_legacy_dn(entry)),
        _ => match property_tag & 0xFFFF {
            0x001F => NspiValue::String(""),
            0x0003 => NspiValue::U32(0),
            _ => NspiValue::U32(0),
        },
    }
}

pub(in crate::mapi) fn nspi_entry_id(entry: &ExchangeAddressBookEntry) -> u32 {
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
        .find(|entry| nspi_entry_matches(entry, value) && nspi_entry_exact_match(entry, value))
        .or_else(|| {
            entries
                .iter()
                .find(|entry| nspi_entry_matches(entry, value))
        })
}

pub(in crate::mapi) fn nspi_entry_exact_match(
    entry: &ExchangeAddressBookEntry,
    value: &str,
) -> bool {
    let value = normalize_nspi_lookup_value(value);
    let legacy_dn = nspi_entry_legacy_dn(entry).to_ascii_lowercase();
    let unprefixed_legacy_dn = nspi_entry_unprefixed_legacy_dn(entry).to_ascii_lowercase();
    value == entry.email.to_ascii_lowercase()
        || value == entry.display_name.to_ascii_lowercase()
        || value == legacy_dn
        || value == unprefixed_legacy_dn
        || value == format!("smtp:{}", entry.email.to_ascii_lowercase())
        || value == format!("=smtp:{}", entry.email.to_ascii_lowercase())
}

pub(in crate::mapi) fn nspi_entry_matches(entry: &ExchangeAddressBookEntry, value: &str) -> bool {
    let value = normalize_nspi_lookup_value(value);
    if value.is_empty() {
        return false;
    }
    nspi_entry_exact_match(entry, &value)
        || entry
            .display_name
            .to_ascii_lowercase()
            .contains(value.as_str())
        || entry.email.to_ascii_lowercase().contains(value.as_str())
        || nspi_entry_legacy_dn(entry)
            .to_ascii_lowercase()
            .contains(value.as_str())
        || nspi_entry_unprefixed_legacy_dn(entry)
            .to_ascii_lowercase()
            .contains(value.as_str())
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
        if value >= 2 && !ids.contains(&value) {
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
