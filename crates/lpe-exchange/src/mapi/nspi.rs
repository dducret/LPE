use super::properties::{
    write_ascii_z, write_multi_string, write_multi_string8, NSPI_PERMANENT_ENTRY_ID_PROVIDER_UID,
};
use super::rop::*;
use super::session::*;
use super::transport::*;
use super::wire::MapiHttpRequestType as MapiRequestType;
use super::*;
use std::collections::{BTreeMap, HashMap};
use std::sync::{Mutex, OnceLock};

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

const NSPI_GAL_CONTAINER_DN: &str = "/guid=741f6fd3-8e1a-654f-9d42-2dfb451c8f10";
const PID_TAG_ENTRY_ID: u32 = 0x0FFF_0102;
const PID_TAG_CONTAINER_FLAGS: u32 = 0x3600_0003;
const PID_TAG_DEPTH: u32 = 0x3005_0003;
const PID_TAG_ADDRESS_BOOK_CONTAINER_ID: u32 = 0xFFFD_0003;
const PID_TAG_ADDRESS_BOOK_IS_MASTER: u32 = 0xFFFB_000B;
const AB_RECIPIENTS: u32 = 0x0000_0001;
const AB_UNMODIFIABLE: u32 = 0x0000_0008;
const DT_CONTAINER: u32 = 0x0000_0100;
const NSPI_ADDRESS_CREATION_TEMPLATES_FLAG: u32 = 0x0000_0002;
const NSPI_UNICODE_STRINGS_FLAG: u32 = 0x0000_0004;

const NSPI_ADDITIONAL_REQUESTED_PROPERTY_TAGS: &[u32] = &[
    0x0FFF_0102, // PidTagEntryId
    0x300B_0102, // PidTagSearchKey
    0x0FF8_0102, // PidTagMappingSignature
    0x3902_0102, // PidTagTemplateid
    0x39FF_001E, // PidTag7BitDisplayName string8
    0x39FF_001F, // PidTag7BitDisplayName
    0x3001_001E, // PidTagDisplayName string8
    0x39FE_001E, // PidTagSmtpAddress string8
    0x3003_001E, // PidTagEmailAddress string8
    0x3A00_001E, // PidTagAccount string8
    0x3004_001E, // PidTagComment string8
    0x3002_001E, // PidTagAddressType string8
    0x3005_001E, // PidTagAddressBookDisplayNamePrintable / legacy DN string8
    0x3005_001F, // PidTagAddressBookDisplayNamePrintable / legacy DN
    0x3A20_001E, // PidTagTransmittableDisplayName string8
    0x3A20_001F, // PidTagTransmittableDisplayName
    0x3F08_0003, // PidTagInitialDetailsPane
    0x3900_0003, // PidTagDisplayType
    0x803C_001E, // PidTagAddressBookObjectDistinguishedName string8
    0x803C_001F, // PidTagAddressBookObjectDistinguishedName
    0x800F_101E, // PidTagAddressBookProxyAddresses string8
    0x800F_101F, // PidTagAddressBookProxyAddresses
    0x8009_000D, // PidTagAddressBookMember
    0x8CA8_001E, // Outlook address book string8 compatibility column
    0x8CE2_0003, // PidTagAddressBookDistributionListMemberCount
    0x8CE3_0003, // PidTagAddressBookDistributionListExternalMemberCount
    0x8C6D_0102, // PidTagAddressBookObjectGuid
    0xFFFD_0003, // PidTagAddressBookContainerId
];

#[allow(dead_code)]
const NSPI_SUPPORTED_REQUEST_TYPES: &[MapiRequestType] = &[
    MapiRequestType::Bind,
    MapiRequestType::Unbind,
    MapiRequestType::CompareMids,
    MapiRequestType::DnToMid,
    MapiRequestType::GetMatches,
    MapiRequestType::GetPropList,
    MapiRequestType::GetProps,
    MapiRequestType::GetSpecialTable,
    MapiRequestType::GetTemplateInfo,
    MapiRequestType::ModLinkAtt,
    MapiRequestType::ModProps,
    MapiRequestType::GetAddressBookUrl,
    MapiRequestType::GetMailboxUrl,
    MapiRequestType::QueryColumns,
    MapiRequestType::QueryRows,
    MapiRequestType::ResolveNames,
    MapiRequestType::ResortRestriction,
    MapiRequestType::SeekEntries,
    MapiRequestType::UpdateStat,
];

const NSPI_KNOWN_UNSUPPORTED_PROPERTY_TAGS: &[(u32, &str)] = &[
    (0x3A06_001E, "PidTagGivenName"),
    (0x3A06_001F, "PidTagGivenName"),
    (0x3A08_001E, "PidTagBusinessTelephoneNumber"),
    (0x3A08_001F, "PidTagBusinessTelephoneNumber"),
    (0x3A09_001E, "PidTagHomeTelephoneNumber"),
    (0x3A09_001F, "PidTagHomeTelephoneNumber"),
    (0x3A0B_001E, "PidTagSurname"),
    (0x3A0B_001F, "PidTagSurname"),
    (0x3A15_001E, "PidTagPostalAddress"),
    (0x3A15_001F, "PidTagPostalAddress"),
    (0x3A16_001E, "PidTagCompanyName"),
    (0x3A16_001F, "PidTagCompanyName"),
    (0x3A17_001E, "PidTagTitle"),
    (0x3A17_001F, "PidTagTitle"),
    (0x3A18_001E, "PidTagDepartmentName"),
    (0x3A18_001F, "PidTagDepartmentName"),
    (0x3A19_001E, "PidTagOfficeLocation"),
    (0x3A19_001F, "PidTagOfficeLocation"),
    (0x3A1A_001E, "PidTagPrimaryTelephoneNumber"),
    (0x3A1A_001F, "PidTagPrimaryTelephoneNumber"),
    (0x3A1B_001F, "PidTagBusiness2TelephoneNumbers"),
    (0x3A1B_101F, "PidTagBusiness2TelephoneNumbers"),
    (0x3A1C_001E, "PidTagMobileTelephoneNumber"),
    (0x3A1C_001F, "PidTagMobileTelephoneNumber"),
    (0x3A26_001E, "PidTagCountry"),
    (0x3A26_001F, "PidTagCountry"),
    (0x3A27_001E, "PidTagLocality"),
    (0x3A27_001F, "PidTagLocality"),
    (0x3A28_001E, "PidTagStateOrProvince"),
    (0x3A28_001F, "PidTagStateOrProvince"),
    (0x3A29_001E, "PidTagStreetAddress"),
    (0x3A29_001F, "PidTagStreetAddress"),
    (0x3A2A_001E, "PidTagPostalCode"),
    (0x3A2A_001F, "PidTagPostalCode"),
    (0x3A4F_001E, "PidTagNickname"),
    (0x3A4F_001F, "PidTagNickname"),
    (0x3A71_001F, "PidTagSendRichInfo"),
    (0x3A8C_001E, "PidTagAddressBookPhoneticDisplayName"),
    (0x3A8C_001F, "PidTagAddressBookPhoneticDisplayName"),
    (0x3A8D_001E, "PidTagAddressBookPhoneticGivenName"),
    (0x3A8D_001F, "PidTagAddressBookPhoneticGivenName"),
    (0x3A8E_001E, "PidTagAddressBookPhoneticSurname"),
    (0x3A8E_001F, "PidTagAddressBookPhoneticSurname"),
    (0x3A8F_001E, "PidTagAddressBookPhoneticCompanyName"),
    (0x3A8F_001F, "PidTagAddressBookPhoneticCompanyName"),
    (0x3A4E_001E, "PidTagManagerName"),
    (0x3A4E_001F, "PidTagManagerName"),
    (0x3A73_001E, "PidTagHomeAddressStreet"),
    (0x3A73_001F, "PidTagHomeAddressStreet"),
    (0x3A74_001E, "PidTagHomeAddressCity"),
    (0x3A74_001F, "PidTagHomeAddressCity"),
    (0x3A75_001E, "PidTagHomeAddressStateOrProvince"),
    (0x3A75_001F, "PidTagHomeAddressStateOrProvince"),
    (0x3A76_001E, "PidTagHomeAddressPostalCode"),
    (0x3A76_001F, "PidTagHomeAddressPostalCode"),
    (0x3A77_001E, "PidTagHomeAddressCountry"),
    (0x3A77_001F, "PidTagHomeAddressCountry"),
];

#[allow(dead_code)]
pub(in crate::mapi) fn nspi_known_unsupported_property_tag_name(tag: u32) -> Option<&'static str> {
    NSPI_KNOWN_UNSUPPORTED_PROPERTY_TAGS
        .iter()
        .find_map(|(known_tag, name)| (*known_tag == tag).then_some(*name))
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
    let values = resolve_names_requested_values(request);
    let matched = nspi_dn_to_mid_match(principal, &entries, &values);
    log_nspi_dn_to_mid_debug(principal, request_id, request, &values, &matched);
    let mut body = Vec::new();
    write_u32(&mut body, 0);
    write_u32(&mut body, 0);
    body.push(1);
    write_u32(&mut body, 1);
    write_u32(&mut body, matched.mid.unwrap_or(0));
    write_u32(&mut body, 0);
    mapi_response("DNToMId", request_id, 0, body, None)
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

fn log_nspi_dn_to_mid_debug(
    principal: &AccountPrincipal,
    request_id: &str,
    request: &[u8],
    values: &[String],
    matched: &NspiDnToMidMatch,
) {
    tracing::info!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = "nspi",
        tenant_id = %principal.tenant_id,
        account_id = %principal.account_id,
        mailbox = %principal.email,
        request_type = "DNToMId",
        mapi_request_id = request_id,
        request_body_bytes = request.len(),
        requested_value_count = values.len(),
        requested_values = %format_nspi_lookup_values_for_debug(values),
        principal_aliases = %format_nspi_lookup_values_for_debug(&principal_legacy_dn_aliases(principal)),
        matched_mid = %matched.mid.map(|mid| format!("{mid:#010x}")).unwrap_or_default(),
        match_source = matched.source,
        message = "rca debug nspi dn to mid"
    );
}

fn format_nspi_lookup_values_for_debug(values: &[String]) -> String {
    values
        .iter()
        .take(12)
        .map(|value| value.chars().take(180).collect::<String>())
        .collect::<Vec<_>>()
        .join("|")
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

fn nspi_get_props_property_tags(request: &[u8]) -> Vec<u32> {
    let tags = nspi_requested_property_tags(request);
    if tags != NSPI_BOOTSTRAP_PROPERTY_TAGS {
        return tags;
    }
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
        offset += 1;
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

fn log_nspi_get_props_debug(
    principal: &AccountPrincipal,
    request: &[u8],
    request_type: &str,
    raw_tag_candidates: &[u32],
    tags: &[u32],
    dropped_tags: &[u32],
    entry: Option<&ExchangeAddressBookEntry>,
) {
    let entry_id = entry
        .map(|entry| nspi_entry_id(principal.account_id, entry))
        .map(|id| format!("{id:#010x}"))
        .unwrap_or_default();
    let entry_kind = entry
        .map(|entry| match entry.entry_kind {
            ExchangeAddressBookEntryKind::Account => "account",
            ExchangeAddressBookEntryKind::Contact => "contact",
            ExchangeAddressBookEntryKind::DistributionList => "distribution_list",
        })
        .unwrap_or("");
    let entry_email = entry.map(|entry| entry.email.as_str()).unwrap_or("");
    let entry_display_name = entry.map(|entry| entry.display_name.as_str()).unwrap_or("");
    let requested_entry_ids = nspi_requested_entry_ids(request);
    let current_rec = nspi_stat_current_rec(request)
        .map(|value| format!("{value:#010x}"))
        .unwrap_or_default();
    let returned_property_tags = if entry.is_some() {
        format_nspi_property_tags_for_debug(tags)
    } else {
        String::new()
    };
    let message = "rca debug mapi nspi get props";
    tracing::info!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = "nspi",
        mailbox = %principal.email,
        request_type = request_type,
        request_body_bytes = request.len(),
        current_rec = %current_rec,
        requested_entry_ids = %format_nspi_u32_values_for_debug(&requested_entry_ids),
        entry_present = entry.is_some(),
        entry_id = %entry_id,
        entry_kind = entry_kind,
        entry_email = entry_email,
        entry_display_name = entry_display_name,
        requested_property_tag_candidate_count = raw_tag_candidates.len(),
        requested_property_tag_candidates = %format_nspi_property_tags_for_debug(raw_tag_candidates),
        effective_property_tag_count = tags.len(),
        effective_property_tags = %format_nspi_property_tags_for_debug(tags),
        returned_property_tag_count = if entry.is_some() { tags.len() } else { 0 },
        returned_property_tags = %returned_property_tags,
        dropped_property_tag_count = dropped_tags.len(),
        dropped_property_tags = %format_nspi_property_tags_for_debug(dropped_tags),
        dropped_known_unsupported_property_tags = %format_nspi_known_unsupported_property_tags_for_debug(dropped_tags),
        message = message,
    );
}

fn format_nspi_known_unsupported_property_tags_for_debug(tags: &[u32]) -> String {
    tags.iter()
        .filter_map(|tag| nspi_known_unsupported_property_tag_name(*tag).map(|name| (*tag, name)))
        .map(|(tag, name)| format!("{tag:#010x}:{name}"))
        .collect::<Vec<_>>()
        .join(",")
}

fn nspi_raw_property_tag_candidates(request: &[u8]) -> Vec<u32> {
    let mut tags = Vec::new();
    let mut offset = 0usize;
    while offset + 4 <= request.len() {
        let tag = u32::from_le_bytes([
            request[offset],
            request[offset + 1],
            request[offset + 2],
            request[offset + 3],
        ]);
        if nspi_word_looks_like_requested_property_tag(tag) && !tags.contains(&tag) {
            tags.push(tag);
        }
        offset += 1;
    }
    tags
}

fn nspi_word_looks_like_requested_property_tag(tag: u32) -> bool {
    let property_id = tag >> 16;
    let property_type = tag & 0xffff;
    property_id != 0
        && matches!(
            property_type,
            0x0002
                | 0x0003
                | 0x0005
                | 0x000A
                | 0x000B
                | 0x0014
                | 0x001E
                | 0x001F
                | 0x0040
                | 0x0048
                | 0x0102
                | 0x1002
                | 0x1003
                | 0x1014
                | 0x101E
                | 0x101F
                | 0x1048
                | 0x1102
        )
}

fn format_nspi_u32_values_for_debug(values: &[u32]) -> String {
    values
        .iter()
        .map(|value| format!("{value:#010x}"))
        .collect::<Vec<_>>()
        .join(",")
}

fn format_nspi_property_tags_for_debug(tags: &[u32]) -> String {
    tags.iter()
        .map(|tag| format!("{tag:#010x}"))
        .collect::<Vec<_>>()
        .join(",")
}

fn log_nspi_rowset_debug(
    principal: &AccountPrincipal,
    request: &[u8],
    request_type: &str,
    available_entry_count: usize,
    lookup_values: &[String],
    tags: &[u32],
    entries: &[ExchangeAddressBookEntry],
    row_limit: Option<usize>,
) {
    let requested_entry_ids = nspi_requested_entry_ids(request);
    let current_rec = nspi_stat_current_rec(request)
        .map(|value| format!("{value:#010x}"))
        .unwrap_or_default();
    let row_limit = row_limit.map(|limit| limit.to_string()).unwrap_or_default();
    let query_rows_count = nspi_query_rows_count_details(request_type, request);
    let query_rows_explicit_entry_ids = nspi_query_rows_explicit_entry_ids(request_type, request);
    let query_rows_explicit_table_count = query_rows_count
        .as_ref()
        .map(|details| details.explicit_table_count.to_string())
        .unwrap_or_default();
    let query_rows_count_offset = query_rows_count
        .as_ref()
        .map(|details| details.count_offset.to_string())
        .unwrap_or_default();
    let (duplicate_entry_key_count, duplicate_entry_keys) =
        format_nspi_duplicate_entry_keys_for_debug(entries);
    let message = "rca debug mapi nspi rowset";
    tracing::info!(
        rca_debug = true,
        nspi_rowset_debug_schema = NSPI_ROWSET_DEBUG_SCHEMA,
        adapter = "mapi",
        endpoint = "nspi",
        mailbox = %principal.email,
        request_type = request_type,
        request_type_is_query_rows = nspi_request_type_is_query_rows(request_type),
        request_type_debug = ?request_type,
        request_body_bytes = request.len(),
        request_body_preview_hex = %hex_preview(request, 96),
        current_rec = %current_rec,
        requested_entry_ids = %format_nspi_u32_values_for_debug(&requested_entry_ids),
        lookup_value_count = lookup_values.len(),
        lookup_values = %lookup_values.join(","),
        requested_property_tag_count = tags.len(),
        requested_property_tags = %format_nspi_property_tags_for_debug(tags),
        available_entry_count = available_entry_count,
        returned_entry_count = entries.len(),
        row_limit = %row_limit,
        query_rows_explicit_table_count = %query_rows_explicit_table_count,
        query_rows_explicit_entry_ids = %format_nspi_u32_values_for_debug(&query_rows_explicit_entry_ids),
        query_rows_count_offset = %query_rows_count_offset,
        duplicate_entry_key_count = duplicate_entry_key_count,
        duplicate_entry_keys = %duplicate_entry_keys,
        returned_entries = %format_nspi_entry_summaries_for_debug(principal.account_id, entries),
        message = message,
    );
}

fn log_nspi_response_contract(
    principal: &AccountPrincipal,
    request_type: &str,
    request_id: &str,
    method_return_value: u32,
    body: &[u8],
    rowset_present: bool,
    returned_row_count: usize,
    property_tags: &[u32],
    context: &str,
) {
    tracing::info!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = "nspi",
        mailbox = %principal.email,
        request_type = request_type,
        mapi_request_id = request_id,
        transport_response_code = 0u16,
        method_return_value = %format!("{method_return_value:#010x}"),
        method_return_status = nspi_method_status_name(method_return_value),
        item_not_found_encoded = method_return_value == 0x8004_010f,
        body_contains_item_not_found = nspi_body_contains_status(body, 0x8004_010f),
        rowset_present = rowset_present,
        returned_row_count = returned_row_count,
        property_tag_count = property_tags.len(),
        property_tags = %format_nspi_property_tags_for_debug(property_tags),
        body_bytes = body.len(),
        body_preview_hex = %hex_preview(body, 160),
        context = context,
        message = "rca debug mapi nspi response contract",
    );
}

fn nspi_body_contains_status(body: &[u8], status: u32) -> bool {
    let status = status.to_le_bytes();
    body.windows(status.len()).any(|bytes| bytes == status)
}

fn nspi_method_status_name(value: u32) -> &'static str {
    match value {
        0x0000_0000 => "Success",
        0x0004_03A9 => "ErrorsReturned",
        0x8004_010F => "NotFound",
        0x8004_010B => "InvalidParameter",
        0x8004_0102 => "NotEnoughMemory",
        0x8004_0106 => "InvalidBookmark",
        _ => "Unknown",
    }
}

fn format_nspi_entry_summaries_for_debug(
    account_id: Uuid,
    entries: &[ExchangeAddressBookEntry],
) -> String {
    entries
        .iter()
        .map(|entry| {
            let kind = match entry.entry_kind {
                ExchangeAddressBookEntryKind::Account => "account",
                ExchangeAddressBookEntryKind::Contact => "contact",
                ExchangeAddressBookEntryKind::DistributionList => "distribution_list",
            };
            format!(
                "{:#010x}:{}:{}:{}",
                nspi_entry_id(account_id, entry),
                kind,
                entry.email,
                entry.display_name
            )
        })
        .collect::<Vec<_>>()
        .join("|")
}

fn format_nspi_duplicate_entry_keys_for_debug(
    entries: &[ExchangeAddressBookEntry],
) -> (usize, String) {
    let mut counts = BTreeMap::<String, usize>::new();
    for entry in entries {
        let kind = match entry.entry_kind {
            ExchangeAddressBookEntryKind::Account => "account",
            ExchangeAddressBookEntryKind::Contact => "contact",
            ExchangeAddressBookEntryKind::DistributionList => "distribution_list",
        };
        let key = format!(
            "{}:{}:{}",
            kind,
            entry.email.trim().to_ascii_lowercase(),
            entry.display_name.trim().to_ascii_lowercase()
        );
        *counts.entry(key).or_insert(0) += 1;
    }
    let duplicates = counts
        .into_iter()
        .filter(|(_, count)| *count > 1)
        .map(|(key, count)| format!("{key}x{count}"))
        .collect::<Vec<_>>();
    (duplicates.len(), duplicates.join("|"))
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

pub(in crate::mapi) fn nspi_special_table_response(
    principal: &AccountPrincipal,
    request: &[u8],
    request_id: &str,
) -> Response {
    let flags = nspi_request_flags(request);
    let context = format!(
        "special_table;request_flags={};unicode_strings={};address_creation_templates={}",
        flags
            .map(|value| format!("{value:#010x}"))
            .unwrap_or_else(|| "missing".to_string()),
        flags.is_some_and(|value| value & NSPI_UNICODE_STRINGS_FLAG != 0),
        flags.is_some_and(|value| value & NSPI_ADDRESS_CREATION_TEMPLATES_FLAG != 0)
    );
    let mut table_row = Vec::new();
    let property_tags = [
        PID_TAG_ENTRY_ID,
        PID_TAG_CONTAINER_FLAGS,
        PID_TAG_DEPTH,
        PID_TAG_ADDRESS_BOOK_CONTAINER_ID,
        0x3001_001F,
        PID_TAG_ADDRESS_BOOK_IS_MASTER,
    ];
    write_u32(&mut table_row, 6);
    write_address_book_tagged_property_value(
        &mut table_row,
        PID_TAG_ENTRY_ID,
        &NspiValue::OwnedBinary(nspi_gal_container_entry_id()),
    );
    write_address_book_tagged_property_value(
        &mut table_row,
        PID_TAG_CONTAINER_FLAGS,
        &NspiValue::U32(AB_RECIPIENTS | AB_UNMODIFIABLE),
    );
    write_address_book_tagged_property_value(&mut table_row, PID_TAG_DEPTH, &NspiValue::U32(0));
    write_address_book_tagged_property_value(
        &mut table_row,
        PID_TAG_ADDRESS_BOOK_CONTAINER_ID,
        &NspiValue::U32(0),
    );
    write_address_book_tagged_property_value(
        &mut table_row,
        0x3001_001F,
        &NspiValue::String("Global Address List"),
    );
    write_address_book_tagged_property_value(
        &mut table_row,
        PID_TAG_ADDRESS_BOOK_IS_MASTER,
        &NspiValue::Bool(false),
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
    log_nspi_response_contract(
        principal,
        "GetSpecialTable",
        request_id,
        0,
        &body,
        true,
        1,
        &property_tags,
        &context,
    );
    mapi_response("GetSpecialTable", request_id, 0, body, None)
}

fn nspi_request_flags(request: &[u8]) -> Option<u32> {
    request
        .get(..4)
        .and_then(|bytes| bytes.try_into().ok())
        .map(u32::from_le_bytes)
}

fn nspi_gal_container_entry_id() -> Vec<u8> {
    let mut value = Vec::with_capacity(28 + NSPI_GAL_CONTAINER_DN.len() + 1);
    value.extend_from_slice(&[0, 0, 0, 0]);
    value.extend_from_slice(&NSPI_PERMANENT_ENTRY_ID_PROVIDER_UID);
    value.extend_from_slice(&1u32.to_le_bytes());
    value.extend_from_slice(&DT_CONTAINER.to_le_bytes());
    value.extend_from_slice(NSPI_GAL_CONTAINER_DN.as_bytes());
    value.push(0);
    value
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

pub(in crate::mapi) fn nspi_resolved_entry_row(
    account_id: Uuid,
    entry: &ExchangeAddressBookEntry,
    columns: &[u32],
    directory_entries: &[ExchangeAddressBookEntry],
) -> Vec<u8> {
    let mut row = Vec::new();
    row.push(0);
    for property_tag in columns {
        write_address_book_property_value(
            &mut row,
            *property_tag,
            &nspi_entry_value_with_directory(account_id, entry, *property_tag, directory_entries),
        );
    }
    row
}

pub(in crate::mapi) fn nspi_entry_property_value_list(
    account_id: Uuid,
    entry: &ExchangeAddressBookEntry,
    tags: &[u32],
    directory_entries: &[ExchangeAddressBookEntry],
) -> Vec<u8> {
    let mut values = Vec::new();
    write_u32(&mut values, 0);
    write_u32(&mut values, tags.len() as u32);
    for property_tag in tags {
        write_address_book_tagged_property_value(
            &mut values,
            *property_tag,
            &nspi_entry_value_with_directory(account_id, entry, *property_tag, directory_entries),
        );
    }
    values
}

pub(in crate::mapi) enum NspiValue<'a> {
    String(&'a str),
    OwnedString(String),
    MultiString(Vec<String>),
    EmbeddedTable(Uuid, Vec<ExchangeAddressBookEntry>),
    OwnedBinary(Vec<u8>),
    U32(u32),
    Bool(bool),
}

#[cfg(test)]
fn nspi_entry_value(
    account_id: Uuid,
    entry: &ExchangeAddressBookEntry,
    property_tag: u32,
) -> NspiValue<'_> {
    nspi_entry_value_with_directory(account_id, entry, property_tag, &[])
}

pub(in crate::mapi) fn nspi_entry_value_with_directory<'a>(
    account_id: Uuid,
    entry: &'a ExchangeAddressBookEntry,
    property_tag: u32,
    directory_entries: &'a [ExchangeAddressBookEntry],
) -> NspiValue<'a> {
    match property_tag {
        0x0FF8_0102 => NspiValue::OwnedBinary(mapi_mailstore::STORE_REPLICA_GUID.to_vec()),
        0x3902_0102 => NspiValue::OwnedBinary(nspi_entry_permanent_entry_id(entry)),
        0x39FF_001F | 0x39FF_001E => NspiValue::String(&entry.display_name),
        0x3001_001F | 0x3001_001E => NspiValue::String(&entry.display_name),
        0x39FE_001F | 0x39FE_001E => NspiValue::String(&entry.email),
        0x3003_001F | 0x3003_001E => NspiValue::OwnedString(nspi_entry_unprefixed_legacy_dn(entry)),
        0x3A00_001F | 0x3A00_001E => NspiValue::OwnedString(nspi_entry_alias(entry)),
        0x0FFE_0003 => NspiValue::U32(MAPI_MAILUSER_OBJECT_TYPE),
        0x3900_0003 => NspiValue::U32(nspi_entry_display_type(entry)),
        0x3000_0003 => NspiValue::U32(nspi_entry_id(account_id, entry)),
        0x0FF6_0102 => NspiValue::OwnedBinary(nspi_entry_instance_key(account_id, entry)),
        0x0FF9_0102 => NspiValue::OwnedBinary(nspi_entry_record_key(entry)),
        0x0FFF_0102 => NspiValue::OwnedBinary(nspi_entry_permanent_entry_id(entry)),
        0x300B_0102 => NspiValue::OwnedBinary(nspi_entry_search_key(entry)),
        0x3004_001F | 0x3004_001E => NspiValue::String(&entry.email),
        0x3002_001F | 0x3002_001E => NspiValue::String("EX"),
        0x3005_001F | 0x3005_001E => NspiValue::OwnedString(nspi_entry_legacy_dn(entry)),
        0x3A20_001F | 0x3A20_001E => NspiValue::String(&entry.display_name),
        0x3F08_0003 => NspiValue::U32(0),
        0x803C_001F | 0x803C_001E => NspiValue::OwnedString(nspi_entry_unprefixed_legacy_dn(entry)),
        0x800F_101F | 0x800F_101E => NspiValue::MultiString(vec![format!("SMTP:{}", entry.email)]),
        0x8009_000D => NspiValue::EmbeddedTable(
            account_id,
            nspi_distribution_list_members(entry, directory_entries),
        ),
        0x8CE2_0003 => NspiValue::U32(
            nspi_distribution_list_members(entry, directory_entries)
                .len()
                .min(u32::MAX as usize) as u32,
        ),
        0x8CE3_0003 => NspiValue::U32(0),
        0x8C6D_0102 => NspiValue::OwnedBinary(entry.id.to_bytes_le().to_vec()),
        0xFFFD_0003 => NspiValue::U32(0),
        _ => match property_tag & 0xFFFF {
            0x001F | 0x001E => NspiValue::String(""),
            0x0003 => NspiValue::U32(0),
            _ => NspiValue::U32(0),
        },
    }
}

fn nspi_distribution_list_members(
    entry: &ExchangeAddressBookEntry,
    directory_entries: &[ExchangeAddressBookEntry],
) -> Vec<ExchangeAddressBookEntry> {
    if entry.entry_kind != ExchangeAddressBookEntryKind::DistributionList {
        return Vec::new();
    }
    entry
        .member_emails
        .iter()
        .filter_map(|email| {
            let normalized = email.trim().to_ascii_lowercase();
            directory_entries
                .iter()
                .find(|candidate| {
                    candidate.entry_kind != ExchangeAddressBookEntryKind::DistributionList
                        && candidate.email.trim().eq_ignore_ascii_case(&normalized)
                })
                .cloned()
        })
        .collect()
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
        .filter_map(nspi_identity_request)
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
    let Some(request) = nspi_identity_request(&entry) else {
        return Ok(());
    };
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
    let records = store
        .fetch_or_allocate_mapi_identities(principal.account_id, requests)
        .await?;
    for (request, record) in requests.iter().zip(records.iter()) {
        if let Some(kind_key) = nspi_identity_kind_key_for_request(request.object_kind) {
            remember_nspi_identity(
                principal.account_id,
                kind_key,
                record.canonical_id,
                record.object_id,
            );
        }
    }
    Ok(())
}

fn nspi_identity_request(entry: &ExchangeAddressBookEntry) -> Option<MapiIdentityRequest> {
    let object_kind = match entry.entry_kind {
        ExchangeAddressBookEntryKind::Account => MapiIdentityObjectKind::Account,
        ExchangeAddressBookEntryKind::Contact => MapiIdentityObjectKind::Contact,
        ExchangeAddressBookEntryKind::DistributionList => return None,
    };
    Some(MapiIdentityRequest {
        object_kind,
        canonical_id: entry.id,
        reserved_global_counter: None,
        source_key: None,
    })
}

fn remember_nspi_identity(account_id: Uuid, kind_key: u8, canonical_id: Uuid, object_id: u64) {
    let mut ids = NSPI_OBJECT_IDS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    ids.insert((account_id, kind_key, canonical_id), object_id);
}

fn mapped_nspi_object_id(account_id: Uuid, entry: &ExchangeAddressBookEntry) -> Option<u64> {
    NSPI_OBJECT_IDS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .get(&(
            account_id,
            nspi_identity_kind_key(entry.entry_kind),
            entry.id,
        ))
        .copied()
}

fn nspi_identity_kind_key(entry_kind: ExchangeAddressBookEntryKind) -> u8 {
    match entry_kind {
        ExchangeAddressBookEntryKind::Account => 1,
        ExchangeAddressBookEntryKind::Contact => 2,
        ExchangeAddressBookEntryKind::DistributionList => 3,
    }
}

fn nspi_identity_kind_key_for_request(object_kind: MapiIdentityObjectKind) -> Option<u8> {
    match object_kind {
        MapiIdentityObjectKind::Account => Some(1),
        MapiIdentityObjectKind::Contact => Some(2),
        _ => None,
    }
}

pub(in crate::mapi) fn nspi_entry_id(account_id: Uuid, entry: &ExchangeAddressBookEntry) -> u32 {
    mapped_nspi_object_id(account_id, entry)
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
            ExchangeAddressBookEntryKind::Contact
            | ExchangeAddressBookEntryKind::DistributionList => 0x4000_0000,
        };
    (value >= 2).then_some(value)
}

fn legacy_nspi_entry_id(entry: &ExchangeAddressBookEntry) -> u32 {
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

pub(in crate::mapi) fn principal_minimal_entry_id(principal: &AccountPrincipal) -> u32 {
    nspi_entry_id(
        principal.account_id,
        &principal_address_book_entry(principal),
    )
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
        member_emails: Vec::new(),
    }
}

pub(in crate::mapi) fn nspi_entry_display_type(entry: &ExchangeAddressBookEntry) -> u32 {
    match (entry.entry_kind, entry.directory_kind) {
        (ExchangeAddressBookEntryKind::DistributionList, _) => 1,
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
    write_u32(body, 0);
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
        (0x101E, NspiValue::MultiString(values)) => {
            body.push(0xFF);
            write_multi_string8(body, values);
        }
        (0x101F, NspiValue::MultiString(values)) => {
            body.push(0xFF);
            write_multi_string(body, values);
        }
        (0x000D, NspiValue::EmbeddedTable(account_id, entries)) => {
            write_embedded_address_book_table(body, *account_id, entries)
        }
        (0x0102, NspiValue::OwnedBinary(value)) => write_nspi_binary(body, value),
        (0x0003, NspiValue::U32(value)) => write_u32(body, *value),
        (0x0003, _) => write_u32(body, 0),
        (0x000B, NspiValue::Bool(value)) => body.push(u8::from(*value)),
        (0x000B, _) => body.push(0),
        (0x101E | 0x101F, _) => write_u32(body, 0),
        (_, NspiValue::U32(value)) => write_u32(body, *value),
        (_, NspiValue::Bool(value)) => body.push(u8::from(*value)),
        (_, NspiValue::OwnedBinary(value)) => write_nspi_binary(body, value),
        (_, NspiValue::MultiString(values)) => {
            body.push(0xFF);
            write_multi_string(body, values);
        }
        (_, NspiValue::EmbeddedTable(account_id, entries)) => {
            write_embedded_address_book_table(body, *account_id, entries)
        }
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

fn write_embedded_address_book_table(
    body: &mut Vec<u8>,
    account_id: Uuid,
    entries: &[ExchangeAddressBookEntry],
) {
    let columns = [0x3001_001F, 0x39FE_001F, 0x3003_001F, 0x3900_0003];
    write_large_property_tag_array(body, &columns);
    write_u32(body, entries.len().min(u32::MAX as usize) as u32);
    for entry in entries {
        body.extend_from_slice(&nspi_resolved_entry_row(
            account_id, entry, &columns, entries,
        ));
    }
}

fn write_nspi_binary(body: &mut Vec<u8>, value: &[u8]) {
    let len = value.len().min(u32::MAX as usize);
    write_u32(body, len as u32);
    body.extend_from_slice(&value[..len]);
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

fn nspi_entry_permanent_entry_id(entry: &ExchangeAddressBookEntry) -> Vec<u8> {
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
    let mut value = value.trim().trim_matches('\0').to_ascii_lowercase();
    if let Some(rest) = value.strip_prefix("=smtp:") {
        value = rest.to_string();
    } else if let Some(rest) = value.strip_prefix("smtp:") {
        value = rest.to_string();
    }
    value
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
mod tests {
    use super::*;

    #[test]
    fn nspi_request_and_property_manifests_cover_implemented_static_values() {
        for request_type in NSPI_SUPPORTED_REQUEST_TYPES {
            assert!(
                request_type.requires_nspi_session()
                    || matches!(
                        request_type,
                        MapiRequestType::Bind | MapiRequestType::DnToMid | MapiRequestType::Unbind
                    )
            );
            assert_ne!(request_type.header_value(), "");
        }

        for tag in NSPI_BOOTSTRAP_PROPERTY_TAGS {
            assert!(nspi_property_tag_is_supported(*tag));
        }
        for tag in NSPI_ADDITIONAL_REQUESTED_PROPERTY_TAGS {
            assert!(nspi_property_tag_is_supported(*tag));
        }
        assert_eq!(
            nspi_known_unsupported_property_tag_name(0x3A06_001F),
            Some("PidTagGivenName")
        );
        assert_eq!(nspi_known_unsupported_property_tag_name(0x0FF8_0102), None);
        assert_eq!(nspi_known_unsupported_property_tag_name(0x3A20_001F), None);
        assert_eq!(
            nspi_known_unsupported_property_tag_name(0x3A1B_101F),
            Some("PidTagBusiness2TelephoneNumbers")
        );
    }

    #[test]
    fn nspi_entry_required_address_book_properties_match_exchange_identity_contract() {
        let account_id = Uuid::parse_str("ea339446-27b9-4a9c-b0de-873f03a35376").unwrap();
        let entry = ExchangeAddressBookEntry {
            id: Uuid::parse_str("26b8ebbd-63a5-4741-b8d6-d7eda9c31c3d").unwrap(),
            display_name: "Bob Contact".to_string(),
            email: "bob.contact@example.test".to_string(),
            entry_kind: ExchangeAddressBookEntryKind::Contact,
            directory_kind: ExchangeAddressBookDirectoryKind::Person,
            member_emails: Vec::new(),
        };
        let legacy_dn = nspi_entry_unprefixed_legacy_dn(&entry);
        let permanent_entry_id = nspi_entry_permanent_entry_id(&entry);

        assert_eq!(
            nspi_string_value(nspi_entry_value(account_id, &entry, 0x3002_001F)),
            "EX"
        );
        assert_eq!(
            nspi_string_value(nspi_entry_value(account_id, &entry, 0x3003_001F)),
            legacy_dn
        );
        assert_eq!(
            nspi_binary_value(nspi_entry_value(account_id, &entry, 0x0FFF_0102)),
            permanent_entry_id
        );
        assert_eq!(
            nspi_binary_value(nspi_entry_value(account_id, &entry, 0x3902_0102)),
            permanent_entry_id
        );
        assert_eq!(
            nspi_binary_value(nspi_entry_value(account_id, &entry, 0x0FF9_0102)),
            permanent_entry_id
        );
        assert_eq!(
            nspi_string_value(nspi_entry_value(account_id, &entry, 0x803C_001F)),
            legacy_dn
        );
        assert_eq!(
            nspi_string_value(nspi_entry_value(account_id, &entry, 0x3A20_001F)),
            "Bob Contact"
        );
        assert_eq!(
            nspi_string_value(nspi_entry_value(account_id, &entry, 0x39FF_001F)),
            "Bob Contact"
        );
        assert_eq!(
            nspi_u32_value(nspi_entry_value(account_id, &entry, 0x3F08_0003)),
            0
        );
        assert_eq!(
            nspi_u32_value(nspi_entry_value(account_id, &entry, 0xFFFD_0003)),
            0
        );
        assert_eq!(
            nspi_binary_value(nspi_entry_value(account_id, &entry, 0x300B_0102)),
            format!("EX:{}", legacy_dn.to_ascii_uppercase())
                .bytes()
                .chain(std::iter::once(0))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn principal_lookup_accepts_autodiscover_and_connect_legacy_dn_aliases() {
        let principal = AccountPrincipal {
            tenant_id: Uuid::from_u128(0xaaaaaaaa_aaaa_aaaa_aaaa_aaaaaaaaaaaa),
            account_id: Uuid::from_u128(0xbbbbbbbb_bbbb_bbbb_bbbb_bbbbbbbbbbbb),
            email: "test@l-p-e.ch".to_string(),
            display_name: "test".to_string(),
            quota_mb: None,
            quota_used_octets: None,
        };

        assert!(nspi_lookup_matches_principal(
            "/o=LPE/ou=Exchange Administrative Group/cn=Recipients/cn=test-l-p-e-ch",
            &principal
        ));
        assert!(nspi_lookup_matches_principal(
            "/o=LPE/ou=Exchange Administrative Group/cn=Recipients/cn=acct-test-l-p-e-ch",
            &principal
        ));
        assert!(nspi_lookup_matches_principal(
            "/o=LPE/ou=Exchange Administrative Group/cn=Recipients/cn=test",
            &principal
        ));
    }

    #[test]
    fn get_props_stat_current_rec_is_parsed_from_documented_stat_field() {
        let request = hex_bytes(
            "00000000ff0000000000000012000080000000000000000000000000b00400000904000009080000ff0100000002016d8c00000000",
        );

        assert_eq!(nspi_stat_current_rec(&request), Some(0x8000_0012));
    }

    #[test]
    fn get_props_stat_words_are_not_entry_ids_when_current_rec_is_empty() {
        let request = hex_bytes(
            "00000000ff0000000000000000000000000000000000000000000000b00400000904000009080000ff0100000002016d8c00000000",
        );

        assert_eq!(nspi_stat_current_rec(&request), None);
        assert!(!nspi_request_has_entry_selector(&request));
    }

    #[test]
    fn requested_entry_ids_ignore_misaligned_utf16_lookup_bytes() {
        let mut request = vec![0, 0, 0];
        request.extend("test@l-p-e.ch\0".encode_utf16().flat_map(u16::to_le_bytes));

        assert!(nspi_requested_entry_ids(&request).is_empty());
        assert_eq!(
            scan_address_book_lookup_values(&request),
            vec!["test@l-p-e.ch".to_string()]
        );
    }

    #[test]
    fn query_rows_count_skips_explicit_table_before_count() {
        let mut request = Vec::new();
        request.extend_from_slice(&0u32.to_le_bytes());
        request.extend_from_slice(&[0; 36]);
        request.extend_from_slice(&2u32.to_le_bytes());
        request.extend_from_slice(&0x8000_0034u32.to_le_bytes());
        request.extend_from_slice(&0x4000_0001u32.to_le_bytes());
        request.extend_from_slice(&7u32.to_le_bytes());

        assert_eq!(nspi_query_rows_count("QueryRows", &request), Some(7));
    }

    #[test]
    fn query_rows_count_parses_outlook_explicit_table_body() {
        let request = hex_bytes(
            "00000000ff0000000000000000000000000000000000000000000000e40400000904000009080000010000003400008001000000ff0b0000000201ff0f1f0001300300fe0f030000391f00203a1f0003301f0002300b00403a1f00ff391f00",
        );

        assert_eq!(nspi_query_rows_count("QueryRows", &request), Some(1));
        assert_eq!(
            nspi_query_rows_explicit_entry_ids("QueryRows", &request),
            vec![0x8000_0034]
        );
    }

    #[test]
    fn query_rows_parser_falls_back_to_body_shape_for_logged_outlook_body() {
        let request = hex_bytes(
            "00000000ff0000000000000000000000000000000000000000000000e40400000904000009080000010000003400008001000000ff0b0000000201ff0f1f0001300300fe0f030000391f00203a1f0003301f0002300b00403a1f00ff391f00",
        );

        assert_eq!(nspi_query_rows_count("", &request), Some(1));
        assert_eq!(
            nspi_query_rows_explicit_entry_ids("", &request),
            vec![0x8000_0034]
        );
    }

    #[test]
    fn query_rows_parser_handles_shifted_outlook_stat_boundary() {
        let request = hex_bytes(
            "00000000ff000000000000000000000000000000000000000000000000e40400000904000009080000010000003400008001000000ff0b0000000201ff0f1f0001300300fe0f030000391f00203a1f0003301f0002300b00403a1f00ff391f00",
        );

        assert_eq!(nspi_query_rows_count("QueryRows", &request), Some(1));
        assert_eq!(
            nspi_query_rows_explicit_entry_ids("QueryRows", &request),
            vec![0x8000_0034]
        );
    }

    #[test]
    fn query_rows_explicit_table_filters_rows_by_requested_mid() {
        let account_id = Uuid::parse_str("ea339446-27b9-4a9c-b0de-873f03a35376").unwrap();
        let contact = ExchangeAddressBookEntry {
            id: Uuid::from_bytes([0x37, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]),
            display_name: "Denis Ducret".to_string(),
            email: "denis.ducret@sdic.ch".to_string(),
            entry_kind: ExchangeAddressBookEntryKind::Contact,
            directory_kind: ExchangeAddressBookDirectoryKind::Person,
            member_emails: Vec::new(),
        };
        let account = ExchangeAddressBookEntry {
            id: Uuid::from_bytes([0x34, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]),
            display_name: "test".to_string(),
            email: "test@l-p-e.ch".to_string(),
            entry_kind: ExchangeAddressBookEntryKind::Account,
            directory_kind: ExchangeAddressBookDirectoryKind::Person,
            member_emails: Vec::new(),
        };

        let filtered =
            nspi_filter_explicit_table_entries(account_id, vec![contact, account], &[0x8000_0034]);

        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].email, "test@l-p-e.ch");
    }

    #[test]
    fn lookup_scanner_ignores_binary_words_that_only_contain_at_sign() {
        assert!(scan_address_book_lookup_values(b"@\x3a\x1f\0").is_empty());
        assert_eq!(
            scan_address_book_lookup_values(b"SMTP:alice@example.test\0"),
            vec!["alice@example.test".to_string()]
        );
    }

    #[test]
    fn nspi_entry_debug_summary_includes_mid_kind_email_and_name() {
        let account_id = Uuid::parse_str("ea339446-27b9-4a9c-b0de-873f03a35376").unwrap();
        let entry = ExchangeAddressBookEntry {
            id: Uuid::parse_str("26b8ebbd-63a5-4741-b8d6-d7eda9c31c3d").unwrap(),
            display_name: "Bob Contact".to_string(),
            email: "bob.contact@example.test".to_string(),
            entry_kind: ExchangeAddressBookEntryKind::Contact,
            directory_kind: ExchangeAddressBookDirectoryKind::Person,
            member_emails: Vec::new(),
        };

        let summary = format_nspi_entry_summaries_for_debug(account_id, &[entry]);

        assert!(summary.contains(":contact:bob.contact@example.test:Bob Contact"));
    }

    #[test]
    fn nspi_duplicate_debug_groups_rows_by_kind_email_and_name() {
        let entries = vec![
            ExchangeAddressBookEntry {
                id: Uuid::parse_str("26b8ebbd-63a5-4741-b8d6-d7eda9c31c3d").unwrap(),
                display_name: "Bob Contact".to_string(),
                email: "bob.contact@example.test".to_string(),
                entry_kind: ExchangeAddressBookEntryKind::Contact,
                directory_kind: ExchangeAddressBookDirectoryKind::Person,
                member_emails: Vec::new(),
            },
            ExchangeAddressBookEntry {
                id: Uuid::parse_str("9bd2958d-9858-4fe3-8e6b-4ddd9dcc6bc6").unwrap(),
                display_name: " bob contact ".to_string(),
                email: "BOB.CONTACT@example.test".to_string(),
                entry_kind: ExchangeAddressBookEntryKind::Contact,
                directory_kind: ExchangeAddressBookDirectoryKind::Person,
                member_emails: Vec::new(),
            },
        ];

        let (count, keys) = format_nspi_duplicate_entry_keys_for_debug(&entries);

        assert_eq!(count, 1);
        assert_eq!(keys, "contact:bob.contact@example.test:bob contactx2");
    }

    #[test]
    fn nspi_duplicate_contacts_have_distinct_outlook_identity_fields() {
        let account_id = Uuid::parse_str("ea339446-27b9-4a9c-b0de-873f03a35376").unwrap();
        let first = ExchangeAddressBookEntry {
            id: Uuid::parse_str("26b8ebbd-63a5-4741-b8d6-d7eda9c31c3d").unwrap(),
            display_name: "Bob Contact".to_string(),
            email: "bob.contact@example.test".to_string(),
            entry_kind: ExchangeAddressBookEntryKind::Contact,
            directory_kind: ExchangeAddressBookDirectoryKind::Person,
            member_emails: Vec::new(),
        };
        let second = ExchangeAddressBookEntry {
            id: Uuid::parse_str("9bd2958d-9858-4fe3-8e6b-4ddd9dcc6bc6").unwrap(),
            display_name: "Bob Contact".to_string(),
            email: "bob.contact@example.test".to_string(),
            entry_kind: ExchangeAddressBookEntryKind::Contact,
            directory_kind: ExchangeAddressBookDirectoryKind::Person,
            member_emails: Vec::new(),
        };

        assert_eq!(
            nspi_string_value(nspi_entry_value(account_id, &first, 0x39FE_001F)),
            nspi_string_value(nspi_entry_value(account_id, &second, 0x39FE_001F))
        );
        assert_ne!(
            nspi_u32_value(nspi_entry_value(account_id, &first, 0x3000_0003)),
            nspi_u32_value(nspi_entry_value(account_id, &second, 0x3000_0003))
        );
        assert_ne!(nspi_entry_legacy_dn(&first), nspi_entry_legacy_dn(&second));
        assert_ne!(
            nspi_binary_value(nspi_entry_value(account_id, &first, 0x0FF6_0102)),
            nspi_binary_value(nspi_entry_value(account_id, &second, 0x0FF6_0102))
        );
        assert_ne!(
            nspi_binary_value(nspi_entry_value(account_id, &first, 0x0FF9_0102)),
            nspi_binary_value(nspi_entry_value(account_id, &second, 0x0FF9_0102))
        );
        assert_ne!(
            nspi_binary_value(nspi_entry_value(account_id, &first, 0x0FFF_0102)),
            nspi_binary_value(nspi_entry_value(account_id, &second, 0x0FFF_0102))
        );
        assert_ne!(
            nspi_binary_value(nspi_entry_value(account_id, &first, 0x300B_0102)),
            nspi_binary_value(nspi_entry_value(account_id, &second, 0x300B_0102))
        );
    }

    fn nspi_binary_value(value: NspiValue<'_>) -> Vec<u8> {
        match value {
            NspiValue::OwnedBinary(value) => value,
            _ => panic!("expected binary NSPI value"),
        }
    }

    fn nspi_u32_value(value: NspiValue<'_>) -> u32 {
        match value {
            NspiValue::U32(value) => value,
            _ => panic!("expected u32 NSPI value"),
        }
    }

    fn nspi_string_value(value: NspiValue<'_>) -> String {
        match value {
            NspiValue::String(value) => value.to_string(),
            NspiValue::OwnedString(value) => value,
            _ => panic!("expected string NSPI value"),
        }
    }

    fn hex_bytes(hex: &str) -> Vec<u8> {
        let compact = hex
            .as_bytes()
            .iter()
            .copied()
            .filter(|byte| !byte.is_ascii_whitespace())
            .collect::<Vec<_>>();
        compact
            .chunks_exact(2)
            .map(|chunk| {
                let high = hex_value(chunk[0]);
                let low = hex_value(chunk[1]);
                (high << 4) | low
            })
            .collect()
    }

    fn hex_value(byte: u8) -> u8 {
        match byte {
            b'0'..=b'9' => byte - b'0',
            b'a'..=b'f' => byte - b'a' + 10,
            b'A'..=b'F' => byte - b'A' + 10,
            _ => panic!("invalid hex byte"),
        }
    }
}
