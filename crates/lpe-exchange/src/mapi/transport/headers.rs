use std::env;

use anyhow::{anyhow, Result};
use axum::{
    http::{
        header::{CONTENT_LENGTH, CONTENT_TYPE, SET_COOKIE},
        HeaderMap,
    },
    response::Response,
};
use lpe_domain::crypto::hex_lower;
use uuid::Uuid;

use super::{
    mapi_payload_fingerprint, MapiRequestType, MAPI_CONTENT_TYPE, MAPI_OCTET_STREAM_CONTENT_TYPE,
};

pub(in crate::mapi) fn request_type(headers: &HeaderMap) -> Result<MapiRequestType> {
    let value = headers
        .get("x-requesttype")
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow!("missing MAPI X-RequestType header"))?;
    Ok(match value.to_ascii_lowercase().as_str() {
        "connect" => MapiRequestType::Connect,
        "disconnect" => MapiRequestType::Disconnect,
        "execute" => MapiRequestType::Execute,
        "notificationwait" => MapiRequestType::NotificationWait,
        "bind" => MapiRequestType::Bind,
        "unbind" => MapiRequestType::Unbind,
        "comparemids" => MapiRequestType::CompareMids,
        "dntoeph" => MapiRequestType::DnToEph,
        "dntomid" => MapiRequestType::DnToMid,
        "getmatches" => MapiRequestType::GetMatches,
        "getproplist" => MapiRequestType::GetPropList,
        "getprops" => MapiRequestType::GetProps,
        "gethierarchyinfo" => MapiRequestType::GetHierarchyInfo,
        "getspecialtable" => MapiRequestType::GetSpecialTable,
        "gettemplateinfo" => MapiRequestType::GetTemplateInfo,
        "modlinkatt" => MapiRequestType::ModLinkAtt,
        "modprops" => MapiRequestType::ModProps,
        "getaddressbookurl" => MapiRequestType::GetAddressBookUrl,
        "getmailboxurl" => MapiRequestType::GetMailboxUrl,
        "querycolumns" => MapiRequestType::QueryColumns,
        "queryrows" => MapiRequestType::QueryRows,
        "resolvenames" => MapiRequestType::ResolveNames,
        "resortrestriction" => MapiRequestType::ResortRestriction,
        "seekentries" => MapiRequestType::SeekEntries,
        "updatestat" => MapiRequestType::UpdateStat,
        "ping" => MapiRequestType::Ping,
        _ => MapiRequestType::Unsupported(value.to_string()),
    })
}

pub(in crate::mapi) fn request_id(headers: &HeaderMap) -> Option<String> {
    headers
        .get("x-requestid")
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

pub(in crate::mapi) fn is_guid_counter_header(value: &str) -> bool {
    let Some((raw_guid, counter)) = value.rsplit_once(':') else {
        return false;
    };
    let guid = raw_guid
        .strip_prefix('{')
        .and_then(|value| value.strip_suffix('}'))
        .unwrap_or(raw_guid);
    !counter.is_empty()
        && counter.bytes().all(|byte| byte.is_ascii_digit())
        && Uuid::parse_str(guid).is_ok()
}

pub(crate) fn guid_counter_debug(value: &str) -> (String, String) {
    let Some((raw_guid, counter)) = value.rsplit_once(':') else {
        return (String::new(), String::new());
    };
    let guid = raw_guid
        .strip_prefix('{')
        .and_then(|value| value.strip_suffix('}'))
        .unwrap_or(raw_guid);
    if Uuid::parse_str(guid).is_err() {
        return (String::new(), String::new());
    }
    (guid.to_ascii_lowercase(), counter.to_string())
}

pub(crate) fn client_flow_key(client_info: &str) -> String {
    let (guid, _) = guid_counter_debug(client_info);
    if guid.is_empty() {
        String::new()
    } else {
        format!("{:016x}", mapi_payload_fingerprint(guid.as_bytes()))
    }
}

pub(in crate::mapi) fn client_info(headers: &HeaderMap) -> Option<String> {
    headers
        .get("x-clientinfo")
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

pub(in crate::mapi) fn host_header(headers: &HeaderMap) -> Option<String> {
    headers
        .get("host")
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

pub(in crate::mapi) fn content_length_header(headers: &HeaderMap) -> Option<String> {
    headers
        .get(CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

pub(in crate::mapi) fn is_valid_content_length(value: &str) -> bool {
    !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit())
}

pub(in crate::mapi) fn content_length_matches_body(value: &str, body: &[u8]) -> bool {
    value == "0"
        || value
            .parse::<usize>()
            .is_ok_and(|length| length == body.len())
}

pub(in crate::mapi) fn is_mapi_content_type(headers: &HeaderMap) -> bool {
    let Some(content_type) = headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .map(str::trim)
    else {
        return false;
    };

    content_type.eq_ignore_ascii_case(MAPI_CONTENT_TYPE)
        || content_type.eq_ignore_ascii_case(MAPI_OCTET_STREAM_CONTENT_TYPE)
}

pub(in crate::mapi) fn response_set_cookie_names(response: &Response) -> String {
    response
        .headers()
        .get_all(SET_COOKIE)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .filter_map(|value| {
            value
                .split_once('=')
                .map(|(name, _)| name.trim().to_string())
        })
        .filter(|name| !name.is_empty())
        .collect::<Vec<_>>()
        .join(",")
}

pub(in crate::mapi) fn response_header(response: &Response, name: &str) -> Option<String> {
    response
        .headers()
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

pub(crate) fn safe_header(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| value.chars().take(240).collect())
}

pub(crate) fn debug_payload_preview_hex(bytes: &[u8]) -> String {
    let limit = debug_payload_preview_limit();
    if limit == 0 {
        return String::new();
    }
    hex_preview(bytes, limit)
}

pub(in crate::mapi) fn debug_payload_preview_limit() -> usize {
    env::var("LPE_RCA_DEBUG_PAYLOAD_PREVIEW_BYTES")
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .unwrap_or(0)
        .min(512)
}

pub(crate) fn hex_preview(bytes: &[u8], limit: usize) -> String {
    hex_lower(&bytes[..bytes.len().min(limit)])
}
