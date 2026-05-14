use super::dispatch::*;
use super::nspi::*;
use super::rop::*;
use super::session::*;
use super::*;

pub(in crate::mapi) const MAPI_CONTENT_TYPE: &str = "application/mapi-http";
pub(in crate::mapi) const MAPI_OCTET_STREAM_CONTENT_TYPE: &str = "application/octet-stream";
pub(in crate::mapi) const MAPI_SERVER_APPLICATION: &str = "Exchange/15.20.0485.000";
pub(in crate::mapi) const EMSMDB_COOKIE: &str = "MapiContext";
pub(in crate::mapi) const NSPI_COOKIE: &str = "MapiContext";
pub(in crate::mapi) const EMSMDB_SEQUENCE_COOKIE: &str = "MapiSequence";
pub(in crate::mapi) const NSPI_SEQUENCE_COOKIE: &str = "MapiSequence";
pub(in crate::mapi) const EMSMDB_COOKIE_PATH: &str = "/mapi/emsmdb";
pub(in crate::mapi) const NSPI_COOKIE_PATH: &str = "/mapi/nspi";
pub(in crate::mapi) const MAPI_SESSION_MAX_AGE_SECONDS: u32 = 1_800;
pub(in crate::mapi) const NSPI_UNICODE_CODEPAGE: u32 = 1200;
pub(in crate::mapi) const MAPI_MAILUSER_OBJECT_TYPE: u32 = 6;
pub(in crate::mapi) const NSPI_MID_RESOLVED: u32 = 0x0000_0002;
pub(in crate::mapi) const MAX_CACHED_EXECUTE_REQUESTS: usize = 64;
pub(in crate::mapi) const NSPI_SERVER_GUID: [u8; 16] = [
    0x2b, 0xe6, 0x0b, 0x5d, 0x9f, 0x35, 0x3f, 0x45, 0x9a, 0x68, 0x4c, 0x4b, 0xc5, 0x8f, 0x3f, 0x30,
];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MapiEndpoint {
    Emsmdb,
    Nspi,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::mapi) enum MapiRequestType {
    Connect,
    Disconnect,
    Execute,
    NotificationWait,
    Bind,
    Unbind,
    CompareMids,
    DnToMid,
    GetMatches,
    GetPropList,
    GetProps,
    GetSpecialTable,
    GetTemplateInfo,
    ModLinkAtt,
    ModProps,
    GetAddressBookUrl,
    GetMailboxUrl,
    QueryColumns,
    QueryRows,
    ResolveNames,
    ResortRestriction,
    SeekEntries,
    UpdateStat,
    Ping,
    Unsupported(String),
}

pub(crate) async fn handle_mapi<S, V>(
    store: &S,
    validator: &Validator<V>,
    endpoint: MapiEndpoint,
    headers: &HeaderMap,
    _body: &[u8],
) -> Result<Response>
where
    S: ExchangeStore,
    V: Detector,
{
    let principal = authenticate_account(store, None, headers, "mapi").await?;
    let request_type = match request_type(headers) {
        Ok(request_type) => request_type,
        Err(error) => {
            let request_id = request_id(headers).unwrap_or_default();
            let response = mapi_diagnostic_response("Unknown", &request_id, 7, &error.to_string());
            let response = finalize_mapi_response(response, headers);
            log_mapi_connection(
                endpoint,
                &principal,
                headers,
                _body,
                "Unknown",
                &request_id,
                &response,
            );
            return Ok(response);
        }
    };
    let request_type_label = request_type.header_value().to_string();
    let Some(request_id) = request_id(headers) else {
        let response = mapi_diagnostic_response(
            &request_type_label,
            "",
            7,
            "missing MAPI X-RequestId header",
        );
        let response = finalize_mapi_response(response, headers);
        log_mapi_connection(
            endpoint,
            &principal,
            headers,
            _body,
            &request_type_label,
            "",
            &response,
        );
        return Ok(response);
    };
    if !is_guid_counter_header(&request_id) {
        let response = mapi_diagnostic_response(
            &request_type_label,
            &request_id,
            4,
            "invalid MAPI X-RequestId header; expected {GUID}:counter",
        );
        let response = finalize_mapi_response(response, headers);
        log_mapi_connection(
            endpoint,
            &principal,
            headers,
            _body,
            &request_type_label,
            &request_id,
            &response,
        );
        return Ok(response);
    }
    let Some(client_info) = client_info(headers) else {
        let response = mapi_diagnostic_response(
            &request_type_label,
            &request_id,
            7,
            "missing MAPI X-ClientInfo header",
        );
        let response = finalize_mapi_response(response, headers);
        log_mapi_connection(
            endpoint,
            &principal,
            headers,
            _body,
            &request_type_label,
            &request_id,
            &response,
        );
        return Ok(response);
    };
    if !is_guid_counter_header(&client_info) {
        let response = mapi_diagnostic_response(
            &request_type_label,
            &request_id,
            4,
            "invalid MAPI X-ClientInfo header; expected {GUID}:counter",
        );
        let response = finalize_mapi_response(response, headers);
        log_mapi_connection(
            endpoint,
            &principal,
            headers,
            _body,
            &request_type_label,
            &request_id,
            &response,
        );
        return Ok(response);
    }
    if host_header(headers).is_none() {
        let response = mapi_diagnostic_response(
            &request_type_label,
            &request_id,
            7,
            "missing MAPI Host header",
        );
        let response = finalize_mapi_response(response, headers);
        log_mapi_connection(
            endpoint,
            &principal,
            headers,
            _body,
            &request_type_label,
            &request_id,
            &response,
        );
        return Ok(response);
    }
    let Some(content_length) = content_length_header(headers) else {
        let response = mapi_diagnostic_response(
            &request_type_label,
            &request_id,
            7,
            "missing MAPI Content-Length header",
        );
        let response = finalize_mapi_response(response, headers);
        log_mapi_connection(
            endpoint,
            &principal,
            headers,
            _body,
            &request_type_label,
            &request_id,
            &response,
        );
        return Ok(response);
    };
    if !is_valid_content_length(&content_length) {
        let response = mapi_diagnostic_response(
            &request_type_label,
            &request_id,
            4,
            "invalid MAPI Content-Length header",
        );
        let response = finalize_mapi_response(response, headers);
        log_mapi_connection(
            endpoint,
            &principal,
            headers,
            _body,
            &request_type_label,
            &request_id,
            &response,
        );
        return Ok(response);
    }
    if !is_mapi_content_type(headers) {
        let response = mapi_diagnostic_response(
            &request_type_label,
            &request_id,
            4,
            "MAPI requests must use Content-Type application/mapi-http.",
        );
        let response = finalize_mapi_response(response, headers);
        log_mapi_connection(
            endpoint,
            &principal,
            headers,
            _body,
            &request_type_label,
            &request_id,
            &response,
        );
        return Ok(response);
    }

    let _nspi_active_request =
        if endpoint == MapiEndpoint::Nspi && request_type.requires_nspi_session() {
            match established_session_request(
                endpoint,
                &principal,
                headers,
                &request_type_label,
                &request_id,
            ) {
                Ok(active_request) => Some(active_request),
                Err(response) => {
                    let response = finalize_mapi_response(response, headers);
                    log_mapi_connection(
                        endpoint,
                        &principal,
                        headers,
                        _body,
                        &request_type_label,
                        &request_id,
                        &response,
                    );
                    return Ok(response);
                }
            }
        } else {
            None
        };

    let response = match (endpoint, request_type) {
        (MapiEndpoint::Emsmdb, MapiRequestType::Connect) => {
            connect_response(endpoint, &principal, headers, &request_id)
        }
        (MapiEndpoint::Emsmdb, MapiRequestType::Disconnect) => {
            disconnect_response(endpoint, &principal, headers, &request_id, "Disconnect")
        }
        (MapiEndpoint::Emsmdb, MapiRequestType::Execute) => {
            execute_response(
                store,
                validator,
                endpoint,
                &principal,
                headers,
                _body,
                &request_id,
            )
            .await
        }
        (MapiEndpoint::Emsmdb, MapiRequestType::NotificationWait) => {
            notification_wait_response(endpoint, &principal, headers, &request_id)
        }
        (MapiEndpoint::Nspi, MapiRequestType::Bind) => {
            bind_response(endpoint, &principal, headers, &request_id)
        }
        (MapiEndpoint::Nspi, MapiRequestType::Unbind) => {
            disconnect_response(endpoint, &principal, headers, &request_id, "Unbind")
        }
        (MapiEndpoint::Nspi, MapiRequestType::CompareMids) => {
            nspi_u32_result_response("CompareMIds", &request_id, 0)
        }
        (MapiEndpoint::Nspi, MapiRequestType::DnToMid) => {
            nspi_dn_to_mid_response(store, &principal, _body, &request_id).await
        }
        (MapiEndpoint::Nspi, MapiRequestType::GetMatches) => {
            nspi_matches_response(store, &principal, _body, &request_id).await
        }
        (MapiEndpoint::Nspi, MapiRequestType::GetPropList) => {
            nspi_property_tags_response("GetPropList", &request_id)
        }
        (MapiEndpoint::Nspi, MapiRequestType::GetProps) => {
            nspi_props_response(store, &principal, _body, "GetProps", &request_id).await
        }
        (MapiEndpoint::Nspi, MapiRequestType::GetSpecialTable) => {
            nspi_special_table_response(&request_id)
        }
        (MapiEndpoint::Nspi, MapiRequestType::GetTemplateInfo) => {
            nspi_template_info_response(&principal, &request_id)
        }
        (MapiEndpoint::Nspi, MapiRequestType::ModLinkAtt) => nspi_disabled_mutation_response(
            "ModLinkAtt",
            &request_id,
            "NSPI link-attribute mutation is disabled; LPE address-book data is projected from canonical accounts and contacts.",
        ),
        (MapiEndpoint::Nspi, MapiRequestType::ModProps) => nspi_disabled_mutation_response(
            "ModProps",
            &request_id,
            "NSPI property mutation is disabled; LPE address-book data is projected from canonical accounts and contacts.",
        ),
        (MapiEndpoint::Nspi, MapiRequestType::GetAddressBookUrl) => {
            endpoint_url_response("GetAddressBookUrl", &request_id, headers, "/mapi/nspi/")
        }
        (MapiEndpoint::Nspi, MapiRequestType::GetMailboxUrl) => {
            endpoint_url_response("GetMailboxUrl", &request_id, headers, "/mapi/emsmdb/")
        }
        (MapiEndpoint::Nspi, MapiRequestType::QueryColumns) => {
            nspi_property_tags_response("QueryColumns", &request_id)
        }
        (MapiEndpoint::Nspi, MapiRequestType::QueryRows) => {
            nspi_rowset_response(store, &principal, _body, "QueryRows", &request_id).await
        }
        (MapiEndpoint::Nspi, MapiRequestType::ResolveNames) => {
            resolve_names_response(store, &principal, _body, &request_id).await
        }
        (MapiEndpoint::Nspi, MapiRequestType::ResortRestriction) => {
            nspi_minimal_ids_response("ResortRestriction", &principal, &request_id)
        }
        (MapiEndpoint::Nspi, MapiRequestType::SeekEntries) => {
            nspi_rowset_response(store, &principal, _body, "SeekEntries", &request_id).await
        }
        (MapiEndpoint::Nspi, MapiRequestType::UpdateStat) => nspi_update_stat_response(&request_id),
        (_, MapiRequestType::Ping) => {
            ping_response(endpoint, &principal, headers, _body, &request_id)
        }
        (_, MapiRequestType::Unsupported(value)) => mapi_diagnostic_response(
            &value,
            &request_id,
            5,
            &format!("invalid MAPI X-RequestType header: {value}"),
        ),
        (MapiEndpoint::Emsmdb, other) => mapi_diagnostic_response(
            other.header_value(),
            &request_id,
            5,
            "request type is not valid for the EMSMDB endpoint",
        ),
        (MapiEndpoint::Nspi, other) => mapi_diagnostic_response(
            other.header_value(),
            &request_id,
            5,
            "request type is not valid for the NSPI endpoint",
        ),
    };

    let response = finalize_mapi_response(response, headers);
    log_mapi_connection(
        endpoint,
        &principal,
        headers,
        _body,
        &request_type_label,
        &request_id,
        &response,
    );
    Ok(response)
}

pub(crate) fn mapi_error_response(error: &anyhow::Error) -> Response {
    let message = error.to_string();
    if is_authentication_error(&message) {
        let mut response = StatusCode::UNAUTHORIZED.into_response();
        response.headers_mut().insert(
            WWW_AUTHENTICATE,
            HeaderValue::from_static("Basic realm=\"LPE MAPI\""),
        );
        return response;
    }

    mapi_diagnostic_response("Unknown", "", 4, &message)
}

pub(in crate::mapi) fn connect_response(
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
    write_u32(&mut body, 60_000);
    write_u32(&mut body, 6);
    write_u32(&mut body, 10_000);
    body.extend_from_slice(b"/o=LPE/ou=Exchange Administrative Group/cn=Recipients/cn=\0");
    write_utf16z(&mut body, &principal.display_name);
    let auxiliary_buffer = connect_auxiliary_buffer();
    write_u32(&mut body, auxiliary_buffer.len() as u32);
    body.extend_from_slice(&auxiliary_buffer);
    mapi_response_with_cookies("Connect", request_id, 0, body, cookies)
}

pub(in crate::mapi) fn connect_auxiliary_buffer() -> Vec<u8> {
    let mut buffer = Vec::new();
    write_u16(&mut buffer, 0); // RPC_HEADER_EXT Version
    write_u16(&mut buffer, 0x0004); // Last flag, uncompressed and unobfuscated.
    write_u16(&mut buffer, 0x0008); // Payload size.
    write_u16(&mut buffer, 0x0008); // Uncompressed payload size.
    write_u16(&mut buffer, 0x0008); // AUX_HEADER Size.
    buffer.push(0x01); // AUX_HEADER Version.
    buffer.push(0x17); // AUX_EXORGINFO.
    write_u32(&mut buffer, 0); // OrgFlags: no public folders are published by LPE.
    buffer
}

pub(in crate::mapi) fn disconnect_response(
    endpoint: MapiEndpoint,
    principal: &AccountPrincipal,
    headers: &HeaderMap,
    request_id: &str,
    response_request_type: &str,
) -> Response {
    let Some(session_id) = request_cookie(endpoint, headers) else {
        return mapi_diagnostic_response(
            response_request_type,
            request_id,
            13,
            "missing MAPI session cookie",
        );
    };
    if !request_sequence_cookie_matches(endpoint, headers, &session_id) {
        return mapi_diagnostic_response(
            response_request_type,
            request_id,
            6,
            "invalid MAPI request sequence cookie",
        );
    }
    let Some(_active_request) = begin_active_session_request(&session_id) else {
        return mapi_diagnostic_response(
            response_request_type,
            request_id,
            15,
            "MAPI session already has an active request",
        );
    };
    let Some(session) = remove_session(&session_id) else {
        return mapi_diagnostic_response(
            response_request_type,
            request_id,
            10,
            "MAPI session context not found",
        );
    };
    if session.endpoint != endpoint
        || session.tenant_id != principal.tenant_id
        || session.account_id != principal.account_id
        || session.email != principal.email
    {
        return mapi_diagnostic_response(
            response_request_type,
            request_id,
            10,
            "MAPI authentication context changed",
        );
    }

    let mut body = Vec::new();
    write_u32(&mut body, 0);
    write_u32(&mut body, 0);
    write_u32(&mut body, 0);
    mapi_response_with_cookies(
        response_request_type,
        request_id,
        0,
        body,
        session_context_cookies(endpoint, "", true),
    )
}

pub(in crate::mapi) fn notification_wait_response(
    endpoint: MapiEndpoint,
    principal: &AccountPrincipal,
    headers: &HeaderMap,
    request_id: &str,
) -> Response {
    let Some(session_id) = request_cookie(endpoint, headers) else {
        return mapi_diagnostic_response(
            "NotificationWait",
            request_id,
            13,
            "missing MAPI session cookie",
        );
    };
    if !request_sequence_cookie_matches(endpoint, headers, &session_id) {
        return mapi_diagnostic_response(
            "NotificationWait",
            request_id,
            6,
            "invalid MAPI request sequence cookie",
        );
    }
    let Some(_active_request) = begin_active_session_request(&session_id) else {
        return mapi_diagnostic_response(
            "NotificationWait",
            request_id,
            15,
            "MAPI session already has an active request",
        );
    };
    let Some(session) = remove_session(&session_id) else {
        return mapi_diagnostic_response(
            "NotificationWait",
            request_id,
            10,
            "MAPI session context not found",
        );
    };
    if !session_matches(&session, endpoint, principal) {
        return mapi_diagnostic_response(
            "NotificationWait",
            request_id,
            10,
            "MAPI authentication context changed",
        );
    }

    store_session(session_id.clone(), session);
    let mut body = Vec::new();
    write_u32(&mut body, 0);
    write_u32(&mut body, 0);
    write_u32(&mut body, 0);
    write_u32(&mut body, 0);
    mapi_response_with_cookies(
        "NotificationWait",
        request_id,
        0,
        body,
        session_context_cookies(endpoint, &session_id, false),
    )
}

pub(in crate::mapi) fn ping_response(
    endpoint: MapiEndpoint,
    principal: &AccountPrincipal,
    headers: &HeaderMap,
    body: &[u8],
    request_id: &str,
) -> Response {
    if content_length_header(headers).as_deref() != Some("0") {
        return mapi_diagnostic_response(
            "PING",
            request_id,
            4,
            "PING requests must use Content-Length 0",
        );
    }
    if !body.is_empty() {
        return mapi_diagnostic_response("PING", request_id, 12, "PING request body must be empty");
    }
    let Some(session_id) = request_cookie(endpoint, headers) else {
        return mapi_diagnostic_response("PING", request_id, 13, "missing MAPI session cookie");
    };
    if !request_sequence_cookie_matches(endpoint, headers, &session_id) {
        return mapi_diagnostic_response(
            "PING",
            request_id,
            6,
            "invalid MAPI request sequence cookie",
        );
    }
    let Some(_active_request) = begin_active_session_request(&session_id) else {
        return mapi_diagnostic_response(
            "PING",
            request_id,
            15,
            "MAPI session already has an active request",
        );
    };
    let Some(session) = remove_session(&session_id) else {
        return mapi_diagnostic_response("PING", request_id, 10, "MAPI session context not found");
    };
    if !session_matches(&session, endpoint, principal) {
        return mapi_diagnostic_response(
            "PING",
            request_id,
            10,
            "MAPI authentication context changed",
        );
    }

    store_session(session_id, session);
    mapi_response("PING", request_id, 0, Vec::new(), None)
}

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
        "dntomid" => MapiRequestType::DnToMid,
        "getmatches" => MapiRequestType::GetMatches,
        "getproplist" => MapiRequestType::GetPropList,
        "getprops" => MapiRequestType::GetProps,
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
    let Some(rest) = value.strip_prefix('{') else {
        return false;
    };
    let Some((guid, counter)) = rest.split_once("}:") else {
        return false;
    };
    !counter.is_empty()
        && counter.bytes().all(|byte| byte.is_ascii_digit())
        && Uuid::parse_str(guid).is_ok()
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

pub(in crate::mapi) fn is_mapi_content_type(headers: &HeaderMap) -> bool {
    headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .map(str::trim)
        .is_some_and(|value| {
            value.eq_ignore_ascii_case(MAPI_CONTENT_TYPE)
                || value.eq_ignore_ascii_case(MAPI_OCTET_STREAM_CONTENT_TYPE)
        })
}

pub(in crate::mapi) fn mapi_diagnostic_response(
    request_type: &str,
    request_id: &str,
    response_code: u16,
    message: &str,
) -> Response {
    mapi_response(
        request_type,
        request_id,
        response_code,
        message.as_bytes().to_vec(),
        None,
    )
}

pub(in crate::mapi) fn mapi_response(
    request_type: &str,
    request_id: &str,
    response_code: u16,
    body: Vec<u8>,
    cookie: Option<String>,
) -> Response {
    let cookies = cookie.into_iter().collect();
    mapi_response_with_cookies(request_type, request_id, response_code, body, cookies)
}

pub(in crate::mapi) fn mapi_response_with_cookies(
    request_type: &str,
    request_id: &str,
    response_code: u16,
    body: Vec<u8>,
    cookies: Vec<String>,
) -> Response {
    let mut framed_body = Vec::new();
    framed_body.extend_from_slice(b"PROCESSING\r\n");
    framed_body.extend_from_slice(b"DONE\r\n");
    framed_body.extend_from_slice(format!("X-ResponseCode: {response_code}\r\n").as_bytes());
    framed_body.extend_from_slice(b"X-ElapsedTime: 0\r\n");
    framed_body.extend_from_slice(b"X-StartTime: Mon, 01 Jan 2001 00:00:00 GMT\r\n");
    framed_body.extend_from_slice(b"\r\n");
    framed_body.extend_from_slice(&body);

    let framed_body_len = framed_body.len();
    let mut response = (StatusCode::OK, framed_body).into_response();
    response.extensions_mut().insert(MapiResponseDebug {
        payload_bytes: body.len(),
    });
    let payload_preview_hex = debug_payload_preview_hex(&body);
    if !payload_preview_hex.is_empty() {
        response
            .extensions_mut()
            .insert(MapiResponsePayloadPreview {
                hex: payload_preview_hex,
            });
    }
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static(MAPI_CONTENT_TYPE));
    insert_header(
        &mut response,
        "content-length",
        &framed_body_len.to_string(),
    );
    insert_header(&mut response, "x-requesttype", request_type);
    insert_header(&mut response, "x-responsecode", &response_code.to_string());
    insert_header(&mut response, "x-requestid", request_id);
    insert_header(
        &mut response,
        "x-serverapplication",
        MAPI_SERVER_APPLICATION,
    );
    for cookie in cookies {
        if let Ok(value) = HeaderValue::from_str(&cookie) {
            response.headers_mut().append(SET_COOKIE, value);
        }
    }
    response
}

#[derive(Clone, Copy, Debug)]
pub(in crate::mapi) struct MapiResponseDebug {
    payload_bytes: usize,
}

#[derive(Clone, Debug)]
pub(in crate::mapi) struct MapiResponsePayloadPreview {
    hex: String,
}

pub(crate) fn mapi_response_payload_bytes(response: &Response) -> Option<usize> {
    response
        .extensions()
        .get::<MapiResponseDebug>()
        .map(|debug| debug.payload_bytes)
}

pub(crate) fn mapi_response_payload_preview_hex(response: &Response) -> Option<&str> {
    response
        .extensions()
        .get::<MapiResponsePayloadPreview>()
        .map(|preview| preview.hex.as_str())
}

pub(in crate::mapi) fn finalize_mapi_response(
    mut response: Response,
    request_headers: &HeaderMap,
) -> Response {
    insert_header(
        &mut response,
        "x-expirationinfo",
        &(MAPI_SESSION_MAX_AGE_SECONDS * 1000).to_string(),
    );
    insert_header(&mut response, "x-pendingperiod", "15000");
    if let Some(client_info) = request_headers.get("x-clientinfo") {
        response
            .headers_mut()
            .insert("x-clientinfo", client_info.clone());
    }
    response
}

pub(in crate::mapi) fn log_mapi_connection(
    endpoint: MapiEndpoint,
    principal: &AccountPrincipal,
    headers: &HeaderMap,
    request_body: &[u8],
    request_type: &str,
    request_id: &str,
    response: &Response,
) {
    let response_code = response_header(response, "x-responsecode").unwrap_or_default();
    let status = response.status().as_u16();
    let payload_bytes = mapi_response_payload_bytes(response).unwrap_or(0);
    let request_body_bytes = request_body.len();
    let request_body_preview_hex = debug_payload_preview_hex(request_body);
    let response_payload_preview_hex =
        mapi_response_payload_preview_hex(response).unwrap_or_default();
    let endpoint = match endpoint {
        MapiEndpoint::Emsmdb => "emsmdb",
        MapiEndpoint::Nspi => "nspi",
    };
    let content_type = safe_header(headers, "content-type").unwrap_or_default();
    let user_agent = safe_header(headers, "user-agent").unwrap_or_default();
    let client_request_id = safe_header(headers, "client-request-id").unwrap_or_default();
    let client_info = safe_header(headers, "x-clientinfo").unwrap_or_default();
    let client_application = safe_header(headers, "x-clientapplication").unwrap_or_default();
    let trace_id = safe_header(headers, "x-trace-id").unwrap_or_default();
    let set_cookie_names = response_set_cookie_names(response);
    let message = "rca debug mapi connection";

    if response_code == "0" {
        info!(
            rca_debug = true,
            adapter = "mapi",
            endpoint = endpoint,
            tenant_id = %principal.tenant_id,
            account_id = %principal.account_id,
            mailbox = %principal.email,
            request_type = %request_type,
            mapi_request_id = %request_id,
            client_request_id = %client_request_id,
            client_info = %client_info,
            client_application = %client_application,
            trace_id = %trace_id,
            http_status = status,
            mapi_response_code = %response_code,
            request_body_bytes,
            response_payload_bytes = payload_bytes,
            request_body_preview_hex = %request_body_preview_hex,
            response_payload_preview_hex = %response_payload_preview_hex,
            set_cookie_names = %set_cookie_names,
            content_type = %content_type,
            user_agent = %user_agent,
            "{message}"
        );
    } else {
        warn!(
            rca_debug = true,
            adapter = "mapi",
            endpoint = endpoint,
            tenant_id = %principal.tenant_id,
            account_id = %principal.account_id,
            mailbox = %principal.email,
            request_type = %request_type,
            mapi_request_id = %request_id,
            client_request_id = %client_request_id,
            client_info = %client_info,
            client_application = %client_application,
            trace_id = %trace_id,
            http_status = status,
            mapi_response_code = %response_code,
            request_body_bytes,
            response_payload_bytes = payload_bytes,
            request_body_preview_hex = %request_body_preview_hex,
            response_payload_preview_hex = %response_payload_preview_hex,
            set_cookie_names = %set_cookie_names,
            content_type = %content_type,
            user_agent = %user_agent,
            "{message}"
        );
    }
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
    bytes
        .iter()
        .take(limit)
        .map(|byte| format!("{byte:02x}"))
        .collect::<Vec<_>>()
        .join("")
}

pub(in crate::mapi) fn execute_success_body(
    rop_buffer: Vec<u8>,
    auxiliary_buffer: Vec<u8>,
) -> Vec<u8> {
    let mut body = Vec::new();
    write_u32(&mut body, 0);
    write_u32(&mut body, 0);
    write_u32(&mut body, 0);
    write_u32(&mut body, rop_buffer.len() as u32);
    body.extend_from_slice(&rop_buffer);
    write_u32(&mut body, auxiliary_buffer.len() as u32);
    body.extend_from_slice(&auxiliary_buffer);
    body
}

pub(in crate::mapi) fn execute_failure_response(
    request_id: &str,
    status_code: u32,
    message: &str,
    cookie: Option<String>,
) -> Response {
    let mut body = Vec::new();
    write_u32(&mut body, status_code);
    write_u32(&mut body, message.len() as u32);
    body.extend_from_slice(message.as_bytes());
    mapi_response("Execute", request_id, status_code as u16, body, cookie)
}

pub(in crate::mapi) fn insert_header(response: &mut Response, name: &'static str, value: &str) {
    if let Ok(value) = HeaderValue::from_str(value) {
        response.headers_mut().insert(name, value);
    }
}

pub(in crate::mapi) fn request_cookie(
    endpoint: MapiEndpoint,
    headers: &HeaderMap,
) -> Option<String> {
    request_named_cookie(cookie_name(endpoint), headers)
}

pub(in crate::mapi) fn request_sequence_cookie(
    endpoint: MapiEndpoint,
    headers: &HeaderMap,
) -> Option<String> {
    request_named_cookie(sequence_cookie_name(endpoint), headers)
}

pub(in crate::mapi) fn request_sequence_cookie_matches(
    endpoint: MapiEndpoint,
    headers: &HeaderMap,
    session_id: &str,
) -> bool {
    match request_sequence_cookie(endpoint, headers) {
        Some(sequence_id) => sequence_id == session_id,
        None => true,
    }
}

pub(in crate::mapi) fn request_named_cookie(name: &str, headers: &HeaderMap) -> Option<String> {
    headers
        .get("cookie")
        .and_then(|value| value.to_str().ok())
        .and_then(|cookie| {
            cookie.split(';').find_map(|part| {
                let (key, value) = part.trim().split_once('=')?;
                (key == name && !value.is_empty()).then(|| value.to_string())
            })
        })
}

pub(in crate::mapi) fn session_cookie(
    endpoint: MapiEndpoint,
    session_id: &str,
    expired: bool,
) -> String {
    context_cookie(endpoint, cookie_name(endpoint), session_id, expired)
}

pub(in crate::mapi) fn sequence_cookie(
    endpoint: MapiEndpoint,
    session_id: &str,
    expired: bool,
) -> String {
    context_cookie(
        endpoint,
        sequence_cookie_name(endpoint),
        session_id,
        expired,
    )
}

pub(in crate::mapi) fn session_context_cookies(
    endpoint: MapiEndpoint,
    session_id: &str,
    expired: bool,
) -> Vec<String> {
    vec![
        session_cookie(endpoint, session_id, expired),
        sequence_cookie(endpoint, session_id, expired),
    ]
}

pub(in crate::mapi) fn context_cookie(
    endpoint: MapiEndpoint,
    name: &str,
    session_id: &str,
    expired: bool,
) -> String {
    let path = cookie_path(endpoint);
    if expired {
        format!("{name}=; Path={path}; Max-Age=0; HttpOnly; SameSite=Lax; Secure")
    } else {
        format!(
            "{name}={session_id}; Path={path}; Max-Age={MAPI_SESSION_MAX_AGE_SECONDS}; HttpOnly; SameSite=Lax; Secure"
        )
    }
}

pub(in crate::mapi) fn cookie_name(endpoint: MapiEndpoint) -> &'static str {
    match endpoint {
        MapiEndpoint::Emsmdb => EMSMDB_COOKIE,
        MapiEndpoint::Nspi => NSPI_COOKIE,
    }
}

pub(in crate::mapi) fn sequence_cookie_name(endpoint: MapiEndpoint) -> &'static str {
    match endpoint {
        MapiEndpoint::Emsmdb => EMSMDB_SEQUENCE_COOKIE,
        MapiEndpoint::Nspi => NSPI_SEQUENCE_COOKIE,
    }
}

pub(in crate::mapi) fn cookie_path(endpoint: MapiEndpoint) -> &'static str {
    match endpoint {
        MapiEndpoint::Emsmdb => EMSMDB_COOKIE_PATH,
        MapiEndpoint::Nspi => NSPI_COOKIE_PATH,
    }
}

pub(in crate::mapi) fn is_authentication_error(message: &str) -> bool {
    matches!(
        message,
        "missing account authentication" | "invalid credentials"
    ) || message.contains("oauth access token")
}

impl MapiRequestType {
    pub(in crate::mapi) fn header_value(&self) -> &str {
        match self {
            MapiRequestType::Connect => "Connect",
            MapiRequestType::Disconnect => "Disconnect",
            MapiRequestType::Execute => "Execute",
            MapiRequestType::NotificationWait => "NotificationWait",
            MapiRequestType::Bind => "Bind",
            MapiRequestType::Unbind => "Unbind",
            MapiRequestType::CompareMids => "CompareMIds",
            MapiRequestType::DnToMid => "DNToMId",
            MapiRequestType::GetMatches => "GetMatches",
            MapiRequestType::GetPropList => "GetPropList",
            MapiRequestType::GetProps => "GetProps",
            MapiRequestType::GetSpecialTable => "GetSpecialTable",
            MapiRequestType::GetTemplateInfo => "GetTemplateInfo",
            MapiRequestType::ModLinkAtt => "ModLinkAtt",
            MapiRequestType::ModProps => "ModProps",
            MapiRequestType::GetAddressBookUrl => "GetAddressBookUrl",
            MapiRequestType::GetMailboxUrl => "GetMailboxUrl",
            MapiRequestType::QueryColumns => "QueryColumns",
            MapiRequestType::QueryRows => "QueryRows",
            MapiRequestType::ResolveNames => "ResolveNames",
            MapiRequestType::ResortRestriction => "ResortRestriction",
            MapiRequestType::SeekEntries => "SeekEntries",
            MapiRequestType::UpdateStat => "UpdateStat",
            MapiRequestType::Ping => "PING",
            MapiRequestType::Unsupported(value) => value,
        }
    }

    pub(in crate::mapi) fn requires_nspi_session(&self) -> bool {
        matches!(
            self,
            MapiRequestType::CompareMids
                | MapiRequestType::DnToMid
                | MapiRequestType::GetMatches
                | MapiRequestType::GetPropList
                | MapiRequestType::GetProps
                | MapiRequestType::GetSpecialTable
                | MapiRequestType::GetTemplateInfo
                | MapiRequestType::ModLinkAtt
                | MapiRequestType::ModProps
                | MapiRequestType::GetAddressBookUrl
                | MapiRequestType::GetMailboxUrl
                | MapiRequestType::QueryColumns
                | MapiRequestType::QueryRows
                | MapiRequestType::ResolveNames
                | MapiRequestType::ResortRestriction
                | MapiRequestType::SeekEntries
                | MapiRequestType::UpdateStat
        )
    }
}
