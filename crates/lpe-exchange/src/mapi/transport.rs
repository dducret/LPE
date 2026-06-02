use super::dispatch::*;
use super::identity::{
    ARCHIVE_FOLDER_ID, CALENDAR_FOLDER_ID, COMMON_VIEWS_FOLDER_ID, CONFLICTS_FOLDER_ID,
    CONTACTS_FOLDER_ID, CONTACTS_SEARCH_FOLDER_ID, CONVERSATION_ACTION_SETTINGS_FOLDER_ID,
    CONVERSATION_HISTORY_FOLDER_ID, DEFERRED_ACTION_FOLDER_ID, DRAFTS_FOLDER_ID,
    FREEBUSY_DATA_FOLDER_ID, INBOX_FOLDER_ID, IPM_SUBTREE_FOLDER_ID, JOURNAL_FOLDER_ID,
    JUNK_FOLDER_ID, LOCAL_FAILURES_FOLDER_ID, NOTES_FOLDER_ID, OUTBOX_FOLDER_ID,
    REMINDERS_FOLDER_ID, ROOT_FOLDER_ID, RSS_FEEDS_FOLDER_ID, SCHEDULE_FOLDER_ID, SEARCH_FOLDER_ID,
    SENT_FOLDER_ID, SERVER_FAILURES_FOLDER_ID, SHORTCUTS_FOLDER_ID, SPOOLER_QUEUE_FOLDER_ID,
    STORE_REPLICA_GUID, SUGGESTED_CONTACTS_FOLDER_ID, SYNC_ISSUES_FOLDER_ID, TASKS_FOLDER_ID,
    TODO_SEARCH_FOLDER_ID, TRACKED_MAIL_PROCESSING_FOLDER_ID, TRASH_FOLDER_ID, VIEWS_FOLDER_ID,
};
use super::notifications::*;
use super::nspi::*;
use super::rop::*;
use super::session::*;
use super::wire::MapiHttpRequestType as MapiRequestType;
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
    if request_type != MapiRequestType::Ping && !content_length_matches_body(&content_length, _body)
    {
        let response = mapi_diagnostic_response(
            &request_type_label,
            &request_id,
            4,
            "MAPI Content-Length header does not match request body length",
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
            "MAPI requests must use Content-Type application/mapi-http or application/octet-stream.",
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
                    log_session_cookie_lookup(endpoint, &principal, headers, &request_type_label);
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
            notification_wait_response(store, endpoint, &principal, headers, &request_id).await
        }
        (_, MapiRequestType::Ping) => {
            ping_response(endpoint, &principal, headers, _body, &request_id)
        }
        (MapiEndpoint::Nspi, request_type) => {
            handle_nspi_request(store, &principal, headers, _body, request_type, &request_id).await
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
    let (session_id, reconnected) =
        match reconnect_session(endpoint, principal, headers, "Connect", request_id) {
            Ok(Some(session_id)) => (session_id, true),
            Ok(None) => (
                create_session(endpoint, principal, "Connect", request_id),
                false,
            ),
            Err(response) => return response,
        };
    log_mapi_session_establish(
        endpoint,
        principal,
        headers,
        "Connect",
        request_id,
        &session_id,
        reconnected,
    );
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
    log_connect_body_debug(endpoint, principal, request_id, &body);
    mapi_response_with_cookies("Connect", request_id, 0, body, cookies)
}

#[derive(Debug, Default)]
struct ConnectBodyDebugSummary {
    status_code: u32,
    error_code: u32,
    polls_max: u32,
    retry_count: u32,
    retry_delay_ms: u32,
    dn_prefix: String,
    display_name: String,
    auxiliary_buffer_bytes: u32,
    parse_error: String,
}

fn log_connect_body_debug(
    endpoint: MapiEndpoint,
    principal: &AccountPrincipal,
    request_id: &str,
    body: &[u8],
) {
    let summary = summarize_connect_body(body);
    let endpoint = match endpoint {
        MapiEndpoint::Emsmdb => "emsmdb",
        MapiEndpoint::Nspi => "nspi",
    };

    tracing::info!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = endpoint,
        tenant_id = %principal.tenant_id,
        account_id = %principal.account_id,
        mailbox = %principal.email,
        request_type = "Connect",
        mapi_request_id = request_id,
        connect_status_code = summary.status_code,
        connect_error_code = summary.error_code,
        connect_polls_max = summary.polls_max,
        connect_retry_count = summary.retry_count,
        connect_retry_delay_ms = summary.retry_delay_ms,
        connect_dn_prefix = %summary.dn_prefix,
        connect_display_name = %summary.display_name,
        connect_auxiliary_buffer_bytes = summary.auxiliary_buffer_bytes,
        connect_body_bytes = body.len(),
        connect_parse_error = %summary.parse_error,
        message = "rca debug mapi connect body",
    );
}

fn summarize_connect_body(body: &[u8]) -> ConnectBodyDebugSummary {
    let mut cursor = Cursor::new(body);
    let mut summary = ConnectBodyDebugSummary::default();
    let result = (|| -> Result<()> {
        summary.status_code = cursor.read_u32()?;
        summary.error_code = cursor.read_u32()?;
        summary.polls_max = cursor.read_u32()?;
        summary.retry_count = cursor.read_u32()?;
        summary.retry_delay_ms = cursor.read_u32()?;
        summary.dn_prefix = cursor.read_ascii_z()?;
        summary.display_name = cursor.read_utf16z()?;
        summary.auxiliary_buffer_bytes = cursor.read_u32()?;
        let auxiliary_buffer_bytes = summary.auxiliary_buffer_bytes as usize;
        cursor.read_bytes(auxiliary_buffer_bytes)?;
        Ok(())
    })();
    if let Err(error) = result {
        summary.parse_error = error.to_string();
    }
    summary
}

pub(in crate::mapi) fn log_mapi_session_establish(
    endpoint: MapiEndpoint,
    principal: &AccountPrincipal,
    headers: &HeaderMap,
    request_type: &str,
    request_id: &str,
    session_id: &str,
    reconnected: bool,
) {
    let endpoint_label = match endpoint {
        MapiEndpoint::Emsmdb => "emsmdb",
        MapiEndpoint::Nspi => "nspi",
    };
    let session_cookie_debug = cookie_value_debug(Some(session_id));
    let client_application = safe_header(headers, "x-clientapplication").unwrap_or_default();
    let client_request_id = safe_header(headers, "client-request-id").unwrap_or_default();
    let client_info = safe_header(headers, "x-clientinfo").unwrap_or_default();
    let trace_id = safe_header(headers, "x-trace-id").unwrap_or_default();
    let x_request_id = safe_header(headers, "x-request-id").unwrap_or_default();
    let user_agent = safe_header(headers, "user-agent").unwrap_or_default();
    let host = safe_header(headers, "host").unwrap_or_default();
    let content_type = safe_header(headers, "content-type").unwrap_or_default();
    let content_length = safe_header(headers, "content-length").unwrap_or_default();
    let store_replica_guid = Uuid::from_bytes(STORE_REPLICA_GUID);
    let store_replica_guid_hex = hex_preview(&STORE_REPLICA_GUID, STORE_REPLICA_GUID.len());

    tracing::info!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = endpoint_label,
        tenant_id = %principal.tenant_id,
        account_id = %principal.account_id,
        mailbox = %principal.email,
        request_type = %request_type,
        mapi_request_id = %request_id,
        session_reconnected = reconnected,
        session_id_suffix = %session_cookie_debug.suffix,
        session_id_hash = %session_cookie_debug.hash,
        mailbox_guid = %principal.account_id,
        store_replica_guid = %store_replica_guid,
        store_replica_guid_hex = %store_replica_guid_hex,
        mapping_signature_source = "store_replica_guid",
        client_application = %client_application,
        client_request_id = %client_request_id,
        client_info = %client_info,
        trace_id = %trace_id,
        x_request_id = %x_request_id,
        user_agent = %user_agent,
        host = %host,
        content_type = %content_type,
        content_length = %content_length,
        message = "rca debug mapi session establish",
    );
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
    log_session_cookie_lookup(endpoint, principal, headers, response_request_type);
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
    let Some(mut session) = remove_session(&session_id) else {
        if endpoint == MapiEndpoint::Nspi && response_request_type == "Unbind" {
            return disconnect_success_response(endpoint, request_id, response_request_type);
        }
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
    session.record_transport_request(response_request_type, request_id);

    log_mapi_session_disconnect(
        endpoint,
        principal,
        headers,
        &session_id,
        &session,
        request_id,
        response_request_type,
    );

    disconnect_success_response(endpoint, request_id, response_request_type)
}

fn disconnect_success_response(
    endpoint: MapiEndpoint,
    request_id: &str,
    response_request_type: &str,
) -> Response {
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

fn log_mapi_session_disconnect(
    endpoint: MapiEndpoint,
    principal: &AccountPrincipal,
    headers: &HeaderMap,
    session_id: &str,
    session: &MapiSession,
    request_id: &str,
    request_type: &str,
) {
    let endpoint_label = match endpoint {
        MapiEndpoint::Emsmdb => "emsmdb",
        MapiEndpoint::Nspi => "nspi",
    };
    let sync_source_summaries = session
        .handles
        .iter()
        .filter_map(|(handle, object)| match object {
            MapiObject::SynchronizationSource {
                folder_id,
                mailbox_id,
                checkpoint_kind,
                checkpoint_change_sequence,
                checkpoint_modseq,
                sync_type,
                state,
                state_upload_buffer,
                client_state_uploaded_bytes,
                client_state_uploaded_marker_mask,
                incremental_transfer_buffer,
                transfer_buffer,
                transfer_position,
                ..
            } => Some(format!(
                "handle={handle};folder=0x{folder_id:016x};sync=0x{sync_type:02x};kind={};mailbox={};seq={checkpoint_change_sequence};modseq={checkpoint_modseq};state={};client_state={};marker_mask=0x{:02x};upload_buffer={};transfer={}/{};incremental={}",
                checkpoint_kind.as_str(),
                mailbox_id.map(|id| id.to_string()).unwrap_or_default(),
                state.len(),
                client_state_uploaded_bytes,
                client_state_uploaded_marker_mask,
                state_upload_buffer.len(),
                transfer_position,
                transfer_buffer.len(),
                incremental_transfer_buffer.is_some(),
            )),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("|");
    let live_handle_summaries = session
        .handles
        .iter()
        .map(|(handle, object)| {
            let folder = object
                .folder_id()
                .map(|folder_id| {
                    format!(
                        "folder=0x{folder_id:016x};role={};container={}",
                        debug_role_for_folder_id(folder_id),
                        debug_container_class_for_folder_id(folder_id)
                    )
                })
                .unwrap_or_else(|| "folder=;role=;container=".to_string());
            format!(
                "handle={handle};kind={};{}",
                mapi_object_debug_kind(object),
                folder
            )
        })
        .collect::<Vec<_>>()
        .join("|");
    let mut hierarchy_sync_source_count = 0usize;
    let mut content_sync_source_count = 0usize;
    let mut read_state_sync_source_count = 0usize;
    let mut completed_sync_source_count = 0usize;
    let mut completed_hierarchy_sync_source_count = 0usize;
    let mut completed_content_sync_source_count = 0usize;
    let mut incomplete_sync_source_count = 0usize;
    let mut total_transfer_buffer_bytes = 0usize;
    let mut total_transfer_position_bytes = 0usize;
    for object in session.handles.values() {
        let MapiObject::SynchronizationSource {
            sync_type,
            transfer_buffer,
            transfer_position,
            ..
        } = object
        else {
            continue;
        };
        match *sync_type {
            0x01 => content_sync_source_count += 1,
            0x02 => hierarchy_sync_source_count += 1,
            0x03 => read_state_sync_source_count += 1,
            _ => {}
        }
        total_transfer_buffer_bytes += transfer_buffer.len();
        total_transfer_position_bytes += *transfer_position;
        let completed = *transfer_position >= transfer_buffer.len();
        if completed {
            completed_sync_source_count += 1;
            match *sync_type {
                0x01 => completed_content_sync_source_count += 1,
                0x02 => completed_hierarchy_sync_source_count += 1,
                _ => {}
            }
        } else {
            incomplete_sync_source_count += 1;
        }
    }
    let sync_source_count = session
        .handles
        .values()
        .filter(|object| matches!(object, MapiObject::SynchronizationSource { .. }))
        .count();
    let sync_collector_count = session
        .handles
        .values()
        .filter(|object| matches!(object, MapiObject::SynchronizationCollector { .. }))
        .count();
    let notification_subscription_count = session
        .handles
        .values()
        .filter(|object| matches!(object, MapiObject::NotificationSubscription { .. }))
        .count();
    let post_hierarchy_summary = post_hierarchy_action_summary(
        session,
        endpoint == MapiEndpoint::Emsmdb && request_type == "Disconnect",
    );
    let client_application = safe_header(headers, "x-clientapplication").unwrap_or_default();
    let trace_id = safe_header(headers, "x-trace-id").unwrap_or_default();
    let client_request_id = safe_header(headers, "client-request-id").unwrap_or_default();
    let client_info = safe_header(headers, "x-clientinfo").unwrap_or_default();
    let (request_guid, request_counter) = guid_counter_debug(request_id);
    let (client_info_guid, client_info_counter) = guid_counter_debug(&client_info);
    let client_flow_key = client_flow_key(&client_info);
    let user_agent = safe_header(headers, "user-agent").unwrap_or_default();
    let host = safe_header(headers, "host").unwrap_or_default();
    let session_cookie_debug = cookie_value_debug(Some(session_id));
    let session_age_ms = SystemTime::now()
        .duration_since(session.created_at)
        .map(|duration| duration.as_millis())
        .unwrap_or_default();
    let completed_sync_checkpoint_summaries = session
        .post_hierarchy_actions
        .completed_sync_checkpoint_summaries
        .join("|");
    let logon_identity = session.logon_identity.clone().unwrap_or_default();
    let recent_execute_summaries = recent_execute_debug_summaries(session, 8);
    let special_folder_contract_summary = special_folder_contract_summary(session);
    let all_sync_sources_completed = sync_source_count == completed_sync_source_count;
    let partial_scope_checkpoint_not_stored_count =
        partial_scope_checkpoint_not_stored_count(&session.post_hierarchy_actions);
    let partial_scope_checkpoint_not_stored_expected =
        partial_scope_checkpoint_not_stored_count > 0 && all_sync_sources_completed;
    let clean_client_close_after_sync = endpoint == MapiEndpoint::Emsmdb
        && request_type == "Disconnect"
        && post_hierarchy_summary.content_sync_configure_observed
        && all_sync_sources_completed;
    let nspi_address_book_probe_only = endpoint == MapiEndpoint::Nspi
        && request_type == "Unbind"
        && session.execute_request_count == 0
        && session.request_count <= 4
        && session.handles.is_empty()
        && sync_source_count == 0
        && notification_subscription_count == 0;
    let outlook_profile_stage = if clean_client_close_after_sync {
        "emsmdb_store_sync_completed"
    } else if nspi_address_book_probe_only {
        "nspi_address_book_probe_only_no_emsmdb_in_session"
    } else if endpoint == MapiEndpoint::Nspi {
        "nspi_address_book_session"
    } else if endpoint == MapiEndpoint::Emsmdb && session.logon_identity.is_some() {
        "emsmdb_store_session"
    } else {
        "mapi_session"
    };
    let next_expected_client_step = if nspi_address_book_probe_only {
        "client_may_open_emsmdb_connect_or_stop_due_to_profile_selection"
    } else if clean_client_close_after_sync {
        "client_reconnect_or_idle"
    } else if endpoint == MapiEndpoint::Emsmdb && session.logon_identity.is_none() {
        "emsmdb_logon"
    } else {
        "client_next_request"
    };

    tracing::info!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = endpoint_label,
        tenant_id = %principal.tenant_id,
        account_id = %principal.account_id,
        mailbox = %principal.email,
        request_type = %request_type,
        mapi_request_id = %request_id,
        session_id_suffix = %session_cookie_debug.suffix,
        session_id_hash = %session_cookie_debug.hash,
        session_age_ms,
        session_request_count = session.request_count,
        session_execute_request_count = session.execute_request_count,
        session_first_request_type = %session.first_request_type,
        session_first_request_id = %session.first_request_id,
        session_last_request_type = %session.last_request_type,
        session_last_request_id = %session.last_request_id,
        logon_mailbox_guid = %logon_identity.mailbox_guid,
        logon_replid = %logon_identity.replid,
        logon_replica_guid = %logon_identity.replica_guid,
        logon_response_flags = %logon_identity.response_flags,
        logon_special_folder_ids = %logon_identity.special_folder_ids,
        expected_mailbox_guid = %principal.account_id,
        expected_replica_guid = %hex_preview(&STORE_REPLICA_GUID, STORE_REPLICA_GUID.len()),
        logon_identity_matches_session =
            logon_identity.mailbox_guid == principal.account_id.to_string()
                && logon_identity.replica_guid
                    == hex_preview(&STORE_REPLICA_GUID, STORE_REPLICA_GUID.len()),
        client_request_id = %client_request_id,
        client_application = %client_application,
        client_info = %client_info,
        client_flow_key = %client_flow_key,
        request_guid = %request_guid,
        request_counter = %request_counter,
        client_info_guid = %client_info_guid,
        client_info_counter = %client_info_counter,
        user_agent = %user_agent,
        host = %host,
        handle_count = session.handles.len(),
        sync_source_count,
        sync_collector_count,
        notification_subscription_count,
        pending_notification_count = session.pending_notifications.len(),
        completed_execute_request_count = session.completed_execute_requests.len(),
        hierarchy_sync_source_count,
        content_sync_source_count,
        read_state_sync_source_count,
        completed_sync_source_count,
        completed_hierarchy_sync_source_count,
        completed_content_sync_source_count,
        incomplete_sync_source_count,
        total_transfer_buffer_bytes,
        total_transfer_position_bytes,
        completed_hierarchy_without_content_sync =
            completed_hierarchy_sync_source_count > 0 && content_sync_source_count == 0,
        post_hierarchy_execute_count = post_hierarchy_summary.execute_count,
        post_hierarchy_rop_ids_seen = %post_hierarchy_summary.rop_ids_seen,
        post_hierarchy_content_sync_configure_observed =
            post_hierarchy_summary.content_sync_configure_observed,
        post_hierarchy_release_client_initiated =
            post_hierarchy_summary.release_client_initiated,
        post_hierarchy_logoff_client_initiated =
            post_hierarchy_summary.logoff_client_initiated,
        post_hierarchy_disconnect_client_initiated =
            post_hierarchy_summary.disconnect_client_initiated,
        post_hierarchy_close_kind = %post_hierarchy_summary.close_kind,
        post_hierarchy_last_completed_sync_root =
            %post_hierarchy_summary.last_completed_hierarchy_sync_root,
        post_hierarchy_last_get_buffer_summary =
            %post_hierarchy_summary.last_successful_hierarchy_get_buffer_summary,
        sync_source_summaries = %sync_source_summaries,
        live_handle_summaries = %live_handle_summaries,
        special_folder_contract_summary = %special_folder_contract_summary,
        completed_sync_checkpoint_summaries = %completed_sync_checkpoint_summaries,
        partial_scope_checkpoint_not_stored_count,
        partial_scope_checkpoint_not_stored_expected,
        nspi_address_book_probe_only,
        outlook_profile_stage = %outlook_profile_stage,
        next_expected_client_step = %next_expected_client_step,
        recent_execute_summaries = %recent_execute_summaries,
        "rca debug mapi session disconnect"
    );
    tracing::info!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = endpoint_label,
        tenant_id = %principal.tenant_id,
        account_id = %principal.account_id,
        mailbox = %principal.email,
        request_type = %request_type,
        mapi_request_id = %request_id,
        session_id_suffix = %session_cookie_debug.suffix,
        session_id_hash = %session_cookie_debug.hash,
        logon_mailbox_guid = %logon_identity.mailbox_guid,
        logon_replica_guid = %logon_identity.replica_guid,
        logon_identity_matches_session =
            logon_identity.mailbox_guid == principal.account_id.to_string()
                && logon_identity.replica_guid
                    == hex_preview(&STORE_REPLICA_GUID, STORE_REPLICA_GUID.len()),
        client_application = %client_application,
        trace_id = %trace_id,
        client_request_id = %client_request_id,
        client_info = %client_info,
        client_flow_key = %client_flow_key,
        request_guid = %request_guid,
        request_counter = %request_counter,
        client_info_guid = %client_info_guid,
        client_info_counter = %client_info_counter,
        user_agent = %user_agent,
        host = %host,
        response_status_code = 0u32,
        response_error_code = 0u32,
        response_auxiliary_buffer_size = 0u32,
        response_body_bytes = 12usize,
        response_body_hex = "000000000000000000000000",
        response_content_type = MAPI_CONTENT_TYPE,
        response_x_response_code = 0u16,
        response_clears_session_context_cookie = true,
        response_set_cookie_count = 2usize,
        response_set_cookie_names =
            %format!("{},{}", cookie_name(endpoint), sequence_cookie_name(endpoint)),
        session_removed_before_response = true,
        live_handle_count_before_remove = session.handles.len(),
        completed_execute_request_count = session.completed_execute_requests.len(),
        recent_execute_summaries = %recent_execute_summaries,
        completed_sync_source_count,
        incomplete_sync_source_count,
        all_sync_sources_completed,
        clean_client_close_after_sync,
        post_hierarchy_execute_count = post_hierarchy_summary.execute_count,
        post_hierarchy_content_sync_configure_observed =
            post_hierarchy_summary.content_sync_configure_observed,
        post_hierarchy_disconnect_client_initiated =
            post_hierarchy_summary.disconnect_client_initiated,
        post_hierarchy_close_kind = %post_hierarchy_summary.close_kind,
        post_hierarchy_last_completed_sync_root =
            %post_hierarchy_summary.last_completed_hierarchy_sync_root,
        post_hierarchy_last_get_buffer_summary =
            %post_hierarchy_summary.last_successful_hierarchy_get_buffer_summary,
        special_folder_contract_summary = %special_folder_contract_summary,
        completed_sync_checkpoint_summaries = %completed_sync_checkpoint_summaries,
        partial_scope_checkpoint_not_stored_count,
        partial_scope_checkpoint_not_stored_expected,
        nspi_address_book_probe_only,
        outlook_profile_stage = %outlook_profile_stage,
        next_expected_client_step = %next_expected_client_step,
        "rca debug mapi disconnect wire contract"
    );
    tracing::info!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = endpoint_label,
        tenant_id = %principal.tenant_id,
        account_id = %principal.account_id,
        mailbox = %principal.email,
        request_type = %request_type,
        mapi_request_id = %request_id,
        session_id_suffix = %session_cookie_debug.suffix,
        session_id_hash = %session_cookie_debug.hash,
        logon_mailbox_guid = %logon_identity.mailbox_guid,
        logon_replica_guid = %logon_identity.replica_guid,
        logon_special_folder_ids = %logon_identity.special_folder_ids,
        logon_identity_matches_session =
            logon_identity.mailbox_guid == principal.account_id.to_string()
                && logon_identity.replica_guid
                    == hex_preview(&STORE_REPLICA_GUID, STORE_REPLICA_GUID.len()),
        client_flow_key = %client_flow_key,
        request_guid = %request_guid,
        request_counter = %request_counter,
        client_info_guid = %client_info_guid,
        client_info_counter = %client_info_counter,
        transport_contract_ok = true,
        response_body_contract_ok = true,
        cookies_invalidated = true,
        all_sync_sources_completed,
        clean_client_close_after_sync,
        post_hierarchy_close_kind = %post_hierarchy_summary.close_kind,
        next_debug_focus =
            if clean_client_close_after_sync {
                "outlook_reconnect_or_client_side_reason"
            } else if incomplete_sync_source_count > 0 {
                "unfinished_sync_source"
            } else if partial_scope_checkpoint_not_stored_count > 0 {
                "partial_scope_checkpoint_not_stored_is_expected_when_sources_completed"
            } else {
                "post_hierarchy_sequence"
            },
        recent_execute_summaries = %recent_execute_summaries,
        special_folder_contract_summary = %special_folder_contract_summary,
        completed_sync_checkpoint_summaries = %completed_sync_checkpoint_summaries,
        partial_scope_checkpoint_not_stored_count,
        partial_scope_checkpoint_not_stored_expected,
        nspi_address_book_probe_only,
        outlook_profile_stage = %outlook_profile_stage,
        next_expected_client_step = %next_expected_client_step,
        "rca debug mapi disconnect verdict"
    );

    if incomplete_sync_source_count > 0 {
        tracing::warn!(
            rca_debug = true,
            rca_warning = "disconnect_with_incomplete_sync_source",
            adapter = "mapi",
            endpoint = endpoint_label,
            tenant_id = %principal.tenant_id,
            account_id = %principal.account_id,
            mailbox = %principal.email,
            request_type = %request_type,
            mapi_request_id = %request_id,
            incomplete_sync_source_count,
            total_transfer_buffer_bytes,
            total_transfer_position_bytes,
            sync_source_summaries = %sync_source_summaries,
            recent_execute_summaries = %recent_execute_summaries,
            "rca debug mapi disconnect with incomplete sync source"
        );
    }

    if endpoint == MapiEndpoint::Emsmdb
        && request_type == "Disconnect"
        && session
            .post_hierarchy_actions
            .last_completed_hierarchy_sync_root
            .is_some()
        && !session
            .post_hierarchy_actions
            .content_sync_configure_observed
    {
        tracing::warn!(
            rca_debug = true,
            rca_warning = %post_hierarchy_summary.close_kind,
            adapter = "mapi",
            endpoint = endpoint_label,
            tenant_id = %principal.tenant_id,
            account_id = %principal.account_id,
            mailbox = %principal.email,
            request_type = %request_type,
            mapi_request_id = %request_id,
            client_application = %client_application,
            trace_id = %trace_id,
            post_hierarchy_execute_count = session.post_hierarchy_actions.execute_count,
            post_hierarchy_rop_ids_seen =
                %format_rop_ids_for_debug(&session.post_hierarchy_actions.rop_ids_seen),
            post_hierarchy_bootstrap_probe_observed =
                session.post_hierarchy_actions.bootstrap_probe_observed,
            post_hierarchy_set_properties_probe_observed =
                session.post_hierarchy_actions.set_properties_probe_observed,
            post_hierarchy_release_client_initiated =
                session.post_hierarchy_actions.release_client_initiated,
            post_hierarchy_logoff_client_initiated =
                session.post_hierarchy_actions.logoff_client_initiated,
            post_hierarchy_close_kind = %post_hierarchy_summary.close_kind,
            post_hierarchy_last_completed_sync_root =
                %post_hierarchy_summary.last_completed_hierarchy_sync_root,
            post_hierarchy_last_get_buffer_summary =
                %post_hierarchy_summary.last_successful_hierarchy_get_buffer_summary,
            sync_source_summaries = %sync_source_summaries,
            live_handle_summaries = %live_handle_summaries,
            "rca debug mapi post hierarchy disconnect before content sync"
        );
    }
}

fn recent_execute_debug_summaries(session: &MapiSession, limit: usize) -> String {
    let mut entries = session
        .completed_execute_request_order
        .iter()
        .rev()
        .take(limit)
        .filter_map(|request_id| {
            let cached = session.completed_execute_requests.get(request_id)?;
            Some(format!(
                "id={};req={};resp={};rv={};resp_rop_bytes={};body_bytes={}",
                request_id,
                cached.request_rop_ids,
                cached.response_rop_ids,
                cached.response_rop_results,
                cached.response_rop_buffer_bytes,
                cached.response_body.len()
            ))
        })
        .collect::<Vec<_>>();
    entries.reverse();
    entries.join("|")
}

fn special_folder_contract_summary(session: &MapiSession) -> String {
    const SPECIAL_FOLDERS: &[(&str, u64, &str)] = &[
        ("root", ROOT_FOLDER_ID, "logon"),
        ("deferred_action", DEFERRED_ACTION_FOLDER_ID, "logon"),
        ("spooler_queue", SPOOLER_QUEUE_FOLDER_ID, "logon"),
        ("ipm_subtree", IPM_SUBTREE_FOLDER_ID, "logon"),
        ("inbox", INBOX_FOLDER_ID, "logon"),
        ("outbox", OUTBOX_FOLDER_ID, "logon"),
        ("sent", SENT_FOLDER_ID, "logon"),
        ("trash", TRASH_FOLDER_ID, "logon"),
        ("common_views", COMMON_VIEWS_FOLDER_ID, "logon"),
        ("schedule", SCHEDULE_FOLDER_ID, "logon"),
        ("search", SEARCH_FOLDER_ID, "logon"),
        ("personal_views", VIEWS_FOLDER_ID, "logon"),
        ("shortcuts", SHORTCUTS_FOLDER_ID, "logon"),
        ("drafts", DRAFTS_FOLDER_ID, "default_ipm"),
        ("contacts", CONTACTS_FOLDER_ID, "default_ipm"),
        ("calendar", CALENDAR_FOLDER_ID, "default_ipm"),
        ("journal", JOURNAL_FOLDER_ID, "default_ipm"),
        ("notes", NOTES_FOLDER_ID, "default_ipm"),
        ("tasks", TASKS_FOLDER_ID, "default_ipm"),
        ("reminders", REMINDERS_FOLDER_ID, "search"),
        (
            "suggested_contacts",
            SUGGESTED_CONTACTS_FOLDER_ID,
            "additional_ren",
        ),
        ("contacts_search", CONTACTS_SEARCH_FOLDER_ID, "search"),
        ("sync_issues", SYNC_ISSUES_FOLDER_ID, "additional_ren"),
        ("conflicts", CONFLICTS_FOLDER_ID, "additional_ren"),
        ("local_failures", LOCAL_FAILURES_FOLDER_ID, "additional_ren"),
        (
            "server_failures",
            SERVER_FAILURES_FOLDER_ID,
            "additional_ren",
        ),
        ("junk", JUNK_FOLDER_ID, "additional_ren"),
        ("rss_feeds", RSS_FEEDS_FOLDER_ID, "additional_ren"),
        (
            "tracked_mail_processing",
            TRACKED_MAIL_PROCESSING_FOLDER_ID,
            "search",
        ),
        ("todo_search", TODO_SEARCH_FOLDER_ID, "search"),
        (
            "conversation_actions",
            CONVERSATION_ACTION_SETTINGS_FOLDER_ID,
            "associated",
        ),
        ("archive", ARCHIVE_FOLDER_ID, "additional_ren"),
        ("freebusy_data", FREEBUSY_DATA_FOLDER_ID, "freebusy"),
        (
            "conversation_history",
            CONVERSATION_HISTORY_FOLDER_ID,
            "additional_ren",
        ),
    ];

    SPECIAL_FOLDERS
        .iter()
        .map(|(role, folder_id, source)| {
            let opened = session
                .post_hierarchy_actions
                .opened_folder_ids
                .contains(folder_id);
            let checkpointed = session
                .post_hierarchy_actions
                .completed_sync_checkpoint_folder_ids
                .contains(folder_id);
            let hierarchy_root = session
                .post_hierarchy_actions
                .last_completed_hierarchy_sync_root
                .is_some_and(|root_id| root_id == *folder_id);
            format!(
                "{role}=0x{folder_id:016x};source={source};opened={opened};checkpointed={checkpointed};hierarchy_root={hierarchy_root}"
            )
        })
        .collect::<Vec<_>>()
        .join("|")
}

fn mapi_object_debug_kind(object: &MapiObject) -> &'static str {
    match object {
        MapiObject::Logon => "logon",
        MapiObject::PublicFolderLogon => "public_folder_logon",
        MapiObject::Folder { .. } => "folder",
        MapiObject::Message { .. } => "message",
        MapiObject::Contact { .. } => "contact",
        MapiObject::Event { .. } => "event",
        MapiObject::Task { .. } => "task",
        MapiObject::Note { .. } => "note",
        MapiObject::JournalEntry { .. } => "journal_entry",
        MapiObject::ConversationAction { .. } => "conversation_action",
        MapiObject::NavigationShortcut { .. } => "navigation_shortcut",
        MapiObject::DelegateFreeBusyMessage { .. } => "delegate_freebusy_message",
        MapiObject::RecoverableItem { .. } => "recoverable_item",
        MapiObject::PublicFolderItem { .. } => "public_folder_item",
        MapiObject::PendingMessage { .. } => "pending_message",
        MapiObject::PendingAssociatedMessage { .. } => "pending_associated_message",
        MapiObject::PendingContact { .. } => "pending_contact",
        MapiObject::PendingEvent { .. } => "pending_event",
        MapiObject::PendingTask { .. } => "pending_task",
        MapiObject::PendingNote { .. } => "pending_note",
        MapiObject::PendingJournalEntry { .. } => "pending_journal_entry",
        MapiObject::PendingConversationAction { .. } => "pending_conversation_action",
        MapiObject::PendingNavigationShortcut { .. } => "pending_navigation_shortcut",
        MapiObject::HierarchyTable { .. } => "hierarchy_table",
        MapiObject::ContentsTable { .. } => "contents_table",
        MapiObject::AttachmentTable { .. } => "attachment_table",
        MapiObject::PermissionTable { .. } => "permission_table",
        MapiObject::RuleTable { .. } => "rule_table",
        MapiObject::Attachment { .. } => "attachment",
        MapiObject::PendingAttachment { .. } => "pending_attachment",
        MapiObject::SavedAttachment { .. } => "saved_attachment",
        MapiObject::AttachmentStream { .. } => "attachment_stream",
        MapiObject::NotificationSubscription { .. } => "notification_subscription",
        MapiObject::SynchronizationSource { .. } => "synchronization_source",
        MapiObject::SynchronizationCollector { .. } => "synchronization_collector",
        MapiObject::FastTransferDestination { .. } => "fast_transfer_destination",
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(in crate::mapi) struct PostHierarchyActionDebugSummary {
    pub(in crate::mapi) execute_count: usize,
    pub(in crate::mapi) rop_ids_seen: String,
    pub(in crate::mapi) content_sync_configure_observed: bool,
    pub(in crate::mapi) release_client_initiated: bool,
    pub(in crate::mapi) logoff_client_initiated: bool,
    pub(in crate::mapi) disconnect_client_initiated: bool,
    pub(in crate::mapi) close_kind: &'static str,
    pub(in crate::mapi) last_completed_hierarchy_sync_root: String,
    pub(in crate::mapi) last_successful_hierarchy_get_buffer_summary: String,
}

pub(in crate::mapi) fn post_hierarchy_action_summary(
    session: &MapiSession,
    disconnect_client_initiated: bool,
) -> PostHierarchyActionDebugSummary {
    let actions = &session.post_hierarchy_actions;
    PostHierarchyActionDebugSummary {
        execute_count: actions.execute_count,
        rop_ids_seen: format_rop_ids_for_debug(&actions.rop_ids_seen),
        content_sync_configure_observed: actions.content_sync_configure_observed,
        release_client_initiated: actions.release_client_initiated,
        logoff_client_initiated: actions.logoff_client_initiated,
        disconnect_client_initiated: disconnect_client_initiated
            && actions.last_completed_hierarchy_sync_root.is_some(),
        close_kind: post_hierarchy_close_kind(actions, disconnect_client_initiated),
        last_completed_hierarchy_sync_root: actions
            .last_completed_hierarchy_sync_root
            .map(|folder_id| format!("0x{folder_id:016x}"))
            .unwrap_or_default(),
        last_successful_hierarchy_get_buffer_summary: actions
            .last_successful_hierarchy_get_buffer_summary
            .clone(),
    }
}

fn post_hierarchy_close_kind(
    actions: &PostHierarchyActionState,
    disconnect_client_initiated: bool,
) -> &'static str {
    if actions.content_sync_configure_observed {
        "post_hierarchy_content_sync_observed"
    } else if actions.release_client_initiated && actions.logoff_client_initiated {
        "outlook_release_logoff_before_content_sync"
    } else if actions.release_client_initiated {
        "outlook_release_before_content_sync"
    } else if actions.execute_count > 0 {
        "outlook_post_hierarchy_execute_before_content_sync"
    } else if disconnect_client_initiated && actions.last_completed_hierarchy_sync_root.is_some() {
        "outlook_disconnect_immediately_after_hierarchy"
    } else {
        "post_hierarchy_no_close"
    }
}

fn partial_scope_checkpoint_not_stored_count(actions: &PostHierarchyActionState) -> usize {
    actions
        .completed_sync_checkpoint_summaries
        .iter()
        .filter(|summary| summary.contains("status=ok_partial_scope_no_checkpoint"))
        .count()
}

fn format_rop_ids_for_debug(rop_ids: &[u8]) -> String {
    rop_ids
        .iter()
        .map(|rop_id| format!("0x{rop_id:02x}"))
        .collect::<Vec<_>>()
        .join(",")
}

pub(in crate::mapi) async fn notification_wait_response<S>(
    store: &S,
    endpoint: MapiEndpoint,
    principal: &AccountPrincipal,
    headers: &HeaderMap,
    request_id: &str,
) -> Response
where
    S: ExchangeStore,
{
    log_session_cookie_lookup(endpoint, principal, headers, "NotificationWait");
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

    let mut session = session;
    let mut events = session.take_pending_notifications();
    let mut event_pending = !events.is_empty();
    if !event_pending {
        if let Some(cursor) = session.notification_cursor {
            if let Ok(poll) = store
                .poll_mapi_notifications(principal.account_id, cursor)
                .await
            {
                events = session.matching_notifications(poll.events);
                event_pending = poll.event_pending || !events.is_empty();
                session.notification_cursor = poll.cursor.or(Some(cursor));
            }
        }
    }
    store_session(session_id.clone(), session);
    let body = notification_wait_body_with_events(event_pending, &events);
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
    log_session_cookie_lookup(endpoint, principal, headers, "PING");
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

fn content_length_matches_body(value: &str, body: &[u8]) -> bool {
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
    let start_time = mapi_http_date(SystemTime::now());
    let mut framed_body = Vec::new();
    framed_body.extend_from_slice(b"PROCESSING\r\n");
    framed_body.extend_from_slice(b"DONE\r\n");
    framed_body.extend_from_slice(format!("X-ResponseCode: {response_code}\r\n").as_bytes());
    framed_body.extend_from_slice(b"X-ElapsedTime: 0\r\n");
    framed_body.extend_from_slice(format!("X-StartTime: {start_time}\r\n").as_bytes());
    framed_body.extend_from_slice(b"\r\n");
    framed_body.extend_from_slice(&body);

    let framed_body_len = framed_body.len();
    let mut response = (StatusCode::OK, framed_body).into_response();
    response.extensions_mut().insert(MapiResponseDebug {
        payload_bytes: body.len(),
    });
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

fn mapi_http_date(time: SystemTime) -> String {
    const WEEKDAYS: [&str; 7] = ["Thu", "Fri", "Sat", "Sun", "Mon", "Tue", "Wed"];
    const MONTHS: [&str; 12] = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ];
    let duration = time
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    let total_seconds = duration.as_secs();
    let days = (total_seconds / 86_400) as i64;
    let seconds_of_day = total_seconds % 86_400;
    let (year, month, day) = civil_from_days(days);
    let hour = seconds_of_day / 3_600;
    let minute = (seconds_of_day % 3_600) / 60;
    let second = seconds_of_day % 60;
    let weekday = WEEKDAYS[days.rem_euclid(7) as usize];
    format!(
        "{weekday}, {day:02} {} {year:04} {hour:02}:{minute:02}:{second:02} GMT",
        MONTHS[(month - 1) as usize]
    )
}

fn civil_from_days(days_since_epoch: i64) -> (i64, u32, u32) {
    let days = days_since_epoch + 719_468;
    let era = if days >= 0 { days } else { days - 146_096 } / 146_097;
    let day_of_era = days - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let mut year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_position = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_position + 2) / 5 + 1;
    let month = month_position + if month_position < 10 { 3 } else { -9 };
    year += i64::from(month <= 2);
    (year, month as u32, day as u32)
}

#[derive(Clone, Copy, Debug)]
pub(in crate::mapi) struct MapiResponseDebug {
    payload_bytes: usize,
}

pub(crate) fn mapi_response_payload_bytes(response: &Response) -> Option<usize> {
    response
        .extensions()
        .get::<MapiResponseDebug>()
        .map(|debug| debug.payload_bytes)
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
    let endpoint = match endpoint {
        MapiEndpoint::Emsmdb => "emsmdb",
        MapiEndpoint::Nspi => "nspi",
    };
    let client_request_id = safe_header(headers, "client-request-id").unwrap_or_default();
    let client_application = safe_header(headers, "x-clientapplication").unwrap_or_default();
    let client_info = safe_header(headers, "x-clientinfo").unwrap_or_default();
    let (request_guid, request_counter) = guid_counter_debug(request_id);
    let (client_info_guid, client_info_counter) = guid_counter_debug(&client_info);
    let client_flow_key = client_flow_key(&client_info);
    let trace_id = safe_header(headers, "x-trace-id").unwrap_or_default();
    let user_agent = safe_header(headers, "user-agent").unwrap_or_default();
    let host = safe_header(headers, "host").unwrap_or_default();
    let set_cookie_names = response_set_cookie_names(response);
    let response_content_type = response_header(response, "content-type").unwrap_or_default();
    let response_www_authenticate =
        response_header(response, "www-authenticate").unwrap_or_default();
    let response_x_request_type = response_header(response, "x-requesttype").unwrap_or_default();
    let response_x_request_id = response_header(response, "x-requestid").unwrap_or_default();
    let response_x_expiration_info =
        response_header(response, "x-expirationinfo").unwrap_or_default();
    let response_x_pending_period =
        response_header(response, "x-pendingperiod").unwrap_or_default();
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
            client_application = %client_application,
            client_info = %client_info,
            client_flow_key = %client_flow_key,
            request_guid = %request_guid,
            request_counter = %request_counter,
            client_info_guid = %client_info_guid,
            client_info_counter = %client_info_counter,
            trace_id = %trace_id,
            user_agent = %user_agent,
            host = %host,
            http_status = status,
            mapi_response_code = %response_code,
            request_body_bytes,
            response_payload_bytes = payload_bytes,
            response_content_type = %response_content_type,
            response_www_authenticate = %response_www_authenticate,
            response_x_request_type = %response_x_request_type,
            response_x_request_id = %response_x_request_id,
            response_x_expiration_info = %response_x_expiration_info,
            response_x_pending_period = %response_x_pending_period,
            set_cookie_names = %set_cookie_names,
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
            client_application = %client_application,
            client_info = %client_info,
            client_flow_key = %client_flow_key,
            request_guid = %request_guid,
            request_counter = %request_counter,
            client_info_guid = %client_info_guid,
            client_info_counter = %client_info_counter,
            trace_id = %trace_id,
            user_agent = %user_agent,
            host = %host,
            http_status = status,
            mapi_response_code = %response_code,
            request_body_bytes,
            response_payload_bytes = payload_bytes,
            response_content_type = %response_content_type,
            response_www_authenticate = %response_www_authenticate,
            response_x_request_type = %response_x_request_type,
            response_x_request_id = %response_x_request_id,
            response_x_expiration_info = %response_x_expiration_info,
            response_x_pending_period = %response_x_pending_period,
            set_cookie_names = %set_cookie_names,
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
    request_named_cookie_candidates(name, headers)
        .last()
        .cloned()
}

fn request_named_cookie_candidates(name: &str, headers: &HeaderMap) -> Vec<String> {
    headers
        .get_all("cookie")
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|cookie| {
            cookie
                .split(';')
                .filter_map(|part| {
                    let (key, value) = part.trim().split_once('=')?;
                    (key == name && !value.is_empty()).then(|| value.to_string())
                })
                .collect::<Vec<_>>()
        })
        .collect()
}

#[derive(Debug, Default, PartialEq, Eq)]
struct CookieValueDebug {
    suffix: String,
    hash: String,
}

#[derive(Debug, Default, PartialEq, Eq)]
struct SessionCookieLookupDebug {
    cookie_header_count: usize,
    context_candidate_count: usize,
    sequence_candidate_count: usize,
    selected_context: CookieValueDebug,
    selected_sequence: CookieValueDebug,
    selected_session_exists: bool,
    selected_session_endpoint_matches: bool,
    selected_session_principal_matches: bool,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct RequestCookieTransportDebug {
    pub(crate) cookie_header_count: usize,
    pub(crate) context_candidate_count: usize,
    pub(crate) sequence_candidate_count: usize,
    pub(crate) selected_context_suffix: String,
    pub(crate) selected_context_hash: String,
    pub(crate) selected_sequence_suffix: String,
    pub(crate) selected_sequence_hash: String,
}

pub(crate) fn request_cookie_transport_debug(
    endpoint: MapiEndpoint,
    headers: &HeaderMap,
) -> RequestCookieTransportDebug {
    let context_candidates = request_named_cookie_candidates(cookie_name(endpoint), headers);
    let sequence_candidates =
        request_named_cookie_candidates(sequence_cookie_name(endpoint), headers);
    let selected_context = context_candidates.last().cloned();
    let selected_sequence = sequence_candidates.last().cloned();
    let selected_context = cookie_value_debug(selected_context.as_deref());
    let selected_sequence = cookie_value_debug(selected_sequence.as_deref());

    RequestCookieTransportDebug {
        cookie_header_count: headers.get_all("cookie").iter().count(),
        context_candidate_count: context_candidates.len(),
        sequence_candidate_count: sequence_candidates.len(),
        selected_context_suffix: selected_context.suffix,
        selected_context_hash: selected_context.hash,
        selected_sequence_suffix: selected_sequence.suffix,
        selected_sequence_hash: selected_sequence.hash,
    }
}

fn session_cookie_lookup_debug(
    endpoint: MapiEndpoint,
    principal: &AccountPrincipal,
    headers: &HeaderMap,
) -> SessionCookieLookupDebug {
    let context_candidates = request_named_cookie_candidates(cookie_name(endpoint), headers);
    let sequence_candidates =
        request_named_cookie_candidates(sequence_cookie_name(endpoint), headers);
    let selected_context = context_candidates.last().cloned();
    let selected_sequence = sequence_candidates.last().cloned();
    let session = selected_context.as_deref().and_then(get_session);
    let selected_session_exists = session.is_some();
    let selected_session_endpoint_matches = session
        .as_ref()
        .is_some_and(|session| session.endpoint == endpoint);
    let selected_session_principal_matches = session.as_ref().is_some_and(|session| {
        session.tenant_id == principal.tenant_id
            && session.account_id == principal.account_id
            && session.email == principal.email
    });

    SessionCookieLookupDebug {
        cookie_header_count: headers.get_all("cookie").iter().count(),
        context_candidate_count: context_candidates.len(),
        sequence_candidate_count: sequence_candidates.len(),
        selected_context: cookie_value_debug(selected_context.as_deref()),
        selected_sequence: cookie_value_debug(selected_sequence.as_deref()),
        selected_session_exists,
        selected_session_endpoint_matches,
        selected_session_principal_matches,
    }
}

fn cookie_value_debug(value: Option<&str>) -> CookieValueDebug {
    let Some(value) = value else {
        return CookieValueDebug::default();
    };
    CookieValueDebug {
        suffix: cookie_value_suffix(value),
        hash: format!("{:016x}", mapi_payload_fingerprint(value.as_bytes())),
    }
}

fn cookie_value_suffix(value: &str) -> String {
    let mut chars = value.chars().rev().take(8).collect::<Vec<_>>();
    chars.reverse();
    chars.into_iter().collect()
}

pub(in crate::mapi) fn log_session_cookie_lookup(
    endpoint: MapiEndpoint,
    principal: &AccountPrincipal,
    headers: &HeaderMap,
    request_type: &str,
) {
    let summary = session_cookie_lookup_debug(endpoint, principal, headers);
    let endpoint = match endpoint {
        MapiEndpoint::Emsmdb => "emsmdb",
        MapiEndpoint::Nspi => "nspi",
    };

    tracing::info!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = endpoint,
        tenant_id = %principal.tenant_id,
        account_id = %principal.account_id,
        mailbox = %principal.email,
        request_type = %request_type,
        cookie_header_count = summary.cookie_header_count,
        mapi_context_candidate_count = summary.context_candidate_count,
        mapi_sequence_candidate_count = summary.sequence_candidate_count,
        selected_context_suffix = %summary.selected_context.suffix,
        selected_context_hash = %summary.selected_context.hash,
        selected_sequence_suffix = %summary.selected_sequence.suffix,
        selected_sequence_hash = %summary.selected_sequence.hash,
        selected_session_exists = summary.selected_session_exists,
        selected_session_endpoint_matches = summary.selected_session_endpoint_matches,
        selected_session_principal_matches = summary.selected_session_principal_matches,
        message = "rca debug mapi session cookie lookup",
    );
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

#[cfg(test)]
mod tests {
    use super::*;

    fn test_session(handles: HashMap<u32, MapiObject>) -> MapiSession {
        MapiSession {
            endpoint: MapiEndpoint::Emsmdb,
            tenant_id: Uuid::from_u128(0xaaaaaaaa_aaaa_aaaa_aaaa_aaaaaaaaaaaa),
            account_id: Uuid::from_u128(0xbbbbbbbb_bbbb_bbbb_bbbb_bbbbbbbbbbbb),
            email: "user@example.test".to_string(),
            created_at: SystemTime::now(),
            last_seen_at: SystemTime::now(),
            first_request_type: "Connect".to_string(),
            first_request_id: "test:1".to_string(),
            last_request_type: "Connect".to_string(),
            last_request_id: "test:1".to_string(),
            request_count: 1,
            execute_request_count: 0,
            next_handle: 1,
            handles,
            message_statuses: HashMap::new(),
            named_properties: HashMap::new(),
            named_property_ids: HashMap::new(),
            next_named_property_id: crate::mapi::properties::FIRST_NAMED_PROPERTY_ID,
            next_local_replica_sequence: 1,
            notification_cursor: None,
            pending_notifications: VecDeque::new(),
            completed_execute_requests: HashMap::new(),
            completed_execute_request_order: VecDeque::new(),
            post_hierarchy_actions: PostHierarchyActionState::default(),
            logon_identity: None,
        }
    }

    fn test_principal() -> AccountPrincipal {
        AccountPrincipal {
            tenant_id: Uuid::from_u128(0xaaaaaaaa_aaaa_aaaa_aaaa_aaaaaaaaaaaa),
            account_id: Uuid::from_u128(0xbbbbbbbb_bbbb_bbbb_bbbb_bbbbbbbbbbbb),
            email: "user@example.test".to_string(),
            display_name: "User".to_string(),
        }
    }

    #[test]
    fn connect_body_debug_summary_decodes_fields() {
        let mut body = Vec::new();
        write_u32(&mut body, 0);
        write_u32(&mut body, 0);
        write_u32(&mut body, 60_000);
        write_u32(&mut body, 6);
        write_u32(&mut body, 10_000);
        body.extend_from_slice(b"/o=LPE/ou=Exchange Administrative Group/cn=Recipients/cn=\0");
        write_utf16z(&mut body, "Alice");
        let auxiliary_buffer = connect_auxiliary_buffer();
        write_u32(&mut body, auxiliary_buffer.len() as u32);
        body.extend_from_slice(&auxiliary_buffer);

        let summary = summarize_connect_body(&body);

        assert_eq!(summary.status_code, 0);
        assert_eq!(summary.error_code, 0);
        assert_eq!(summary.polls_max, 60_000);
        assert_eq!(summary.retry_count, 6);
        assert_eq!(summary.retry_delay_ms, 10_000);
        assert_eq!(
            summary.dn_prefix,
            "/o=LPE/ou=Exchange Administrative Group/cn=Recipients/cn="
        );
        assert_eq!(summary.display_name, "Alice");
        assert_eq!(
            summary.auxiliary_buffer_bytes,
            auxiliary_buffer.len() as u32
        );
        assert!(summary.parse_error.is_empty());
    }

    #[test]
    fn mapi_http_date_formats_imf_fixdate_in_gmt() {
        assert_eq!(
            mapi_http_date(SystemTime::UNIX_EPOCH + Duration::from_secs(0)),
            "Thu, 01 Jan 1970 00:00:00 GMT"
        );
        assert_eq!(
            mapi_http_date(SystemTime::UNIX_EPOCH + Duration::from_secs(1_780_144_640)),
            "Sat, 30 May 2026 12:37:20 GMT"
        );
    }

    #[tokio::test]
    async fn mapi_response_start_time_uses_current_http_date_not_sentinel() {
        let response = mapi_response("Execute", "request:1", 0, Vec::new(), None);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body = String::from_utf8(body.to_vec()).unwrap();

        assert!(body.contains("\r\nX-StartTime: "));
        assert!(body.contains(" GMT\r\n\r\n"));
        assert!(!body.contains("Mon, 01 Jan 2001 00:00:00 GMT"));
    }

    #[test]
    fn session_cookie_lookup_debug_reports_sanitized_latest_cookie_selection() {
        let principal = test_principal();
        let session_id = create_session(MapiEndpoint::Emsmdb, &principal, "Connect", "test:1");
        let stale_id = "00000000-0000-0000-0000-000000000000";
        let mut headers = HeaderMap::new();
        headers.append(
            "cookie",
            HeaderValue::from_str(&format!("MapiContext={stale_id}; MapiSequence={stale_id}"))
                .unwrap(),
        );
        headers.append(
            "cookie",
            HeaderValue::from_str(&format!(
                "MapiContext={session_id}; MapiSequence={session_id}"
            ))
            .unwrap(),
        );

        let summary = session_cookie_lookup_debug(MapiEndpoint::Emsmdb, &principal, &headers);

        assert_eq!(summary.cookie_header_count, 2);
        assert_eq!(summary.context_candidate_count, 2);
        assert_eq!(summary.sequence_candidate_count, 2);
        assert_eq!(
            summary.selected_context.suffix,
            cookie_value_suffix(&session_id)
        );
        assert_eq!(
            summary.selected_sequence.suffix,
            cookie_value_suffix(&session_id)
        );
        assert_eq!(
            summary.selected_context.hash,
            format!("{:016x}", mapi_payload_fingerprint(session_id.as_bytes()))
        );
        assert_eq!(summary.selected_context.hash.len(), 16);
        assert_ne!(summary.selected_context.hash, session_id);
        assert_ne!(summary.selected_sequence.hash, session_id);
        assert!(summary.selected_session_exists);
        assert!(summary.selected_session_endpoint_matches);
        assert!(summary.selected_session_principal_matches);
        remove_session(&session_id);
    }

    #[test]
    fn session_cookie_lookup_debug_reports_endpoint_and_principal_mismatch() {
        let principal = test_principal();
        let session_id = create_session(MapiEndpoint::Nspi, &principal, "Bind", "test:1");
        let mut headers = HeaderMap::new();
        headers.insert(
            "cookie",
            HeaderValue::from_str(&format!(
                "MapiContext={session_id}; MapiSequence={session_id}"
            ))
            .unwrap(),
        );

        let summary = session_cookie_lookup_debug(MapiEndpoint::Emsmdb, &principal, &headers);

        assert!(summary.selected_session_exists);
        assert!(!summary.selected_session_endpoint_matches);
        assert!(summary.selected_session_principal_matches);
        remove_session(&session_id);

        let session_id = create_session(MapiEndpoint::Emsmdb, &principal, "Connect", "test:1");
        let other_principal = AccountPrincipal {
            account_id: Uuid::from_u128(0xcccccccc_cccc_cccc_cccc_cccccccccccc),
            email: "other@example.test".to_string(),
            ..principal
        };
        let mut headers = HeaderMap::new();
        headers.insert(
            "cookie",
            HeaderValue::from_str(&format!(
                "MapiContext={session_id}; MapiSequence={session_id}"
            ))
            .unwrap(),
        );

        let summary = session_cookie_lookup_debug(MapiEndpoint::Emsmdb, &other_principal, &headers);

        assert!(summary.selected_session_exists);
        assert!(summary.selected_session_endpoint_matches);
        assert!(!summary.selected_session_principal_matches);
        remove_session(&session_id);
    }

    #[test]
    fn post_hierarchy_action_summary_stays_empty_before_completed_hierarchy() {
        let mut session = test_session(HashMap::new());

        session.record_execute_after_hierarchy_completion(&[0x01, 0x70]);
        let summary = post_hierarchy_action_summary(&session, true);

        assert_eq!(summary.execute_count, 0);
        assert_eq!(summary.rop_ids_seen, "");
        assert!(!summary.content_sync_configure_observed);
        assert!(!summary.release_client_initiated);
        assert!(!summary.logoff_client_initiated);
        assert!(!summary.disconnect_client_initiated);
        assert_eq!(summary.close_kind, "post_hierarchy_no_close");
        assert_eq!(summary.last_completed_hierarchy_sync_root, "");
        assert_eq!(summary.last_successful_hierarchy_get_buffer_summary, "");
    }

    #[test]
    fn post_hierarchy_action_summary_records_execute_rops_and_client_actions() {
        let mut session = test_session(HashMap::new());

        session.record_completed_hierarchy_sync(
            crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
            "folder=0x0000000000040001;status=0x0003".to_string(),
        );
        let first = session.record_execute_after_hierarchy_completion(&[0x02, 0x70, 0x4e]);
        let second = session.record_execute_after_hierarchy_completion(&[0x01, 0x70]);
        session.record_content_sync_configure();
        session.record_logoff_after_hierarchy_completion();
        let summary = post_hierarchy_action_summary(&session, true);

        assert!(first.first_execute);
        assert!(first.first_bootstrap_probe);
        assert!(!first.first_set_properties_probe);
        assert!(!second.first_execute);
        assert!(!second.first_bootstrap_probe);
        assert!(!second.first_set_properties_probe);
        assert_eq!(summary.execute_count, 2);
        assert_eq!(summary.rop_ids_seen, "0x02,0x70,0x4e,0x01");
        assert!(summary.content_sync_configure_observed);
        assert!(summary.release_client_initiated);
        assert!(summary.logoff_client_initiated);
        assert!(summary.disconnect_client_initiated);
        assert_eq!(summary.close_kind, "post_hierarchy_content_sync_observed");
        assert_eq!(
            summary.last_completed_hierarchy_sync_root,
            format!("0x{:016x}", crate::mapi::identity::IPM_SUBTREE_FOLDER_ID)
        );
        assert_eq!(
            summary.last_successful_hierarchy_get_buffer_summary,
            "folder=0x0000000000040001;status=0x0003"
        );
    }

    #[test]
    fn partial_scope_checkpoint_not_stored_count_counts_expected_partial_scope_summaries() {
        let mut session = test_session(HashMap::new());

        session.record_completed_sync_checkpoint(
            crate::mapi::identity::TRASH_FOLDER_ID,
            "trash",
            "IPF.Note",
            "content",
            0x01,
            "ok_partial_scope_no_checkpoint",
        );
        session.record_completed_sync_checkpoint(
            crate::mapi::identity::CALENDAR_FOLDER_ID,
            "calendar",
            "IPF.Appointment",
            "content",
            0x01,
            "ok",
        );

        assert_eq!(
            partial_scope_checkpoint_not_stored_count(&session.post_hierarchy_actions),
            1
        );
    }

    #[test]
    fn post_hierarchy_action_summary_classifies_release_logoff_without_content_sync() {
        let mut session = test_session(HashMap::new());

        session.record_completed_hierarchy_sync(
            crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
            "folder=0x0000000000040001;status=0x0003".to_string(),
        );
        session.record_execute_after_hierarchy_completion(&[0x01]);
        session.record_logoff_after_hierarchy_completion();
        let summary = post_hierarchy_action_summary(&session, true);

        assert_eq!(summary.execute_count, 1);
        assert_eq!(summary.rop_ids_seen, "0x01");
        assert!(summary.release_client_initiated);
        assert!(summary.logoff_client_initiated);
        assert!(!summary.content_sync_configure_observed);
        assert_eq!(
            summary.close_kind,
            "outlook_release_logoff_before_content_sync"
        );
    }

    #[test]
    fn post_hierarchy_observation_logs_first_execute_and_later_first_bootstrap_probe() {
        let mut session = test_session(HashMap::new());

        session.record_completed_hierarchy_sync(
            crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
            "folder=0x0000000000040001;status=0x0003".to_string(),
        );
        let receive_folder_probe = session.record_execute_after_hierarchy_completion(&[0x01, 0x27]);
        let default_folder_probe = session.record_execute_after_hierarchy_completion(&[0x02, 0x07]);
        let later_default_folder_probe =
            session.record_execute_after_hierarchy_completion(&[0x02, 0x0a]);
        let second_set_properties_probe =
            session.record_execute_after_hierarchy_completion(&[0x02, 0x0a]);

        assert!(receive_folder_probe.first_execute);
        assert!(!receive_folder_probe.first_bootstrap_probe);
        assert!(!receive_folder_probe.first_set_properties_probe);
        assert!(!default_folder_probe.first_execute);
        assert!(default_folder_probe.first_bootstrap_probe);
        assert!(!default_folder_probe.first_set_properties_probe);
        assert!(!later_default_folder_probe.first_execute);
        assert!(!later_default_folder_probe.first_bootstrap_probe);
        assert!(later_default_folder_probe.first_set_properties_probe);
        assert!(!second_set_properties_probe.first_execute);
        assert!(!second_set_properties_probe.first_bootstrap_probe);
        assert!(!second_set_properties_probe.first_set_properties_probe);
    }
}
