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
use super::outlook_startup::*;
use super::rop::*;
use super::session::*;
use super::wire::MapiHttpRequestType as MapiRequestType;
use super::*;
use lpe_core::outlook_trace::{write_outlook_trace, OutlookTraceDirection, OutlookTraceEvent};
use lpe_domain::{month_abbrev, utc_from_unix_seconds, weekday_abbrev_from_unix_days};

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
pub(in crate::mapi) const MAPI_NOTIFICATION_WAIT_EMPTY_DELAY_MILLIS: u64 = 1_500;
pub(in crate::mapi) const MAPI_NOTIFICATION_WAIT_SUBSCRIPTION_EMPTY_DELAY_MILLIS: u64 = 300_000;
pub(in crate::mapi) const MAPI_NOTIFICATION_WAIT_REACQUIRE_RETRY_ATTEMPTS: usize = 200;
pub(in crate::mapi) const MAPI_NOTIFICATION_WAIT_REACQUIRE_RETRY_DELAY_MS: u64 = 10;
pub(in crate::mapi) const NSPI_UNICODE_CODEPAGE: u32 = 1200;
pub(in crate::mapi) const MAPI_MAILUSER_OBJECT_TYPE: u32 = 6;
pub(in crate::mapi) const NSPI_MID_RESOLVED: u32 = 0x0000_0002;
pub(in crate::mapi) const MAX_CACHED_EXECUTE_REQUESTS: usize = 64;
pub(in crate::mapi) const NSPI_SERVER_GUID: [u8; 16] = [
    0x2b, 0xe6, 0x0b, 0x5d, 0x9f, 0x35, 0x3f, 0x45, 0x9a, 0x68, 0x4c, 0x4b, 0xc5, 0x8f, 0x3f, 0x30,
];

mod cookies;
mod diagnostics;
mod headers;

pub(crate) use cookies::request_cookie_transport_debug;
pub(in crate::mapi) use cookies::*;
use diagnostics::{log_connect_body_debug, log_mapi_session_disconnect};
#[cfg(test)]
use diagnostics::{
    outlook_bootstrap_next_expected_phase, outlook_bootstrap_phase, outlook_bootstrap_phase_name,
    outlook_bootstrap_stall_code, outlook_bootstrap_stall_name,
    partial_scope_checkpoint_not_stored_count, post_fai_inbox_probe_loop_terminal_summary,
    required_default_folder_disconnect_coverage_summary, special_folder_contract_summary,
    summarize_connect_body,
};
pub(in crate::mapi) use diagnostics::{
    post_hierarchy_action_summary, visible_inbox_release_without_query_rows_observed,
};
pub(in crate::mapi) use headers::*;
pub(crate) use headers::{
    client_flow_key, debug_payload_preview_hex, guid_counter_debug, hex_preview, safe_header,
};

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
    let outlook_smart_input_variant = configured_smart_input_variant();

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
        outlook_smart_input_variant = %outlook_smart_input_variant,
        outlook_smart_input_variant_scope = "session",
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
    let client_info = safe_header(headers, "x-clientinfo").unwrap_or_default();
    let client_flow_key = client_flow_key(&client_info);
    let (request_guid, request_counter) = guid_counter_debug(request_id);
    let (client_info_guid, client_info_counter) = guid_counter_debug(&client_info);
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
    let Some(_active_request) = acquire_notification_wait_active_session_request(&session_id).await
    else {
        info!(
            rca_debug = true,
            adapter = "mapi",
            operation = "NotificationWait",
            account_id = %principal.account_id,
            mailbox = %principal.email,
            mapi_request_id = %request_id,
            request_guid = %request_guid,
            request_counter = %request_counter,
            client_info = %client_info,
            client_flow_key = %client_flow_key,
            client_info_guid = %client_info_guid,
            client_info_counter = %client_info_counter,
            session_id_prefix = %session_id_prefix(&session_id),
            active_session_overlap = true,
            "rca debug notification wait overlap empty response"
        );
        return notification_wait_empty_response(endpoint, request_id, &session_id);
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
    let initial_queued_event_count = session.pending_notification_count();
    let mut event_pending = initial_queued_event_count != 0;
    let mut waited_for_empty_events = false;
    let initial_cursor = session.notification_cursor;
    let mut first_poll_event_pending = false;
    let mut first_poll_event_count = 0usize;
    let mut first_poll_cursor = initial_cursor;
    let mut second_poll_event_pending = false;
    let mut second_poll_event_count = 0usize;
    let mut second_poll_cursor = initial_cursor;
    let mut active_during_empty_wait = false;
    let mut reacquire_after_wait_failed = false;
    let mut empty_wait_elapsed_millis = 0u128;
    let empty_wait_delay_millis = notification_wait_empty_delay_millis(&session);
    if !event_pending {
        if let Some(cursor) = session.notification_cursor {
            if let Ok(poll) = store
                .poll_mapi_notifications(principal.account_id, cursor)
                .await
            {
                first_poll_event_pending = poll.event_pending;
                first_poll_event_count = poll.events.len();
                first_poll_cursor = poll.cursor.or(Some(cursor));
                for event in session.matching_notifications(poll.events) {
                    session.record_notification(event);
                }
                event_pending = session.pending_notification_count() != 0;
                session.notification_cursor = poll.cursor.or(Some(cursor));
            }
        }
    }
    if !event_pending {
        store_session(session_id.clone(), session);
        drop(_active_request);
        let empty_wait_started_at = std::time::Instant::now();
        tokio::time::sleep(std::time::Duration::from_millis(empty_wait_delay_millis)).await;
        empty_wait_elapsed_millis = empty_wait_started_at.elapsed().as_millis();
        active_during_empty_wait = session_request_is_active(&session_id);
        waited_for_empty_events = true;
        let Some(_active_request) =
            acquire_notification_wait_active_session_request(&session_id).await
        else {
            reacquire_after_wait_failed = true;
            info!(
                rca_debug = true,
                adapter = "mapi",
                operation = "NotificationWait",
                account_id = %principal.account_id,
                mailbox = %principal.email,
                mapi_request_id = %request_id,
                request_guid = %request_guid,
                request_counter = %request_counter,
                client_info = %client_info,
                client_flow_key = %client_flow_key,
                client_info_guid = %client_info_guid,
                client_info_counter = %client_info_counter,
                session_id_prefix = %session_id_prefix(&session_id),
                initial_queued_event_count,
                initial_cursor = ?initial_cursor,
                first_poll_event_pending,
                first_poll_event_count,
                first_poll_cursor = ?first_poll_cursor,
                waited_for_empty_events,
                configured_empty_wait_millis = empty_wait_delay_millis,
                empty_wait_elapsed_millis,
                active_during_empty_wait,
                reacquire_after_wait_failed,
                "rca debug notification wait reacquire overlap empty response"
            );
            return notification_wait_empty_response(endpoint, request_id, &session_id);
        };
        let Some(waited_session) = remove_session(&session_id) else {
            return mapi_diagnostic_response(
                "NotificationWait",
                request_id,
                10,
                "MAPI session context not found after notification wait",
            );
        };
        if !session_matches(&waited_session, endpoint, principal) {
            return mapi_diagnostic_response(
                "NotificationWait",
                request_id,
                10,
                "MAPI authentication context changed after notification wait",
            );
        }
        session = waited_session;
        second_poll_cursor = session.notification_cursor;
        if let Some(cursor) = session.notification_cursor {
            if let Ok(poll) = store
                .poll_mapi_notifications(principal.account_id, cursor)
                .await
            {
                second_poll_event_pending = poll.event_pending;
                second_poll_event_count = poll.events.len();
                second_poll_cursor = poll.cursor.or(Some(cursor));
                for event in session.matching_notifications(poll.events) {
                    session.record_notification(event);
                }
                event_pending = session.pending_notification_count() != 0;
                session.notification_cursor = poll.cursor.or(Some(cursor));
            }
        }
    }
    let post_inbox_fai_handoff_context =
        format_inbox_post_fai_handoff_context(&session.post_hierarchy_actions);
    if event_pending
        || session.pending_notification_count() != 0
        || active_during_empty_wait
        || reacquire_after_wait_failed
    {
        info!(
            rca_debug = true,
            adapter = "mapi",
            operation = "NotificationWait",
            account_id = %principal.account_id,
            mailbox = %principal.email,
            mapi_request_id = %request_id,
            request_guid = %request_guid,
            request_counter = %request_counter,
            client_info = %client_info,
            client_flow_key = %client_flow_key,
            client_info_guid = %client_info_guid,
            client_info_counter = %client_info_counter,
            session_id_prefix = %session_id_prefix(&session_id),
            initial_queued_event_count,
            initial_cursor = ?initial_cursor,
            first_poll_event_pending,
            first_poll_event_count,
            first_poll_cursor = ?first_poll_cursor,
            second_poll_event_pending,
            second_poll_event_count,
            second_poll_cursor = ?second_poll_cursor,
            event_pending,
            event_count = session.pending_notification_count(),
            inbox_associated_query_rows_returned_non_empty = session
                .post_hierarchy_actions
                .inbox_associated_query_rows_returned_non_empty,
            inbox_associated_findrow_returned_content = session
                .post_hierarchy_actions
                .inbox_associated_findrow_returned_content,
            inbox_associated_query_rows_reached_end = session
                .post_hierarchy_actions
                .inbox_associated_query_rows_reached_end,
            inbox_associated_config_open_observed = session
                .post_hierarchy_actions
                .inbox_associated_config_open_observed,
            inbox_associated_config_stream_open_observed = session
                .post_hierarchy_actions
                .inbox_associated_config_stream_open_observed,
            inbox_associated_config_stream_read_observed = session
                .post_hierarchy_actions
                .inbox_associated_config_stream_read_observed,
            inbox_normal_contents_table_observed = session
                .post_hierarchy_actions
                .inbox_normal_contents_table_observed,
            inbox_normal_query_rows_observed = session
                .post_hierarchy_actions
                .inbox_normal_contents_table_query_rows_observed,
            post_inbox_fai_handoff_context = %post_inbox_fai_handoff_context,
            waited_for_empty_events,
            configured_empty_wait_millis = if waited_for_empty_events {
                empty_wait_delay_millis
            } else {
                0
            },
            empty_wait_elapsed_millis,
            active_during_empty_wait,
            reacquire_after_wait_failed,
            "rca debug notification wait result"
        );
    } else {
        tracing::debug!(
            rca_debug = true,
            adapter = "mapi",
            operation = "NotificationWait",
            account_id = %principal.account_id,
            mailbox = %principal.email,
            mapi_request_id = %request_id,
            request_guid = %request_guid,
            request_counter = %request_counter,
            client_info = %client_info,
            client_flow_key = %client_flow_key,
            client_info_guid = %client_info_guid,
            client_info_counter = %client_info_counter,
            session_id_prefix = %session_id_prefix(&session_id),
            initial_queued_event_count,
            initial_cursor = ?initial_cursor,
            first_poll_event_pending,
            first_poll_event_count,
            first_poll_cursor = ?first_poll_cursor,
            second_poll_event_pending,
            second_poll_event_count,
            second_poll_cursor = ?second_poll_cursor,
            event_pending,
            event_count = session.pending_notification_count(),
            inbox_associated_query_rows_returned_non_empty = session
                .post_hierarchy_actions
                .inbox_associated_query_rows_returned_non_empty,
            inbox_associated_findrow_returned_content = session
                .post_hierarchy_actions
                .inbox_associated_findrow_returned_content,
            inbox_associated_query_rows_reached_end = session
                .post_hierarchy_actions
                .inbox_associated_query_rows_reached_end,
            inbox_associated_config_open_observed = session
                .post_hierarchy_actions
                .inbox_associated_config_open_observed,
            inbox_associated_config_stream_open_observed = session
                .post_hierarchy_actions
                .inbox_associated_config_stream_open_observed,
            inbox_associated_config_stream_read_observed = session
                .post_hierarchy_actions
                .inbox_associated_config_stream_read_observed,
            inbox_normal_contents_table_observed = session
                .post_hierarchy_actions
                .inbox_normal_contents_table_observed,
            inbox_normal_query_rows_observed = session
                .post_hierarchy_actions
                .inbox_normal_contents_table_query_rows_observed,
            post_inbox_fai_handoff_context = %post_inbox_fai_handoff_context,
            waited_for_empty_events,
            configured_empty_wait_millis = if waited_for_empty_events {
                empty_wait_delay_millis
            } else {
                0
            },
            empty_wait_elapsed_millis,
            active_during_empty_wait,
            reacquire_after_wait_failed,
            "rca debug notification wait result"
        );
    }
    store_session(session_id.clone(), session);
    let body = notification_wait_body(event_pending);
    mapi_response_with_cookies(
        "NotificationWait",
        request_id,
        0,
        body,
        session_context_cookies(endpoint, &session_id, false),
    )
}

fn notification_wait_empty_delay_millis(session: &MapiSession) -> u64 {
    if session.logon_identity.is_some()
        || session
            .handles
            .values()
            .any(|object| matches!(object, MapiObject::NotificationSubscription { .. }))
    {
        MAPI_NOTIFICATION_WAIT_SUBSCRIPTION_EMPTY_DELAY_MILLIS
    } else {
        MAPI_NOTIFICATION_WAIT_EMPTY_DELAY_MILLIS
    }
}

fn notification_wait_empty_response(
    endpoint: MapiEndpoint,
    request_id: &str,
    session_id: &str,
) -> Response {
    mapi_response_with_cookies(
        "NotificationWait",
        request_id,
        0,
        notification_wait_body(false),
        session_context_cookies(endpoint, session_id, false),
    )
}

async fn acquire_notification_wait_active_session_request(
    session_id: &str,
) -> Option<ActiveSessionRequest> {
    for attempt in 0..MAPI_NOTIFICATION_WAIT_REACQUIRE_RETRY_ATTEMPTS {
        if let Some(active_request) = begin_active_session_request(session_id) {
            return Some(active_request);
        }
        if attempt + 1 < MAPI_NOTIFICATION_WAIT_REACQUIRE_RETRY_ATTEMPTS {
            tokio::time::sleep(std::time::Duration::from_millis(
                MAPI_NOTIFICATION_WAIT_REACQUIRE_RETRY_DELAY_MS,
            ))
            .await;
        }
    }
    None
}

fn session_id_prefix(session_id: &str) -> &str {
    session_id
        .char_indices()
        .nth(8)
        .map(|(index, _)| &session_id[..index])
        .unwrap_or(session_id)
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

pub(in crate::mapi) fn mapi_diagnostic_response(
    request_type: &str,
    request_id: &str,
    response_code: u16,
    message: &str,
) -> Response {
    mapi_diagnostic_response_with_cookies(
        request_type,
        request_id,
        response_code,
        message,
        Vec::new(),
    )
}

fn mapi_diagnostic_response_with_cookies(
    request_type: &str,
    request_id: &str,
    response_code: u16,
    message: &str,
    cookies: Vec<String>,
) -> Response {
    let mut response = mapi_response_with_cookies(
        request_type,
        request_id,
        response_code,
        message.as_bytes().to_vec(),
        cookies,
    );
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("text/html"));
    response
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
        payload: body,
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
    let duration = time
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    let date = utc_from_unix_seconds(duration.as_secs());
    let weekday = weekday_abbrev_from_unix_days(date.unix_days);
    let month = month_abbrev(date.month).unwrap_or("Jan");
    format!(
        "{weekday}, {day:02} {month} {year:04} {hour:02}:{minute:02}:{second:02} GMT",
        day = date.day,
        year = date.year,
        hour = date.hour,
        minute = date.minute,
        second = date.second
    )
}

#[derive(Clone, Debug)]
pub(in crate::mapi) struct MapiResponseDebug {
    payload_bytes: usize,
    payload: Vec<u8>,
}

pub(crate) fn mapi_response_payload_bytes(response: &Response) -> Option<usize> {
    response
        .extensions()
        .get::<MapiResponseDebug>()
        .map(|debug| debug.payload_bytes)
}

pub(in crate::mapi) fn mapi_response_payload(response: &Response) -> Option<&[u8]> {
    response
        .extensions()
        .get::<MapiResponseDebug>()
        .map(|debug| debug.payload.as_slice())
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
    trace_mapi_connection(
        endpoint,
        principal,
        headers,
        request_body,
        request_type,
        request_id,
        response,
        payload_bytes,
    );
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
        tracing::debug!(
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

fn trace_mapi_connection(
    endpoint: MapiEndpoint,
    principal: &AccountPrincipal,
    headers: &HeaderMap,
    request_body: &[u8],
    request_type: &str,
    request_id: &str,
    response: &Response,
    response_payload_bytes: usize,
) {
    let endpoint_label = match endpoint {
        MapiEndpoint::Emsmdb => "emsmdb",
        MapiEndpoint::Nspi => "nspi",
    };
    let session_key = request_cookie(endpoint, headers)
        .unwrap_or_else(|| format!("{endpoint_label}:{request_id}"));
    let remote_peer = remote_peer(headers);
    let tenant_id = principal.tenant_id.to_string();
    let account_id = principal.account_id.to_string();
    let status = response.status().as_u16();
    let response_code = response_header(response, "x-responsecode").unwrap_or_default();
    let trace_id = safe_header(headers, "x-trace-id").unwrap_or_default();
    let client_request_id = safe_header(headers, "client-request-id").unwrap_or_default();
    let client_info = safe_header(headers, "x-clientinfo").unwrap_or_default();
    let client_application = safe_header(headers, "x-clientapplication").unwrap_or_default();
    let user_agent = safe_header(headers, "user-agent").unwrap_or_default();
    let execute_trace_metadata = execute_request_trace_metadata(request_type, request_body);
    let execute_response_trace_metadata = execute_response_trace_metadata(
        request_type,
        request_body,
        mapi_response_payload(response).unwrap_or_default(),
    );

    let mut inbound_metadata = vec![
        ("account_id", account_id.clone()),
        ("mapi_request_id", request_id.to_string()),
        ("trace_id", trace_id.clone()),
        ("client_request_id", client_request_id.clone()),
        ("client_info", client_info.clone()),
        ("client_application", client_application.clone()),
        ("user_agent", user_agent.clone()),
    ];
    inbound_metadata.extend(execute_trace_metadata.clone());
    write_outlook_trace(&OutlookTraceEvent {
        component: "mapi",
        endpoint: endpoint_label,
        session_key: &session_key,
        direction: OutlookTraceDirection::Inbound,
        phase: request_type,
        remote_peer: remote_peer.as_deref(),
        tenant_id: Some(&tenant_id),
        account: Some(&principal.email),
        status: None,
        metadata: inbound_metadata,
        payload: Some(request_body),
    });
    let mut outbound_metadata = vec![
        ("account_id", account_id),
        ("mapi_request_id", request_id.to_string()),
        ("mapi_response_code", response_code),
        ("trace_id", trace_id),
        ("client_request_id", client_request_id),
        ("client_info", client_info),
        ("client_application", client_application),
        ("user_agent", user_agent),
        ("response_payload_bytes", response_payload_bytes.to_string()),
    ];
    outbound_metadata.extend(execute_trace_metadata);
    outbound_metadata.extend(execute_response_trace_metadata);
    write_outlook_trace(&OutlookTraceEvent {
        component: "mapi",
        endpoint: endpoint_label,
        session_key: &session_key,
        direction: OutlookTraceDirection::Outbound,
        phase: request_type,
        remote_peer: remote_peer.as_deref(),
        tenant_id: Some(&tenant_id),
        account: Some(&principal.email),
        status: Some(status),
        metadata: outbound_metadata,
        payload: mapi_response_payload(response),
    });
}

fn execute_response_trace_metadata(
    request_type: &str,
    request_body: &[u8],
    response_body: &[u8],
) -> Vec<(&'static str, String)> {
    if request_type != "Execute" {
        return Vec::new();
    }
    let request_summary = match parse_execute_request(request_body) {
        Ok(execute) => summarize_request_rop_buffer(&execute.rop_buffer),
        Err(error) => {
            return vec![("response_rop_parse_error", format!("request:{error}"))];
        }
    };
    let response_rop_buffer = match execute_response_rop_buffer_for_trace(response_body) {
        Ok(buffer) => buffer,
        Err(error) => return vec![("response_rop_parse_error", error)],
    };
    let response_summary = summarize_response_rop_buffer_with_expected_handles(
        response_rop_buffer,
        &request_summary.full_ids,
        &request_summary.full_response_handle_indexes,
    );

    vec![
        (
            "response_rop_buffer_bytes",
            response_summary
                .response_payload_bytes
                .saturating_add(2)
                .to_string(),
        ),
        (
            "response_rop_buffer_preview",
            hex_preview(response_rop_buffer, 96),
        ),
        ("response_rop_ids", response_summary.ids_csv),
        ("response_rop_names", response_summary.names_csv),
        ("response_rop_results", response_summary.results_csv),
        ("response_rop_count", response_summary.count.to_string()),
        (
            "response_handle_table_bytes",
            response_summary.handle_table_bytes.to_string(),
        ),
        ("response_rop_frames", response_summary.frames),
        ("response_rop_parse_error", response_summary.parse_error),
    ]
}

fn execute_response_rop_buffer_for_trace(response_body: &[u8]) -> Result<&[u8], String> {
    let status = response_body
        .get(0..4)
        .and_then(|bytes| bytes.try_into().ok())
        .map(u32::from_le_bytes)
        .ok_or_else(|| "truncated_execute_response_status".to_string())?;
    if status != 0 {
        return Err(format!("execute_status_{status:#010x}"));
    }
    let error = response_body
        .get(4..8)
        .and_then(|bytes| bytes.try_into().ok())
        .map(u32::from_le_bytes)
        .ok_or_else(|| "truncated_execute_response_error".to_string())?;
    if error != 0 {
        return Err(format!("execute_error_{error:#010x}"));
    }
    let rop_buffer_len = response_body
        .get(12..16)
        .and_then(|bytes| bytes.try_into().ok())
        .map(u32::from_le_bytes)
        .ok_or_else(|| "truncated_execute_response_rop_buffer_length".to_string())?
        as usize;
    response_body
        .get(16..16 + rop_buffer_len)
        .ok_or_else(|| "truncated_execute_response_rop_buffer".to_string())
}

fn execute_request_trace_metadata(
    request_type: &str,
    request_body: &[u8],
) -> Vec<(&'static str, String)> {
    if request_type != "Execute" {
        return Vec::new();
    }
    match parse_execute_request(request_body) {
        Ok(execute) => {
            let summary = summarize_request_rop_buffer(&execute.rop_buffer);
            vec![
                ("request_rop_ids", summary.ids_csv),
                ("request_rop_names", summary.names_csv),
                ("request_rop_count", summary.total_count.to_string()),
                ("request_non_release_rops", summary.non_release_rops),
                (
                    "request_all_rops_are_release",
                    summary.all_release.to_string(),
                ),
                ("request_handle_count", summary.handle_count.to_string()),
                ("request_handle_table", summary.handle_table_summary),
                ("request_rop_parse_error", summary.parse_error),
            ]
        }
        Err(error) => vec![("request_execute_parse_error", error.to_string())],
    }
}

fn remote_peer(headers: &HeaderMap) -> Option<String> {
    safe_header(headers, "x-forwarded-for")
        .and_then(|value| value.split(',').next().map(|part| part.trim().to_string()))
        .filter(|value| !value.is_empty())
        .or_else(|| safe_header(headers, "x-real-ip"))
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

pub(in crate::mapi) fn execute_transport_failure_response(
    request_id: &str,
    response_code: u16,
    message: &str,
    cookies: Vec<String>,
) -> Response {
    mapi_diagnostic_response_with_cookies("Execute", request_id, response_code, message, cookies)
}

pub(in crate::mapi) fn insert_header(response: &mut Response, name: &'static str, value: &str) {
    if let Ok(value) = HeaderValue::from_str(value) {
        response.headers_mut().insert(name, value);
    }
}

pub(in crate::mapi) fn is_authentication_error(message: &str) -> bool {
    matches!(
        message,
        "missing account authentication" | "invalid credentials"
    ) || message.contains("oauth access token")
}

#[cfg(test)]
pub(in crate::mapi) mod tests;
