use super::*;
use axum::{
    body::{Body, Bytes},
    http::{
        header::{CONTENT_TYPE, SET_COOKIE, TRANSFER_ENCODING},
        HeaderMap, HeaderValue, StatusCode,
    },
    response::Response,
};
use std::{io, time::Duration};
use tokio_stream::wrappers::ReceiverStream;

pub(in crate::mapi) const MAPI_NOTIFICATION_WAIT_PENDING_PERIOD_MILLIS: u32 = 15_000;
pub(in crate::mapi) const MAPI_NOTIFICATION_WAIT_MAXIMUM_WAIT: Duration = Duration::from_secs(300);
const MAPI_NOTIFICATION_WAIT_REACQUIRE_RETRY_ATTEMPTS: usize = 200;
const MAPI_NOTIFICATION_WAIT_REACQUIRE_RETRY_DELAY_MS: u64 = 10;

type NotificationWaitFrame = std::result::Result<Bytes, io::Error>;
type NotificationWaitSender = tokio::sync::mpsc::Sender<NotificationWaitFrame>;

#[derive(Clone)]
pub(in crate::mapi) struct DeferredNotificationWaitTrace;

/// [MS-OXCMAPIHTTP] sections 2.2.7, 3.2.2, 3.2.5.2, and 3.2.5.5 require an
/// immediate chunked PROCESSING response, PENDING keep-alives, and a final
/// DONE response while retaining the Session Context for up to five minutes.
pub(in crate::mapi) async fn notification_wait_response<S>(
    store: S,
    endpoint: MapiEndpoint,
    principal: &AccountPrincipal,
    headers: &HeaderMap,
    request_id: &str,
) -> Response
where
    S: ExchangeStore + Send + Sync + 'static,
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
    let Some(active_request) = acquire_notification_wait_active_session_request(&session_id).await
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
            "notification wait overlap returned an empty response"
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

    // The Session Context remains available while the long poll is pending.
    store_session(session_id.clone(), session);
    drop(active_request);

    let (sender, receiver) = tokio::sync::mpsc::channel(4);
    if sender
        .try_send(Ok(Bytes::from_static(b"PROCESSING\r\n")))
        .is_err()
    {
        return notification_wait_empty_response(endpoint, request_id, &session_id);
    }

    let request_headers = headers.clone();
    let principal = principal.clone();
    let request_id = request_id.to_string();
    let task_request_id = request_id.clone();
    let stream_session_id = session_id.clone();
    tokio::spawn(async move {
        run_notification_wait(
            store,
            endpoint,
            principal,
            request_headers,
            task_request_id,
            stream_session_id,
            sender,
        )
        .await;
    });

    notification_wait_streaming_response(endpoint, request_id.as_str(), &session_id, receiver)
}

async fn run_notification_wait<S>(
    store: S,
    endpoint: MapiEndpoint,
    principal: AccountPrincipal,
    request_headers: HeaderMap,
    request_id: String,
    session_id: String,
    sender: NotificationWaitSender,
) where
    S: ExchangeStore + Send + Sync + 'static,
{
    let started_at = std::time::Instant::now();
    let start_time = std::time::SystemTime::now();
    let deadline = tokio::time::Instant::now() + MAPI_NOTIFICATION_WAIT_MAXIMUM_WAIT;
    let pending_period =
        Duration::from_millis(u64::from(MAPI_NOTIFICATION_WAIT_PENDING_PERIOD_MILLIS));

    loop {
        let outcome =
            notification_wait_event_pending(&store, endpoint, &principal, &session_id).await;
        let (event_pending, response_code) = match outcome {
            Ok(Some(event_pending)) => (event_pending, 0),
            Ok(None) => (false, 0),
            Err(response_code) => (false, response_code),
        };
        if event_pending || response_code != 0 || tokio::time::Instant::now() >= deadline {
            let body = if response_code == 0 {
                notification_wait_body(event_pending)
            } else {
                Vec::new()
            };
            complete_notification_wait(
                endpoint,
                &principal,
                &request_headers,
                &request_id,
                &session_id,
                response_code,
                body,
                started_at,
                start_time,
                sender,
            )
            .await;
            return;
        }
        if sender.is_closed() {
            return;
        }

        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        tokio::time::sleep(remaining.min(pending_period)).await;
        if tokio::time::Instant::now() < deadline
            && sender
                .send(Ok(Bytes::from_static(b"PENDING\r\n")))
                .await
                .is_err()
        {
            return;
        }
    }
}

async fn notification_wait_event_pending<S>(
    store: &S,
    endpoint: MapiEndpoint,
    principal: &AccountPrincipal,
    session_id: &str,
) -> std::result::Result<Option<bool>, u16>
where
    S: ExchangeStore,
{
    let Some(active_request) = acquire_notification_wait_active_session_request(session_id).await
    else {
        return Ok(None);
    };
    let Some(mut session) = remove_session(session_id) else {
        return Err(10);
    };
    if !session_matches(&session, endpoint, principal) {
        return Err(10);
    }

    if session.pending_notification_count() == 0 {
        if let Some(cursor) = session.notification_cursor {
            if let Ok(poll) = store
                .poll_mapi_notifications(principal.account_id, cursor)
                .await
            {
                for event in session.matching_notifications(poll.events) {
                    session.record_notification(event);
                }
                session.notification_cursor = poll.cursor.or(Some(cursor));
            }
        }
    }
    let event_pending = session.pending_notification_count() != 0;
    store_session(session_id.to_string(), session);
    drop(active_request);
    Ok(Some(event_pending))
}

#[allow(clippy::too_many_arguments)]
async fn complete_notification_wait(
    endpoint: MapiEndpoint,
    principal: &AccountPrincipal,
    request_headers: &HeaderMap,
    request_id: &str,
    session_id: &str,
    response_code: u16,
    body: Vec<u8>,
    started_at: std::time::Instant,
    start_time: std::time::SystemTime,
    sender: NotificationWaitSender,
) {
    let final_frame = notification_wait_final_frame(response_code, &body, started_at, start_time);
    if sender.send(Ok(final_frame)).await.is_err() {
        return;
    }

    let response = finalize_mapi_response(
        notification_wait_trace_response(endpoint, request_id, session_id, response_code, body),
        request_headers,
    );
    log_mapi_connection(
        endpoint,
        principal,
        request_headers,
        b"",
        "NotificationWait",
        request_id,
        &response,
    );
}

fn notification_wait_final_frame(
    response_code: u16,
    body: &[u8],
    started_at: std::time::Instant,
    start_time: std::time::SystemTime,
) -> Bytes {
    let mut frame = Vec::new();
    frame.extend_from_slice(b"DONE\r\n");
    frame.extend_from_slice(format!("X-ResponseCode: {response_code}\r\n").as_bytes());
    frame.extend_from_slice(
        format!("X-ElapsedTime: {}\r\n", started_at.elapsed().as_millis()).as_bytes(),
    );
    frame.extend_from_slice(format!("X-StartTime: {}\r\n", mapi_http_date(start_time)).as_bytes());
    frame.extend_from_slice(b"\r\n");
    frame.extend_from_slice(body);
    Bytes::from(frame)
}

fn notification_wait_streaming_response(
    endpoint: MapiEndpoint,
    request_id: &str,
    session_id: &str,
    receiver: tokio::sync::mpsc::Receiver<NotificationWaitFrame>,
) -> Response {
    let mut response = Response::new(Body::from_stream(ReceiverStream::new(receiver)));
    response.extensions_mut().insert(MapiResponseDebug {
        payload_bytes: 0,
        payload: Vec::new(),
    });
    response
        .extensions_mut()
        .insert(DeferredNotificationWaitTrace);
    decorate_notification_wait_response(&mut response, endpoint, request_id, 0, session_id);
    response
}

fn notification_wait_trace_response(
    endpoint: MapiEndpoint,
    request_id: &str,
    session_id: &str,
    response_code: u16,
    body: Vec<u8>,
) -> Response {
    let mut response = Response::new(Body::empty());
    response.extensions_mut().insert(MapiResponseDebug {
        payload_bytes: body.len(),
        payload: body,
    });
    decorate_notification_wait_response(
        &mut response,
        endpoint,
        request_id,
        response_code,
        session_id,
    );
    response
}

fn decorate_notification_wait_response(
    response: &mut Response,
    endpoint: MapiEndpoint,
    request_id: &str,
    response_code: u16,
    session_id: &str,
) {
    *response.status_mut() = StatusCode::OK;
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static(MAPI_CONTENT_TYPE));
    response
        .headers_mut()
        .insert(TRANSFER_ENCODING, HeaderValue::from_static("chunked"));
    insert_header(response, "x-requesttype", "NotificationWait");
    insert_header(response, "x-responsecode", &response_code.to_string());
    insert_header(response, "x-requestid", request_id);
    insert_header(response, "x-serverapplication", MAPI_SERVER_APPLICATION);
    for cookie in session_context_cookies(endpoint, session_id, false) {
        if let Ok(value) = HeaderValue::from_str(&cookie) {
            response.headers_mut().append(SET_COOKIE, value);
        }
    }
}

pub(in crate::mapi) fn notification_wait_empty_response(
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

pub(in crate::mapi) async fn acquire_notification_wait_active_session_request(
    session_id: &str,
) -> Option<ActiveSessionRequest> {
    for attempt in 0..MAPI_NOTIFICATION_WAIT_REACQUIRE_RETRY_ATTEMPTS {
        if let Some(active_request) = begin_active_session_request(session_id) {
            return Some(active_request);
        }
        if attempt + 1 < MAPI_NOTIFICATION_WAIT_REACQUIRE_RETRY_ATTEMPTS {
            tokio::time::sleep(Duration::from_millis(
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
