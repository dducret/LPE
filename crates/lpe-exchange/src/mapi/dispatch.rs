use super::notifications::*;
use super::permissions::*;
use super::properties::*;
use super::rop::*;
use super::session::*;
use super::store_adapter::*;
use super::sync::*;
use super::tables::*;
use super::transport::*;
use super::wire::{FastTransferMarker, MapiPropertyType, MapiSyncType, RopId};
use super::*;
use crate::store::{
    MapiCustomPropertyObjectKind, MapiCustomPropertyValue, MapiSyncChangeSet, MapiSyncCheckpoint,
    UpsertMapiNavigationShortcutInput,
};

const HIERARCHY_SYNC_CURSOR_VERSION: u64 = 2;

async fn hard_delete_folder_contents<S: ExchangeStore>(
    store: &S,
    principal: &AccountPrincipal,
    folder_id: u64,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
) -> Result<(bool, bool), u32> {
    let mailbox = role_for_folder_id(folder_id)
        .and_then(|role| mailboxes.iter().find(|mailbox| mailbox.role == role))
        .or_else(|| {
            mailboxes.iter().find(|mailbox| {
                crate::mapi::identity::mapped_mapi_object_id(&mailbox.id) == Some(folder_id)
            })
        })
        .ok_or(0x8004_010Fu32)?;

    if !snapshot
        .folder_access_for_principal(folder_id, principal.account_id)
        .map(|access| access.may_delete)
        .unwrap_or(true)
    {
        return Err(0x8007_0005);
    }

    let mut partial_completion = false;
    let mut deleted_any = false;
    let message_ids = emails
        .iter()
        .filter(|email| email_matches_folder(email, folder_id, mailboxes))
        .map(|email| email.id)
        .collect::<Vec<_>>();
    let attempted_count = message_ids.len();
    let mut succeeded_count = 0usize;
    let mut failed_count = 0usize;

    for message_id in message_ids {
        if store
            .delete_jmap_email_from_mailbox(
                principal.account_id,
                mailbox.id,
                message_id,
                AuditEntryInput {
                    actor: principal.email.clone(),
                    action: "mapi-hard-delete-folder-contents".to_string(),
                    subject: format!("folder:{} message:{}", mailbox.id, message_id),
                },
            )
            .await
            .is_err()
        {
            partial_completion = true;
            failed_count += 1;
        } else {
            deleted_any = true;
            succeeded_count += 1;
        }
    }
    tracing::info!(
        rca_debug = true,
        adapter = "mapi",
        mailbox = %principal.email,
        folder_id = %format!("{folder_id:#018x}"),
        folder_role = debug_role_for_folder_id(folder_id),
        attempted_count,
        succeeded_count,
        failed_count,
        partial_completion,
        message = "rca debug mapi hard delete folder contents"
    );
    record_mapi_folder_purge_metrics(
        attempted_count,
        succeeded_count,
        failed_count,
        partial_completion,
    );
    Ok((deleted_any, partial_completion))
}

pub(in crate::mapi) async fn execute_response<S, V>(
    store: &S,
    validator: &Validator<V>,
    endpoint: MapiEndpoint,
    principal: &AccountPrincipal,
    headers: &HeaderMap,
    body: &[u8],
    request_id: &str,
) -> Response
where
    S: ExchangeStore,
    V: Detector,
{
    log_session_cookie_lookup(endpoint, principal, headers, "Execute");
    let Some(session_id) = request_cookie(endpoint, headers) else {
        return execute_failure_response(request_id, 13, "missing MAPI session cookie", None);
    };
    if !request_sequence_cookie_matches(endpoint, headers, &session_id) {
        return execute_failure_response(
            request_id,
            6,
            "invalid MAPI request sequence cookie",
            None,
        );
    }
    let Some(_active_request) = begin_active_session_request(&session_id) else {
        return execute_failure_response(
            request_id,
            15,
            "MAPI session already has an active request",
            None,
        );
    };
    let Some(mut session) = get_session(&session_id) else {
        return execute_failure_response(request_id, 10, "MAPI session context not found", None);
    };
    if session.endpoint != endpoint
        || session.tenant_id != principal.tenant_id
        || session.account_id != principal.account_id
        || session.email != principal.email
    {
        return execute_failure_response(
            request_id,
            10,
            "MAPI authentication context changed",
            None,
        );
    }
    session.record_transport_request("Execute", request_id);

    let execute = match parse_execute_request(body) {
        Ok(execute) => execute,
        Err(error) => {
            return execute_failure_response(
                request_id,
                4,
                &format!("invalid Execute request body: {error}"),
                Some(session_cookie(endpoint, &session_id, false)),
            );
        }
    };
    if !session_matches(&session, endpoint, principal) {
        return execute_failure_response(
            request_id,
            10,
            "MAPI authentication context changed",
            Some(session_cookie(endpoint, &session_id, false)),
        );
    }
    let rop_fingerprint = mapi_payload_fingerprint(&execute.rop_buffer);
    let request_debug = summarize_request_rop_buffer(&execute.rop_buffer);
    log_execute_request_start_debug(
        endpoint,
        principal,
        headers,
        request_id,
        body.len(),
        &execute.rop_buffer,
        &request_debug,
    );
    let hierarchy_completed_before_execute = session.hierarchy_sync_completed();
    if let Some(cached) = session.completed_execute_requests.get(request_id).cloned() {
        if cached.rop_fingerprint == rop_fingerprint {
            let post_hierarchy_observation =
                if endpoint == MapiEndpoint::Emsmdb && hierarchy_completed_before_execute {
                    session.record_execute_after_hierarchy_completion(&request_debug.ids)
                } else {
                    PostHierarchyExecuteObservation::default()
                };
            let cached_rop_buffer = execute_success_rop_buffer(&cached.response_body);
            log_execute_rop_debug(
                endpoint,
                principal,
                headers,
                &session_id,
                request_id,
                &request_debug,
                &execute.rop_buffer,
                cached_rop_buffer.unwrap_or_default(),
                &session,
                post_hierarchy_observation,
            );
            store_session(session_id.clone(), session);
            return mapi_response_with_cookies(
                "Execute",
                request_id,
                0,
                cached.response_body,
                session_context_cookies(endpoint, &session_id, false),
            );
        }
        store_session(session_id.clone(), session);
        return execute_failure_response(
            request_id,
            12,
            "reused MAPI Execute request id with a different ROP payload",
            Some(session_cookie(endpoint, &session_id, false)),
        );
    }

    if rop_buffer_has_no_requests(&execute.rop_buffer)
        || rop_buffer_is_store_independent_logon(&execute.rop_buffer)
    {
        let snapshot = MapiMailStoreSnapshot::empty();
        let mailboxes = snapshot.mailboxes();
        let emails = snapshot.emails();
        log_execute_dispatch_start_debug(
            endpoint,
            principal,
            headers,
            request_id,
            mailboxes.len(),
            emails.len(),
        );
        let rop_buffer = execute_rops(
            store,
            principal,
            &mut session,
            &mailboxes,
            &emails,
            &snapshot,
            validator,
            &execute.rop_buffer,
        )
        .await;
        let post_hierarchy_observation =
            if endpoint == MapiEndpoint::Emsmdb && hierarchy_completed_before_execute {
                session.record_execute_after_hierarchy_completion(&request_debug.ids)
            } else {
                PostHierarchyExecuteObservation::default()
            };
        log_execute_rop_debug(
            endpoint,
            principal,
            headers,
            &session_id,
            request_id,
            &request_debug,
            &execute.rop_buffer,
            &rop_buffer,
            &session,
            post_hierarchy_observation,
        );
        let response_body = execute_success_body(rop_buffer, Vec::new());
        let response_debug = summarize_response_rop_buffer(
            execute_success_rop_buffer(&response_body).unwrap_or_default(),
            &request_debug.ids,
        );
        cache_execute_response(
            &mut session,
            request_id,
            rop_fingerprint,
            &response_body,
            request_debug.ids_csv.clone(),
            response_debug.ids_csv,
            response_debug.results_csv,
            response_debug.response_payload_bytes,
        );
        store_session(session_id.clone(), session);
        return mapi_response_with_cookies(
            "Execute",
            request_id,
            0,
            response_body,
            session_context_cookies(endpoint, &session_id, false),
        );
    }

    let access_plan = plan_mapi_store_access(&session, &execute.rop_buffer);
    log_execute_store_access_debug(endpoint, principal, headers, request_id, &access_plan);
    let snapshot = match load_mapi_store_for_access_plan(
        store,
        principal.account_id,
        &access_plan,
        500,
    )
    .await
    {
        Ok(snapshot) => snapshot,
        Err(error) => {
            if let Some(fallback_plan) = hierarchy_sync_selective_fallback_plan(&execute.rop_buffer)
            {
                tracing::warn!(
                    rca_debug = true,
                    adapter = "mapi",
                    endpoint = "emsmdb",
                    tenant_id = %principal.tenant_id,
                    account_id = %principal.account_id,
                    mailbox = %principal.email,
                    request_type = "Execute",
                    mapi_request_id = request_id,
                    full_snapshot_error = %format!("{error:#}"),
                    "rca debug mapi full snapshot fallback to hierarchy store view"
                );
                match load_mapi_store_for_access_plan(
                    store,
                    principal.account_id,
                    &fallback_plan,
                    500,
                )
                .await
                {
                    Ok(snapshot) => snapshot,
                    Err(fallback_error) => {
                        store_session(session_id.clone(), session);
                        return execute_failure_response(
                                request_id,
                                4,
                                &format!(
                                    "failed to load MAPI mail store view: {error:#}; fallback failed: {fallback_error:#}"
                                ),
                                Some(session_cookie(endpoint, &session_id, false)),
                            );
                    }
                }
            } else {
                store_session(session_id.clone(), session);
                return execute_failure_response(
                    request_id,
                    4,
                    &format!("failed to load MAPI mail store view: {error:#}"),
                    Some(session_cookie(endpoint, &session_id, false)),
                );
            }
        }
    };
    let mailboxes = snapshot.mailboxes();
    let emails = snapshot.emails();
    log_execute_dispatch_start_debug(
        endpoint,
        principal,
        headers,
        request_id,
        mailboxes.len(),
        emails.len(),
    );
    let rop_buffer = execute_rops(
        store,
        principal,
        &mut session,
        &mailboxes,
        &emails,
        &snapshot,
        validator,
        &execute.rop_buffer,
    )
    .await;
    let post_hierarchy_observation =
        if endpoint == MapiEndpoint::Emsmdb && hierarchy_completed_before_execute {
            session.record_execute_after_hierarchy_completion(&request_debug.ids)
        } else {
            PostHierarchyExecuteObservation::default()
        };
    log_execute_rop_debug(
        endpoint,
        principal,
        headers,
        &session_id,
        request_id,
        &request_debug,
        &execute.rop_buffer,
        &rop_buffer,
        &session,
        post_hierarchy_observation,
    );
    let response_body = execute_success_body(rop_buffer, Vec::new());
    let response_debug = summarize_response_rop_buffer(
        execute_success_rop_buffer(&response_body).unwrap_or_default(),
        &request_debug.ids,
    );
    cache_execute_response(
        &mut session,
        request_id,
        rop_fingerprint,
        &response_body,
        request_debug.ids_csv.clone(),
        response_debug.ids_csv,
        response_debug.results_csv,
        response_debug.response_payload_bytes,
    );
    store_session(session_id.clone(), session);
    mapi_response_with_cookies(
        "Execute",
        request_id,
        0,
        response_body,
        session_context_cookies(endpoint, &session_id, false),
    )
}

pub(in crate::mapi) struct ExecuteRequest {
    rop_buffer: Vec<u8>,
}

pub(in crate::mapi) fn parse_execute_request(body: &[u8]) -> Result<ExecuteRequest> {
    let mut cursor = Cursor::new(body);
    let _flags = cursor.read_u32()?;
    let rop_buffer_size = cursor.read_u32()? as usize;
    let rop_buffer = cursor.read_bytes(rop_buffer_size)?.to_vec();
    let _max_rop_out = cursor.read_u32()?;
    let auxiliary_buffer_size = cursor.read_u32()? as usize;
    let _auxiliary_buffer = cursor.read_bytes(auxiliary_buffer_size)?;
    Ok(ExecuteRequest { rop_buffer })
}

fn rop_buffer_is_store_independent_logon(rop_buffer: &[u8]) -> bool {
    let Some((requests, _handle_table)) = split_rop_buffer(rop_buffer) else {
        return false;
    };
    let mut cursor = Cursor::new(requests);
    let mut saw_request = false;
    while cursor.remaining() > 0 {
        let Ok(request) = read_rop_request(&mut cursor) else {
            return false;
        };
        if !matches!(RopId::from_u8(request.rop_id), Some(RopId::Logon)) {
            return false;
        }
        saw_request = true;
    }
    saw_request
}

fn rop_buffer_has_no_requests(rop_buffer: &[u8]) -> bool {
    split_rop_buffer(rop_buffer)
        .map(|(requests, _handle_table)| requests.is_empty())
        .unwrap_or(false)
}

fn execute_success_rop_buffer(body: &[u8]) -> Option<&[u8]> {
    let mut cursor = Cursor::new(body);
    cursor.read_u32().ok()?;
    cursor.read_u32().ok()?;
    cursor.read_u32().ok()?;
    let rop_buffer_size = cursor.read_u32().ok()? as usize;
    cursor.read_bytes(rop_buffer_size).ok()
}

const MAX_ROP_DEBUG_ENTRIES: usize = 32;

#[derive(Debug, Default)]
struct RopRequestDebugSummary {
    ids: Vec<u8>,
    ids_csv: String,
    handle_count: usize,
    handle_table_summary: String,
    request_payload_bytes: usize,
    handle_table_bytes: usize,
    raw_frame_count: usize,
    raw_frames: String,
    extended: bool,
    parse_error: String,
}

#[derive(Debug, Default)]
struct RopResponseDebugSummary {
    ids_csv: String,
    results_csv: String,
    count: usize,
    handle_count: usize,
    handle_table_summary: String,
    extended: bool,
    buffer_layout: String,
    buffer_size_word: String,
    response_payload_bytes: usize,
    handle_table_bytes: usize,
    frames: String,
    parse_error: String,
}

#[derive(Debug, Default)]
struct LogonResponseDebugSummary {
    present: bool,
    output_handle_index: String,
    error_code: String,
    logon_flags: String,
    special_folder_ids: String,
    response_flags: String,
    mailbox_guid: String,
    replid: String,
    replica_guid: String,
    parse_error: String,
}

fn log_execute_rop_debug(
    endpoint: MapiEndpoint,
    principal: &AccountPrincipal,
    _headers: &HeaderMap,
    _session_id: &str,
    request_id: &str,
    request: &RopRequestDebugSummary,
    request_rop_buffer: &[u8],
    response_rop_buffer: &[u8],
    session: &MapiSession,
    post_hierarchy_observation: PostHierarchyExecuteObservation,
) {
    let response = summarize_response_rop_buffer(response_rop_buffer, &request.ids);
    let logon = summarize_logon_response_rop(response_rop_buffer, &request.ids);
    let endpoint = match endpoint {
        MapiEndpoint::Emsmdb => "emsmdb",
        MapiEndpoint::Nspi => "nspi",
    };
    let post_hierarchy = post_hierarchy_action_summary(session, false);
    let message = "rca debug mapi execute rops";

    tracing::info!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = endpoint,
        tenant_id = %principal.tenant_id,
        account_id = %principal.account_id,
        mailbox = %principal.email,
        request_type = "Execute",
        mapi_request_id = request_id,
        request_rop_ids = %request.ids_csv,
        request_rop_count = request.ids.len(),
        request_handle_count = request.handle_count,
        input_handle_table_summary = %request.handle_table_summary,
        request_extended_rop_buffer = request.extended,
        request_rop_parse_error = %request.parse_error,
        response_rop_ids = %response.ids_csv,
        response_rop_results_best_effort = %response.results_csv,
        response_rop_count = response.count,
        response_handle_count = response.handle_count,
        output_handle_table_summary = %response.handle_table_summary,
        response_extended_rop_buffer = response.extended,
        response_rop_parse_error = %response.parse_error,
        last_completed_hierarchy_sync_root = %post_hierarchy.last_completed_hierarchy_sync_root,
        content_sync_started_after_hierarchy =
            post_hierarchy.content_sync_configure_observed,
        post_hierarchy_execute_count = post_hierarchy.execute_count,
        post_hierarchy_rop_ids_seen = %post_hierarchy.rop_ids_seen,
        logon_response_present = logon.present,
        logon_error_code = %logon.error_code,
        logon_parse_error = %logon.parse_error,
        request_rop_buffer_bytes = request_rop_buffer.len(),
        response_rop_buffer_bytes = response_rop_buffer.len(),
        message = message,
    );

    if logon.present {
        tracing::info!(
            rca_debug = true,
            adapter = "mapi",
            endpoint = endpoint,
            tenant_id = %principal.tenant_id,
            account_id = %principal.account_id,
            mailbox = %principal.email,
            request_type = "Execute",
            mapi_request_id = request_id,
            output_handle_index = %logon.output_handle_index,
            logon_error_code = %logon.error_code,
            logon_flags = %logon.logon_flags,
            response_flags = %logon.response_flags,
            special_folder_ids = %logon.special_folder_ids,
            mailbox_guid = %logon.mailbox_guid,
            replid = %logon.replid,
            replica_guid = %logon.replica_guid,
            parse_error = %logon.parse_error,
            message = "rca debug mapi logon response",
        );
    }

    if endpoint == "emsmdb" && !request.parse_error.is_empty() {
        tracing::info!(
            rca_debug = true,
            adapter = "mapi",
            endpoint = endpoint,
            tenant_id = %principal.tenant_id,
            account_id = %principal.account_id,
            mailbox = %principal.email,
            request_type = "Execute",
            mapi_request_id = request_id,
            request_rop_ids = %request.ids_csv,
            request_rop_parse_error = %request.parse_error,
            request_rop_payload_bytes = request.request_payload_bytes,
            request_handle_table_bytes = request.handle_table_bytes,
            request_handle_count = request.handle_count,
            input_handle_table_summary = %request.handle_table_summary,
            request_rop_raw_frame_count = request.raw_frame_count,
            request_rop_raw_frames = %request.raw_frames,
            "rca debug mapi execute request framing"
        );
    }

    if let Some(response_framing_context) =
        execute_response_framing_context(&request.ids).filter(|_| endpoint == "emsmdb")
    {
        tracing::info!(
            rca_debug = true,
            adapter = "mapi",
            endpoint = endpoint,
            tenant_id = %principal.tenant_id,
            account_id = %principal.account_id,
            mailbox = %principal.email,
            request_type = "Execute",
            mapi_request_id = request_id,
            response_framing_context = response_framing_context,
            request_rop_ids = %request.ids_csv,
            response_rop_ids = %response.ids_csv,
            response_rop_results_best_effort = %response.results_csv,
            response_rop_buffer_layout = %response.buffer_layout,
            response_rop_buffer_size_word = %response.buffer_size_word,
            response_rop_payload_bytes = response.response_payload_bytes,
            response_handle_table_bytes = response.handle_table_bytes,
            response_handle_count = response.handle_count,
            output_handle_table_summary = %response.handle_table_summary,
            response_rop_frame_count = response.count,
            response_rop_frames = %response.frames,
            response_rop_parse_error = %response.parse_error,
            "rca debug mapi execute response framing"
        );
    }

    if endpoint == "emsmdb"
        && (post_hierarchy_observation.first_execute
            || post_hierarchy_observation.first_bootstrap_probe
            || post_hierarchy_observation.first_set_properties_probe)
    {
        let probe = summarize_first_post_hierarchy_probe(request_rop_buffer, response_rop_buffer);
        tracing::info!(
            rca_debug = true,
            adapter = "mapi",
            endpoint = endpoint,
            tenant_id = %principal.tenant_id,
            account_id = %principal.account_id,
            mailbox = %principal.email,
            request_type = "Execute",
            mapi_request_id = request_id,
            last_completed_hierarchy_sync_root =
                %post_hierarchy.last_completed_hierarchy_sync_root,
            first_post_hierarchy_execute = post_hierarchy_observation.first_execute,
            first_post_hierarchy_bootstrap_probe =
                post_hierarchy_observation.first_bootstrap_probe,
            first_post_hierarchy_set_properties_probe =
                post_hierarchy_observation.first_set_properties_probe,
            request_rop_ids = %request.ids_csv,
            response_rop_results_best_effort = %response.results_csv,
            open_folder_request_count = probe.open_folder_request_count,
            open_folder_requests = %probe.open_folder_requests,
            open_folder_response_shapes = %probe.open_folder_response_shapes,
            get_properties_specific_request_count = probe.get_properties_specific_request_count,
            get_properties_specific_requests = %probe.get_properties_specific_requests,
            get_properties_specific_response_shapes =
                %probe.get_properties_specific_response_shapes,
            set_properties_request_count = probe.set_properties_request_count,
            set_properties_requests = %probe.set_properties_requests,
            set_properties_response_shapes = %probe.set_properties_response_shapes,
            probe_parse_error = %probe.parse_error,
            "rca debug mapi post hierarchy execute probe"
        );
    }

    if endpoint == "emsmdb" && session.post_hierarchy_actions.set_properties_probe_observed {
        let probe = summarize_first_post_hierarchy_probe(request_rop_buffer, response_rop_buffer);
        let root_logon_requests =
            summarize_root_logon_default_folder_getprops_requests(request_rop_buffer, session);
        if !root_logon_requests.is_empty() {
            tracing::info!(
                rca_debug = true,
                adapter = "mapi",
                endpoint = endpoint,
                tenant_id = %principal.tenant_id,
                account_id = %principal.account_id,
                mailbox = %principal.email,
                request_type = "Execute",
                mapi_request_id = request_id,
                last_completed_hierarchy_sync_root =
                    %post_hierarchy.last_completed_hierarchy_sync_root,
                post_hierarchy_execute_count = post_hierarchy.execute_count,
                request_rop_ids = %request.ids_csv,
                response_rop_results_best_effort = %response.results_csv,
                get_properties_specific_request_count = probe.get_properties_specific_request_count,
                get_properties_specific_requests = %probe.get_properties_specific_requests,
                get_properties_specific_root_logon_requests = %root_logon_requests,
                get_properties_specific_response_shapes =
                    %probe.get_properties_specific_response_shapes,
                probe_parse_error = %probe.parse_error,
                "rca debug mapi post setprops default folder get props"
            );
        }
    }
}

fn log_execute_request_start_debug(
    endpoint: MapiEndpoint,
    principal: &AccountPrincipal,
    _headers: &HeaderMap,
    request_id: &str,
    request_body_bytes: usize,
    request_rop_buffer: &[u8],
    request: &RopRequestDebugSummary,
) {
    let endpoint = match endpoint {
        MapiEndpoint::Emsmdb => "emsmdb",
        MapiEndpoint::Nspi => "nspi",
    };
    let message = "rca debug mapi execute request start";

    tracing::info!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = endpoint,
        tenant_id = %principal.tenant_id,
        account_id = %principal.account_id,
        mailbox = %principal.email,
        request_type = "Execute",
        mapi_request_id = request_id,
        body_bytes = request_body_bytes,
        request_rop_buffer_bytes = request_rop_buffer.len(),
        rop_ids = %request.ids_csv,
        rop_count = request.ids.len(),
        handle_count = request.handle_count,
        handle_table = %request.handle_table_summary,
        extended = request.extended,
        parse_error = %request.parse_error,
        message = message,
    );
}

fn log_execute_store_access_debug(
    endpoint: MapiEndpoint,
    principal: &AccountPrincipal,
    _headers: &HeaderMap,
    request_id: &str,
    access_plan: &MapiAccessPlan,
) {
    let endpoint = match endpoint {
        MapiEndpoint::Emsmdb => "emsmdb",
        MapiEndpoint::Nspi => "nspi",
    };
    let message = "rca debug mapi execute store access";

    tracing::info!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = endpoint,
        tenant_id = %principal.tenant_id,
        account_id = %principal.account_id,
        mailbox = %principal.email,
        request_type = "Execute",
        mapi_request_id = request_id,
        full_snapshot = access_plan.requires_full_snapshot,
        object_id_count = access_plan.object_ids.len(),
        content_query_count = access_plan.content_queries.len(),
        message = message,
    );
}

fn log_execute_dispatch_start_debug(
    endpoint: MapiEndpoint,
    principal: &AccountPrincipal,
    _headers: &HeaderMap,
    request_id: &str,
    mailbox_count: usize,
    email_count: usize,
) {
    let endpoint = match endpoint {
        MapiEndpoint::Emsmdb => "emsmdb",
        MapiEndpoint::Nspi => "nspi",
    };
    let message = "rca debug mapi execute dispatch start";

    tracing::info!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = endpoint,
        tenant_id = %principal.tenant_id,
        account_id = %principal.account_id,
        mailbox = %principal.email,
        request_type = "Execute",
        mapi_request_id = request_id,
        mailbox_count = mailbox_count,
        email_count = email_count,
        message = message,
    );
}

#[derive(Debug, Default, PartialEq, Eq)]
struct FirstPostHierarchyProbeDebugSummary {
    open_folder_request_count: usize,
    open_folder_requests: String,
    open_folder_response_shapes: String,
    get_properties_specific_request_count: usize,
    get_properties_specific_requests: String,
    get_properties_specific_response_shapes: String,
    set_properties_request_count: usize,
    set_properties_requests: String,
    set_properties_response_shapes: String,
    parse_error: String,
}

#[derive(Debug, PartialEq, Eq)]
struct OpenFolderProbeRequest {
    output_handle_index: u8,
    folder_id: u64,
}

#[derive(Debug, PartialEq, Eq)]
struct GetPropertiesSpecificProbeRequest {
    input_handle_index: u8,
    property_tags: Vec<u32>,
}

#[derive(Debug, PartialEq, Eq)]
struct SetPropertiesProbeRequest {
    input_handle_index: u8,
    property_tags: Vec<u32>,
    property_value_shapes: String,
    default_folder_entry_id_values: String,
    parse_error: String,
}

fn summarize_first_post_hierarchy_probe(
    request_rop_buffer: &[u8],
    response_rop_buffer: &[u8],
) -> FirstPostHierarchyProbeDebugSummary {
    let mut summary = FirstPostHierarchyProbeDebugSummary::default();
    let Some((requests, _request_handle_table)) = split_rop_buffer(request_rop_buffer) else {
        summary.parse_error = "invalid request ROP buffer".to_string();
        return summary;
    };
    let mut request_cursor = Cursor::new(requests);
    let mut request_rop_ids = Vec::new();
    let mut open_folder_requests = Vec::new();
    let mut get_properties_requests = Vec::new();
    let mut set_properties_requests = Vec::new();
    while request_cursor.remaining() > 0 {
        let request = match read_rop_request(&mut request_cursor) {
            Ok(request) => request,
            Err(error) => {
                summary.parse_error = error.to_string();
                break;
            }
        };
        let rop_id = request.typed().rop_id();
        request_rop_ids.push(rop_id);
        match rop_id {
            0x02 => open_folder_requests.push(OpenFolderProbeRequest {
                output_handle_index: request.output_handle_index.unwrap_or(0),
                folder_id: request.folder_id().unwrap_or(ROOT_FOLDER_ID),
            }),
            0x07 => get_properties_requests.push(GetPropertiesSpecificProbeRequest {
                input_handle_index: request.input_handle_index().unwrap_or(0),
                property_tags: request.property_tags(),
            }),
            0x0A | 0x79 => set_properties_requests.push(set_properties_probe_request(&request)),
            _ => {}
        }
    }

    summary.open_folder_request_count = open_folder_requests.len();
    summary.open_folder_requests = open_folder_requests
        .iter()
        .map(|request| {
            format!(
                "out={};folder=0x{:016x};name={}",
                request.output_handle_index,
                request.folder_id,
                post_hierarchy_probe_folder_name(request.folder_id)
            )
        })
        .collect::<Vec<_>>()
        .join("|");
    summary.get_properties_specific_request_count = get_properties_requests.len();
    summary.get_properties_specific_requests = get_properties_requests
        .iter()
        .map(|request| {
            format!(
                "in={};tags={}",
                request.input_handle_index,
                format_debug_property_tags(&request.property_tags)
            )
        })
        .collect::<Vec<_>>()
        .join("|");
    summary.set_properties_request_count = set_properties_requests.len();
    summary.set_properties_requests = set_properties_requests
        .iter()
        .map(|request| {
            format!(
                "in={};tags={};values={};default_folder_entry_ids={};parse_error={}",
                request.input_handle_index,
                format_debug_property_tags(&request.property_tags),
                request.property_value_shapes,
                request.default_folder_entry_id_values,
                request.parse_error
            )
        })
        .collect::<Vec<_>>()
        .join("|");

    let Some((responses, _response_handle_table)) = split_rop_buffer(response_rop_buffer) else {
        if summary.parse_error.is_empty() {
            summary.parse_error = "invalid response ROP buffer".to_string();
        }
        return summary;
    };
    let mut response_offset = 0usize;
    let mut open_folder_index = 0usize;
    let mut get_properties_index = 0usize;
    let mut set_properties_index = 0usize;
    let mut open_folder_responses = Vec::new();
    let mut get_properties_responses = Vec::new();
    let mut set_properties_responses = Vec::new();
    for rop_id in request_rop_ids {
        if rop_has_no_response(rop_id) {
            continue;
        }
        let Some(found) = responses
            .get(response_offset..)
            .and_then(|remaining| remaining.iter().position(|candidate| *candidate == rop_id))
        else {
            break;
        };
        response_offset += found;
        match rop_id {
            0x02 => {
                if let Some(request) = open_folder_requests.get(open_folder_index) {
                    open_folder_responses.push(summarize_open_folder_probe_response(
                        responses,
                        response_offset,
                        request,
                    ));
                }
                open_folder_index = open_folder_index.saturating_add(1);
            }
            0x07 => {
                if let Some(request) = get_properties_requests.get(get_properties_index) {
                    get_properties_responses.push(summarize_get_properties_probe_response(
                        responses,
                        response_offset,
                        request,
                    ));
                }
                get_properties_index = get_properties_index.saturating_add(1);
            }
            0x0A | 0x79 => {
                if let Some(request) = set_properties_requests.get(set_properties_index) {
                    set_properties_responses.push(summarize_set_properties_probe_response(
                        responses,
                        response_offset,
                        request,
                    ));
                }
                set_properties_index = set_properties_index.saturating_add(1);
            }
            _ => {}
        }
        response_offset = response_offset.saturating_add(6);
    }
    summary.open_folder_response_shapes = open_folder_responses.join("|");
    summary.get_properties_specific_response_shapes = get_properties_responses.join("|");
    summary.set_properties_response_shapes = set_properties_responses.join("|");
    summary
}

fn summarize_root_logon_default_folder_getprops_requests(
    rop_buffer: &[u8],
    session: &MapiSession,
) -> String {
    let Some((requests, handle_table)) = split_rop_buffer(rop_buffer) else {
        return String::new();
    };
    let Ok(input_handles) = read_handle_table(handle_table) else {
        return String::new();
    };
    let mut cursor = Cursor::new(requests);
    let mut opened_root_output_indexes = HashSet::new();
    let mut getprops = Vec::new();
    while cursor.remaining() > 0 {
        let Ok(request) = read_rop_request(&mut cursor) else {
            break;
        };
        match request.typed().rop_id() {
            0x02 if request.folder_id() == Some(ROOT_FOLDER_ID) => {
                if let Some(output_handle_index) = request.output_handle_index {
                    opened_root_output_indexes.insert(output_handle_index);
                }
            }
            0x07 if request
                .property_tags()
                .iter()
                .copied()
                .any(is_default_folder_identification_property_tag) =>
            {
                let input_handle_index = request.input_handle_index().unwrap_or(0);
                let target = if opened_root_output_indexes.contains(&input_handle_index) {
                    Some("root")
                } else {
                    match input_object(session, &input_handles, &request) {
                        Some(MapiObject::Logon) => Some("logon"),
                        Some(MapiObject::Folder { folder_id, .. })
                            if *folder_id == ROOT_FOLDER_ID =>
                        {
                            Some("root")
                        }
                        _ => None,
                    }
                };
                if let Some(target) = target {
                    getprops.push(format!(
                        "in={input_handle_index};target={target};tags={}",
                        format_debug_property_tags(&request.property_tags())
                    ));
                }
            }
            _ => {}
        }
    }
    getprops.join("|")
}

fn format_optional_debug_handle(handle: Option<u32>) -> String {
    handle
        .map(|handle| handle.to_string())
        .unwrap_or_else(|| "none".to_string())
}

fn mapi_object_debug_kind(object: Option<&MapiObject>) -> &'static str {
    match object {
        None => "none",
        Some(MapiObject::Logon) => "logon",
        Some(MapiObject::Folder { .. }) => "folder",
        Some(MapiObject::Message { .. }) => "message",
        Some(MapiObject::Contact { .. }) => "contact",
        Some(MapiObject::Event { .. }) => "event",
        Some(MapiObject::Task { .. }) => "task",
        Some(MapiObject::Note { .. }) => "note",
        Some(MapiObject::JournalEntry { .. }) => "journal_entry",
        Some(MapiObject::ConversationAction { .. }) => "conversation_action",
        Some(MapiObject::NavigationShortcut { .. }) => "navigation_shortcut",
        Some(MapiObject::DelegateFreeBusyMessage { .. }) => "delegate_freebusy_message",
        Some(MapiObject::PendingMessage { .. }) => "pending_message",
        Some(MapiObject::PendingContact { .. }) => "pending_contact",
        Some(MapiObject::PendingEvent { .. }) => "pending_event",
        Some(MapiObject::PendingTask { .. }) => "pending_task",
        Some(MapiObject::PendingNote { .. }) => "pending_note",
        Some(MapiObject::PendingJournalEntry { .. }) => "pending_journal_entry",
        Some(MapiObject::PendingConversationAction { .. }) => "pending_conversation_action",
        Some(MapiObject::PendingNavigationShortcut { .. }) => "pending_navigation_shortcut",
        Some(MapiObject::HierarchyTable { .. }) => "hierarchy_table",
        Some(MapiObject::ContentsTable { .. }) => "contents_table",
        Some(MapiObject::AttachmentTable { .. }) => "attachment_table",
        Some(MapiObject::PermissionTable { .. }) => "permission_table",
        Some(MapiObject::RuleTable { .. }) => "rule_table",
        Some(MapiObject::Attachment { .. }) => "attachment",
        Some(MapiObject::PendingAttachment { .. }) => "pending_attachment",
        Some(MapiObject::SavedAttachment { .. }) => "saved_attachment",
        Some(MapiObject::AttachmentStream { .. }) => "attachment_stream",
        Some(MapiObject::NotificationSubscription { .. }) => "notification_subscription",
        Some(MapiObject::SynchronizationSource { .. }) => "synchronization_source",
        Some(MapiObject::SynchronizationCollector { .. }) => "synchronization_collector",
    }
}

fn mapi_object_debug_folder_id(object: Option<&MapiObject>) -> String {
    object
        .and_then(MapiObject::folder_id)
        .map(|folder_id| format!("0x{folder_id:016x}"))
        .unwrap_or_else(|| "none".to_string())
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct PostHierarchyReleaseDebugEvent {
    input_handle_index: u8,
    handle: String,
    object_kind: String,
    folder_id: String,
    remaining_before: usize,
    remaining_after: usize,
    logon_before_content_sync: bool,
}

fn format_post_hierarchy_release_kinds(events: &[PostHierarchyReleaseDebugEvent]) -> String {
    events
        .iter()
        .map(|event| event.object_kind.as_str())
        .collect::<Vec<_>>()
        .join(",")
}

fn format_post_hierarchy_release_context(events: &[PostHierarchyReleaseDebugEvent]) -> String {
    events
        .iter()
        .map(|event| {
            format!(
                "in={};handle={};kind={};folder={};before={};after={};logon_before_content={}",
                event.input_handle_index,
                event.handle,
                event.object_kind,
                event.folder_id,
                event.remaining_before,
                event.remaining_after,
                event.logon_before_content_sync
            )
        })
        .collect::<Vec<_>>()
        .join("|")
}

async fn folder_properties_for_open<S>(
    store: &S,
    principal: &AccountPrincipal,
    _session: &MapiSession,
    folder_id: u64,
) -> HashMap<u32, MapiValue>
where
    S: ExchangeStore,
{
    let mut properties = HashMap::new();
    if folder_id == IPM_SUBTREE_FOLDER_ID {
        if let Ok(Some(ost_id)) = store
            .fetch_mapi_ipm_subtree_ost_id(principal.account_id)
            .await
        {
            properties.insert(PID_TAG_OST_OSTID, MapiValue::Binary(ost_id));
        }
    }
    properties
}

fn set_properties_probe_request(request: &RopRequest) -> SetPropertiesProbeRequest {
    match request.property_values() {
        Ok(values) => SetPropertiesProbeRequest {
            input_handle_index: request.input_handle_index().unwrap_or(0),
            property_tags: values.iter().map(|(tag, _value)| *tag).collect(),
            property_value_shapes: values
                .iter()
                .map(|(tag, value)| format!("{tag:#010x}:{}", mapi_value_debug_shape(value)))
                .collect::<Vec<_>>()
                .join(","),
            default_folder_entry_id_values: default_folder_entry_id_values_for_debug(&values),
            parse_error: String::new(),
        },
        Err(error) => SetPropertiesProbeRequest {
            input_handle_index: request.input_handle_index().unwrap_or(0),
            property_tags: Vec::new(),
            property_value_shapes: String::new(),
            default_folder_entry_id_values: String::new(),
            parse_error: error.to_string(),
        },
    }
}

fn log_set_properties_specific_debug(
    principal: &AccountPrincipal,
    request: &RopRequest,
    object: Option<&MapiObject>,
    probe: &SetPropertiesProbeRequest,
) {
    let default_folder_identification_values_stripped =
        default_folder_identification_values_stripped_by_safe_values(object, &probe.property_tags);
    let default_folder_entry_id_storage_mode = if default_folder_identification_values_stripped {
        "accepted_canonical_projection_stripped"
    } else if probe.default_folder_entry_id_values.is_empty() {
        "not_default_folder_entry_ids"
    } else if matches!(
        object,
        Some(MapiObject::Folder {
            folder_id: ROOT_FOLDER_ID,
            ..
        })
    ) {
        "accepted_session_projection_not_persisted"
    } else {
        "normal_property_validation"
    };
    tracing::info!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = "emsmdb",
        mailbox = %principal.email,
        request_type = "Execute",
        request_rop_id = %rop_id_hex(request.rop_id),
        input_handle_index = request.input_handle_index().unwrap_or(0),
        response_handle_index = request.response_handle_index(),
        object_kind = mapi_object_debug_kind(object),
        folder_id = %mapi_object_debug_folder_id(object),
        property_tag_count = probe.property_tags.len(),
        property_tags = %format_debug_property_tags(&probe.property_tags),
        property_value_shapes = %probe.property_value_shapes,
        default_folder_entry_id_values = %probe.default_folder_entry_id_values,
        default_folder_identification_values_stripped = default_folder_identification_values_stripped,
        default_folder_entry_id_storage_mode = default_folder_entry_id_storage_mode,
        parse_error = %probe.parse_error,
        "rca debug mapi set properties specific"
    );
}

fn default_folder_entry_id_values_for_debug(values: &[(u32, MapiValue)]) -> String {
    values
        .iter()
        .filter_map(|(tag, value)| {
            let storage_tag = canonical_property_storage_tag(*tag);
            if storage_tag == PID_TAG_ADDITIONAL_REN_ENTRY_IDS {
                return Some(indexed_special_folder_entry_ids_for_debug(
                    storage_tag,
                    "PidTagAdditionalRenEntryIds",
                    value,
                    &[
                        CONFLICTS_FOLDER_ID,
                        SYNC_ISSUES_FOLDER_ID,
                        LOCAL_FAILURES_FOLDER_ID,
                        SERVER_FAILURES_FOLDER_ID,
                        JUNK_FOLDER_ID,
                    ],
                ));
            }
            if storage_tag == PID_TAG_FREE_BUSY_ENTRY_IDS {
                return Some(indexed_special_folder_entry_ids_for_debug(
                    storage_tag,
                    "PidTagFreeBusyEntryIds",
                    value,
                    &[0, 0, 0, FREEBUSY_DATA_FOLDER_ID],
                ));
            }
            let expected_folder_id = default_folder_entry_id_expected_folder_id(storage_tag)?;
            let property_name = default_folder_entry_id_property_name(storage_tag);
            let MapiValue::Binary(bytes) = value else {
                return Some(format!(
                    "{storage_tag:#010x}:{property_name}:value_type={}",
                    mapi_value_debug_shape(value)
                ));
            };
            let decoded_folder_id =
                crate::mapi::identity::object_id_from_folder_identifier_bytes(bytes).unwrap_or(0);
            let decoded_name = if decoded_folder_id == 0 {
                "invalid"
            } else {
                post_hierarchy_probe_folder_name(decoded_folder_id)
            };
            Some(format!(
                "{storage_tag:#010x}:{property_name}:bytes={}:decoded_folder_id=0x{decoded_folder_id:016x}:decoded_name={decoded_name}:expected_folder_id=0x{expected_folder_id:016x}:matches_expected={}",
                bytes.len(),
                decoded_folder_id == expected_folder_id
            ))
        })
        .collect::<Vec<_>>()
        .join(",")
}

fn indexed_special_folder_entry_ids_for_debug(
    storage_tag: u32,
    property_name: &'static str,
    value: &MapiValue,
    expected_folder_ids: &[u64],
) -> String {
    let MapiValue::MultiBinary(values) = value else {
        return format!(
            "{storage_tag:#010x}:{property_name}:value_type={}",
            mapi_value_debug_shape(value)
        );
    };
    let mut summaries = Vec::new();
    for (index, bytes) in values.iter().enumerate() {
        let expected_folder_id = expected_folder_ids.get(index).copied().unwrap_or(0);
        summaries.push(format_indexed_special_folder_entry_id(
            index,
            bytes,
            expected_folder_id,
        ));
    }
    if values.len() < expected_folder_ids.len() {
        summaries.push(format!(
            "omitted_preserved_indexes={}",
            (values.len()..expected_folder_ids.len())
                .map(|index| index.to_string())
                .collect::<Vec<_>>()
                .join("+")
        ));
    }
    format!(
        "{storage_tag:#010x}:{property_name}:count={}:{}",
        values.len(),
        summaries.join(";")
    )
}

fn format_indexed_special_folder_entry_id(
    index: usize,
    bytes: &[u8],
    expected_folder_id: u64,
) -> String {
    if bytes.is_empty() {
        return format!(
            "index={index}:bytes=0:expected_folder_id={}:matches_expected={}",
            format_expected_folder_id_for_debug(expected_folder_id),
            expected_folder_id == 0
        );
    }
    let decoded_folder_id =
        crate::mapi::identity::object_id_from_folder_identifier_bytes(bytes).unwrap_or(0);
    let decoded_name = if decoded_folder_id == 0 {
        "invalid"
    } else {
        post_hierarchy_probe_folder_name(decoded_folder_id)
    };
    format!(
        "index={index}:bytes={}:decoded_folder_id=0x{decoded_folder_id:016x}:decoded_name={decoded_name}:expected_folder_id={}:matches_expected={}",
        bytes.len(),
        format_expected_folder_id_for_debug(expected_folder_id),
        decoded_folder_id == expected_folder_id
    )
}

fn format_expected_folder_id_for_debug(folder_id: u64) -> String {
    if folder_id == 0 {
        "empty".to_string()
    } else {
        format!("0x{folder_id:016x}")
    }
}

fn format_optional_folder_id(folder_id: Option<u64>) -> String {
    folder_id
        .map(|folder_id| format!("0x{folder_id:016x}"))
        .unwrap_or_default()
}

fn default_folder_entry_id_expected_folder_id(tag: u32) -> Option<u64> {
    match canonical_property_storage_tag(tag) {
        PID_TAG_IPM_SUBTREE_ENTRY_ID => Some(IPM_SUBTREE_FOLDER_ID),
        PID_TAG_IPM_OUTBOX_ENTRY_ID => Some(OUTBOX_FOLDER_ID),
        PID_TAG_IPM_WASTEBASKET_ENTRY_ID => Some(TRASH_FOLDER_ID),
        PID_TAG_IPM_SENTMAIL_ENTRY_ID => Some(SENT_FOLDER_ID),
        PID_TAG_VIEWS_ENTRY_ID => Some(VIEWS_FOLDER_ID),
        PID_TAG_COMMON_VIEWS_ENTRY_ID => Some(COMMON_VIEWS_FOLDER_ID),
        PID_TAG_FINDER_ENTRY_ID => Some(SEARCH_FOLDER_ID),
        PID_TAG_IPM_ARCHIVE_ENTRY_ID => Some(ARCHIVE_FOLDER_ID),
        PID_TAG_IPM_APPOINTMENT_ENTRY_ID => Some(CALENDAR_FOLDER_ID),
        PID_TAG_IPM_CONTACT_ENTRY_ID => Some(CONTACTS_FOLDER_ID),
        PID_TAG_IPM_JOURNAL_ENTRY_ID => Some(JOURNAL_FOLDER_ID),
        PID_TAG_IPM_NOTE_ENTRY_ID => Some(NOTES_FOLDER_ID),
        PID_TAG_IPM_TASK_ENTRY_ID => Some(TASKS_FOLDER_ID),
        PID_TAG_REM_ONLINE_ENTRY_ID => Some(REMINDERS_FOLDER_ID),
        PID_TAG_IPM_DRAFTS_ENTRY_ID => Some(DRAFTS_FOLDER_ID),
        _ => None,
    }
}

fn folder_set_property_problems(
    object: Option<&MapiObject>,
    values: &[(u32, MapiValue)],
) -> Vec<(usize, u32, u32)> {
    let Some(MapiObject::Folder { folder_id, .. }) = object else {
        return Vec::new();
    };
    values
        .iter()
        .enumerate()
        .filter_map(|(index, (tag, value))| {
            let storage_tag = canonical_property_storage_tag(*tag);
            if *folder_id == IPM_SUBTREE_FOLDER_ID && storage_tag == PID_TAG_OST_OSTID {
                return match value {
                    MapiValue::Binary(bytes) if !bytes.is_empty() => None,
                    _ => Some((index, *tag, 0x8004_0102)),
                };
            }
            if storage_tag == PID_TAG_ADDITIONAL_REN_ENTRY_IDS {
                if *folder_id != INBOX_FOLDER_ID {
                    return Some((index, *tag, 0x8004_0102));
                }
                return match value {
                    MapiValue::MultiBinary(values) if !values.is_empty() => None,
                    _ => Some((index, *tag, 0x8004_0102)),
                };
            }
            if storage_tag == PID_TAG_FREE_BUSY_ENTRY_IDS {
                if !matches!(*folder_id, ROOT_FOLDER_ID | INBOX_FOLDER_ID) {
                    return Some((index, *tag, 0x8004_0102));
                }
                let MapiValue::MultiBinary(values) = value else {
                    return Some((index, *tag, 0x8004_0102));
                };
                return match values.get(3).and_then(|bytes| {
                    crate::mapi::identity::object_id_from_folder_identifier_bytes(bytes)
                }) {
                    Some(folder_id) if folder_id == FREEBUSY_DATA_FOLDER_ID => None,
                    _ => Some((index, *tag, 0x8004_0102)),
                };
            }
            if !matches!(*folder_id, ROOT_FOLDER_ID | INBOX_FOLDER_ID) {
                return Some((index, *tag, 0x8004_0102));
            }
            if !is_scalar_default_folder_entry_id_property_tag(storage_tag) {
                return Some((index, *tag, 0x8004_0102));
            }
            let Some(expected_folder_id) = default_folder_entry_id_expected_folder_id(storage_tag)
            else {
                return Some((index, *tag, 0x8004_0102));
            };
            let MapiValue::Binary(bytes) = value else {
                return Some((index, *tag, 0x8004_0102));
            };
            match crate::mapi::identity::object_id_from_folder_identifier_bytes(bytes) {
                Some(folder_id) if folder_id == expected_folder_id => None,
                _ => Some((index, *tag, 0x8004_0102)),
            }
        })
        .collect()
}

fn default_folder_identification_safe_property_values(
    principal: &AccountPrincipal,
    object: Option<&MapiObject>,
    values: Vec<(u32, MapiValue)>,
) -> Vec<(u32, MapiValue)> {
    if !strips_any_default_folder_identification_values(object) {
        return values;
    }
    values
        .into_iter()
        .filter_map(|(tag, value)| {
            default_folder_identification_safe_property_value(principal, object, tag, value)
        })
        .collect()
}

fn default_folder_identification_safe_property_value(
    principal: &AccountPrincipal,
    object: Option<&MapiObject>,
    tag: u32,
    value: MapiValue,
) -> Option<(u32, MapiValue)> {
    if !strips_default_folder_identification_value(object, tag) {
        return Some((tag, value));
    }
    match canonical_property_storage_tag(tag) {
        PID_TAG_ADDITIONAL_REN_ENTRY_IDS => {
            if !matches!(
                object,
                Some(MapiObject::Folder {
                    folder_id: INBOX_FOLDER_ID,
                    ..
                })
            ) {
                return None;
            }
            merge_indexed_special_folder_entry_ids(principal, tag, value)
                .map(|value| (canonical_property_storage_tag(tag), value))
        }
        PID_TAG_FREE_BUSY_ENTRY_IDS => {
            merge_indexed_special_folder_entry_ids(principal, tag, value)
                .map(|value| (canonical_property_storage_tag(tag), value))
        }
        _ => None,
    }
}

fn merge_indexed_special_folder_entry_ids(
    principal: &AccountPrincipal,
    tag: u32,
    value: MapiValue,
) -> Option<MapiValue> {
    let MapiValue::MultiBinary(client_values) = value else {
        return None;
    };
    let Some(MapiValue::MultiBinary(mut canonical_values)) =
        special_folder_identification_property_value(principal.account_id, tag)
    else {
        return None;
    };
    let canonical_len = canonical_values.len();
    if client_values.len() > canonical_len {
        canonical_values.extend(client_values.into_iter().skip(canonical_len));
    }
    Some(MapiValue::MultiBinary(canonical_values))
}

fn default_folder_identification_values_stripped_by_safe_values(
    object: Option<&MapiObject>,
    property_tags: &[u32],
) -> bool {
    property_tags
        .iter()
        .any(|tag| strips_default_folder_identification_value(object, *tag))
}

fn strips_default_folder_identification_value(object: Option<&MapiObject>, tag: u32) -> bool {
    if !is_default_folder_identification_property_tag(tag) {
        return false;
    }
    match object {
        Some(MapiObject::Folder {
            folder_id: ROOT_FOLDER_ID,
            ..
        }) => {
            matches!(
                canonical_property_storage_tag(tag),
                PID_TAG_ADDITIONAL_REN_ENTRY_IDS
                    | PID_TAG_ADDITIONAL_REN_ENTRY_IDS_EX
                    | PID_TAG_FREE_BUSY_ENTRY_IDS
            )
        }
        Some(MapiObject::Folder {
            folder_id: INBOX_FOLDER_ID,
            ..
        }) => {
            matches!(
                canonical_property_storage_tag(tag),
                PID_TAG_ADDITIONAL_REN_ENTRY_IDS
                    | PID_TAG_ADDITIONAL_REN_ENTRY_IDS_EX
                    | PID_TAG_FREE_BUSY_ENTRY_IDS
            )
        }
        _ => false,
    }
}

fn strips_any_default_folder_identification_values(object: Option<&MapiObject>) -> bool {
    matches!(
        object,
        Some(MapiObject::Folder {
            folder_id: ROOT_FOLDER_ID | INBOX_FOLDER_ID,
            ..
        })
    )
}

fn default_folder_entry_id_property_name(tag: u32) -> &'static str {
    match canonical_property_storage_tag(tag) {
        PID_TAG_IPM_SUBTREE_ENTRY_ID => "PidTagIpmSubtreeEntryId",
        PID_TAG_IPM_OUTBOX_ENTRY_ID => "PidTagIpmOutboxEntryId",
        PID_TAG_IPM_WASTEBASKET_ENTRY_ID => "PidTagIpmWastebasketEntryId",
        PID_TAG_IPM_SENTMAIL_ENTRY_ID => "PidTagIpmSentMailEntryId",
        PID_TAG_VIEWS_ENTRY_ID => "PidTagViewsEntryId",
        PID_TAG_COMMON_VIEWS_ENTRY_ID => "PidTagCommonViewsEntryId",
        PID_TAG_FINDER_ENTRY_ID => "PidTagFinderEntryId",
        PID_TAG_IPM_ARCHIVE_ENTRY_ID => "PidTagIpmArchiveEntryId",
        PID_TAG_IPM_APPOINTMENT_ENTRY_ID => "PidTagIpmAppointmentEntryId",
        PID_TAG_IPM_CONTACT_ENTRY_ID => "PidTagIpmContactEntryId",
        PID_TAG_IPM_JOURNAL_ENTRY_ID => "PidTagIpmJournalEntryId",
        PID_TAG_IPM_NOTE_ENTRY_ID => "PidTagIpmNoteEntryId",
        PID_TAG_IPM_TASK_ENTRY_ID => "PidTagIpmTaskEntryId",
        PID_TAG_REM_ONLINE_ENTRY_ID => "PidTagRemOnlineEntryId",
        PID_TAG_IPM_DRAFTS_ENTRY_ID => "PidTagIpmDraftsEntryId",
        _ => "unknown",
    }
}

fn summarize_open_folder_probe_response(
    responses: &[u8],
    offset: usize,
    request: &OpenFolderProbeRequest,
) -> String {
    let result = read_response_error_code(responses, offset)
        .map(|code| format!("{code:#010x}"))
        .unwrap_or_else(|| "truncated".to_string());
    let has_rules = responses
        .get(offset + 6)
        .map(|value| value.to_string())
        .unwrap_or_else(|| "truncated".to_string());
    let is_ghosted = responses
        .get(offset + 7)
        .map(|value| value.to_string())
        .unwrap_or_else(|| "truncated".to_string());
    format!(
        "out={};folder=0x{:016x};name={};result={result};has_rules={has_rules};is_ghosted={is_ghosted}",
        request.output_handle_index,
        request.folder_id,
        post_hierarchy_probe_folder_name(request.folder_id)
    )
}

fn summarize_get_properties_probe_response(
    responses: &[u8],
    offset: usize,
    request: &GetPropertiesSpecificProbeRequest,
) -> String {
    let result = read_response_error_code(responses, offset)
        .map(|code| format!("{code:#010x}"))
        .unwrap_or_else(|| "truncated".to_string());
    let row_shape = match responses.get(offset + 6).copied() {
        Some(0) => "standard",
        Some(1) => "flagged",
        Some(_) => "unknown",
        None => "truncated",
    };
    let values = summarize_get_properties_probe_response_values(responses, offset, request);
    format!(
        "in={};result={result};row={row_shape};tags={};values={values}",
        request.input_handle_index,
        format_debug_property_tags(&request.property_tags)
    )
}

fn summarize_get_properties_probe_response_values(
    responses: &[u8],
    offset: usize,
    request: &GetPropertiesSpecificProbeRequest,
) -> String {
    if responses.get(offset + 6).copied() != Some(0) {
        return "not-standard-row".to_string();
    }
    let mut cursor = Cursor::new(responses.get(offset + 7..).unwrap_or_default());
    let mut values = Vec::new();
    for tag in &request.property_tags {
        match parse_property_value_for_tag(&mut cursor, *tag) {
            Ok(value) => values.push(format!("{tag:#010x}:{}", mapi_value_debug_shape(&value))),
            Err(error) => {
                values.push(format!("{tag:#010x}:parse_error={error}"));
                break;
            }
        }
    }
    values.join(",")
}

fn summarize_set_properties_probe_response(
    responses: &[u8],
    offset: usize,
    request: &SetPropertiesProbeRequest,
) -> String {
    let result = read_response_error_code(responses, offset)
        .map(|code| format!("{code:#010x}"))
        .unwrap_or_else(|| "truncated".to_string());
    let property_problem_count = responses
        .get(offset + 6..offset + 8)
        .and_then(|bytes| bytes.try_into().ok())
        .map(u16::from_le_bytes)
        .map(|count| count.to_string())
        .unwrap_or_else(|| "truncated".to_string());
    format!(
        "in={};result={result};property_problem_count={property_problem_count};tags={}",
        request.input_handle_index,
        format_debug_property_tags(&request.property_tags)
    )
}

fn mapi_value_debug_shape(value: &MapiValue) -> String {
    match value {
        MapiValue::Bool(_) => "bool".to_string(),
        MapiValue::I16(_) => "i16".to_string(),
        MapiValue::I32(_) => "i32".to_string(),
        MapiValue::I64(_) => "i64".to_string(),
        MapiValue::U32(_) => "u32".to_string(),
        MapiValue::U64(_) => "u64".to_string(),
        MapiValue::String(value) => format!("string:chars={}", value.chars().count()),
        MapiValue::Binary(value) => format!("binary:bytes={}", value.len()),
        MapiValue::Guid(_) => "guid".to_string(),
        MapiValue::Error(error) => format!("error:{error:#010x}"),
        MapiValue::MultiI16(value) => format!("multi_i16:count={}", value.len()),
        MapiValue::MultiI32(value) => format!("multi_i32:count={}", value.len()),
        MapiValue::MultiI64(value) => format!("multi_i64:count={}", value.len()),
        MapiValue::MultiString(value) => format!("multi_string:count={}", value.len()),
        MapiValue::MultiBinary(value) => format!("multi_binary:count={}", value.len()),
        MapiValue::MultiGuid(value) => format!("multi_guid:count={}", value.len()),
    }
}

async fn apply_supported_object_property_values<S>(
    store: &S,
    principal: &AccountPrincipal,
    object: &MapiObject,
    values: Vec<(u32, MapiValue)>,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
) -> Result<()>
where
    S: ExchangeStore,
{
    let (canonical_values, custom_values) = split_custom_property_values(values);
    if !canonical_values.is_empty() {
        match object {
            MapiObject::Message {
                folder_id,
                message_id,
            } => {
                apply_canonical_message_property_values(
                    store,
                    principal,
                    *folder_id,
                    *message_id,
                    canonical_values,
                    mailboxes,
                    emails,
                )
                .await?;
            }
            MapiObject::Contact {
                folder_id,
                contact_id,
            } => {
                apply_canonical_contact_property_values(
                    store,
                    principal,
                    *folder_id,
                    *contact_id,
                    canonical_values,
                    snapshot,
                )
                .await?;
            }
            MapiObject::Event {
                folder_id,
                event_id,
            } => {
                apply_canonical_event_property_values(
                    store,
                    principal,
                    *folder_id,
                    *event_id,
                    canonical_values,
                    snapshot,
                )
                .await?;
            }
            MapiObject::Task { folder_id, task_id } => {
                apply_canonical_task_property_values(
                    store,
                    principal,
                    *folder_id,
                    *task_id,
                    canonical_values,
                    snapshot,
                )
                .await?;
            }
            MapiObject::Note { folder_id, note_id } => {
                apply_canonical_note_property_values(
                    store,
                    principal,
                    *folder_id,
                    *note_id,
                    canonical_values,
                    snapshot,
                )
                .await?;
            }
            MapiObject::JournalEntry {
                folder_id,
                journal_entry_id,
            } => {
                apply_canonical_journal_entry_property_values(
                    store,
                    principal,
                    *folder_id,
                    *journal_entry_id,
                    canonical_values,
                    snapshot,
                )
                .await?;
            }
            MapiObject::ConversationAction {
                folder_id: _,
                conversation_action_id,
            } => {
                let Some(existing) =
                    snapshot.conversation_action_message_for_id(*conversation_action_id)
                else {
                    return Err(anyhow!("canonical MAPI conversation action was not found"));
                };
                let mut properties = conversation_action_properties(&existing.action);
                apply_mapi_property_values_to_map(&mut properties, canonical_values);
                let action = conversation_action_from_mapi_properties(&properties);
                let move_target_mailbox_id =
                    conversation_action_target_mailbox_id(&action, mailboxes);
                let input = lpe_storage::UpsertConversationActionInput {
                    account_id: principal.account_id,
                    conversation_id: action.conversation_id,
                    subject: action.subject,
                    categories_json: action.categories_json,
                    move_folder_entry_id: action.move_folder_entry_id,
                    move_store_entry_id: action.move_store_entry_id,
                    move_target_mailbox_id,
                    max_delivery_time: action.max_delivery_time,
                    last_applied_time: action.last_applied_time,
                    version: Some(action.version),
                    processed: Some(action.processed),
                };
                let saved = store.upsert_conversation_action(input).await?;
                apply_conversation_action_to_existing_messages(
                    store, principal, &saved, mailboxes, emails,
                )
                .await?;
            }
            MapiObject::NavigationShortcut {
                folder_id: _,
                shortcut_id,
            } => {
                let Some(existing) = snapshot.navigation_shortcut_message_for_id(*shortcut_id)
                else {
                    return Err(anyhow!("canonical MAPI navigation shortcut was not found"));
                };
                let mut properties = HashMap::new();
                for tag in [
                    PID_TAG_SUBJECT_W,
                    PID_TAG_NORMALIZED_SUBJECT_W,
                    PID_TAG_WLINK_ENTRY_ID,
                    PID_TAG_WLINK_TYPE,
                    PID_TAG_WLINK_FLAGS,
                    PID_TAG_WLINK_SECTION,
                    PID_TAG_WLINK_ORDINAL,
                ] {
                    if let Some(value) =
                        navigation_shortcut_property_value(&existing, principal.account_id, tag)
                    {
                        properties.insert(tag, value);
                    }
                }
                apply_mapi_property_values_to_map(&mut properties, canonical_values);
                let shortcut = navigation_shortcut_from_mapi_properties(
                    principal.account_id,
                    Some(existing.canonical_id),
                    &properties,
                );
                store
                    .upsert_mapi_navigation_shortcut(UpsertMapiNavigationShortcutInput {
                        id: Some(shortcut.canonical_id),
                        account_id: principal.account_id,
                        subject: shortcut.subject,
                        target_folder_id: shortcut.target_folder_id,
                        shortcut_type: shortcut.shortcut_type,
                        flags: shortcut.flags,
                        section: shortcut.section,
                        ordinal: shortcut.ordinal,
                    })
                    .await?;
            }
            MapiObject::DelegateFreeBusyMessage { .. } => {}
            _ => return Err(anyhow!("MAPI object does not support property mutation")),
        }
    }
    if custom_values.is_empty() {
        return Ok(());
    }
    let (object_kind, canonical_id) =
        custom_property_object_identity(Some(object), mailboxes, emails, snapshot)
            .ok_or_else(|| anyhow!("canonical MAPI object was not found"))?;
    upsert_custom_property_values(store, principal, object_kind, canonical_id, custom_values).await
}

async fn persist_profile_folder_property_values<S>(
    store: &S,
    principal: &AccountPrincipal,
    folder_id: u64,
    values: &[(u32, MapiValue)],
) -> Result<()>
where
    S: ExchangeStore,
{
    if folder_id != IPM_SUBTREE_FOLDER_ID {
        return Ok(());
    }
    for (tag, value) in values {
        if canonical_property_storage_tag(*tag) == PID_TAG_OST_OSTID {
            if let MapiValue::Binary(ost_id) = value {
                store
                    .store_mapi_ipm_subtree_ost_id(principal.account_id, ost_id)
                    .await?;
            }
        }
    }
    Ok(())
}

fn split_custom_property_values(
    values: Vec<(u32, MapiValue)>,
) -> (Vec<(u32, MapiValue)>, Vec<(u32, MapiValue)>) {
    values
        .into_iter()
        .partition(|(tag, _)| !is_custom_property_tag(*tag))
}

fn apply_mapi_property_values_to_map(
    properties: &mut HashMap<u32, MapiValue>,
    values: Vec<(u32, MapiValue)>,
) {
    properties.extend(
        values
            .into_iter()
            .map(|(tag, value)| (canonical_property_storage_tag(tag), value)),
    );
}

fn conversation_action_properties(
    action: &lpe_storage::ConversationAction,
) -> HashMap<u32, MapiValue> {
    let mut properties = HashMap::new();
    properties.insert(
        PID_TAG_CONVERSATION_INDEX,
        MapiValue::Binary(conversation_index_for_uuid(action.conversation_id)),
    );
    properties.insert(
        PID_TAG_SUBJECT_W,
        MapiValue::String(conversation_action_subject(action)),
    );
    if let Some(value) = &action.move_folder_entry_id {
        properties.insert(
            PID_LID_CONVERSATION_ACTION_MOVE_FOLDER_EID_TAG,
            MapiValue::Binary(value.clone()),
        );
    }
    if let Some(value) = &action.move_store_entry_id {
        properties.insert(
            PID_LID_CONVERSATION_ACTION_MOVE_STORE_EID_TAG,
            MapiValue::Binary(value.clone()),
        );
    }
    if let Some(value) = &action.max_delivery_time {
        properties.insert(
            PID_LID_CONVERSATION_ACTION_MAX_DELIVERY_TIME_TAG,
            MapiValue::U64(mapi_mailstore::filetime_from_rfc3339_utc(value)),
        );
    }
    if let Some(value) = &action.last_applied_time {
        properties.insert(
            PID_LID_CONVERSATION_ACTION_LAST_APPLIED_TIME_TAG,
            MapiValue::U64(mapi_mailstore::filetime_from_rfc3339_utc(value)),
        );
    }
    properties.insert(
        PID_LID_CONVERSATION_ACTION_VERSION_TAG,
        MapiValue::I32(action.version),
    );
    properties.insert(
        PID_LID_CONVERSATION_PROCESSED_TAG,
        MapiValue::I32(action.processed),
    );
    properties.insert(
        PID_NAME_KEYWORDS_TAG,
        MapiValue::MultiString(
            serde_json::from_str::<Vec<String>>(&action.categories_json).unwrap_or_default(),
        ),
    );
    properties
}

async fn apply_conversation_action_to_existing_messages<S>(
    store: &S,
    principal: &AccountPrincipal,
    action: &lpe_storage::ConversationAction,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
) -> Result<()>
where
    S: ExchangeStore,
{
    let categories = serde_json::from_str::<Vec<String>>(&action.categories_json)
        .unwrap_or_default()
        .into_iter()
        .map(|category| category.trim().to_string())
        .filter(|category| !category.is_empty())
        .collect::<Vec<_>>();
    let target_mailbox = if action.move_store_entry_id.is_some() {
        None
    } else {
        conversation_action_target_mailbox(action, mailboxes)
    };
    for email in emails
        .iter()
        .filter(|email| email.thread_id == action.conversation_id)
        .filter(|email| email.mailbox_role != "sent")
        .filter(|email| {
            action
                .max_delivery_time
                .as_deref()
                .map(|max_delivery| email.received_at.as_str() > max_delivery)
                .unwrap_or(true)
        })
    {
        if !categories.is_empty() && email.categories != categories {
            store
                .update_jmap_email_followup_flags(
                    principal.account_id,
                    email.id,
                    lpe_storage::JmapEmailFollowupUpdate {
                        categories: Some(categories.clone()),
                        ..Default::default()
                    },
                    AuditEntryInput {
                        actor: principal.email.clone(),
                        action: "mapi-conversation-action-categorize".to_string(),
                        subject: format!("message:{}", email.id),
                    },
                )
                .await?;
        }
        let Some(target_mailbox) = target_mailbox else {
            continue;
        };
        if email.mailbox_id == target_mailbox.id {
            continue;
        }
        store
            .move_jmap_email_from_mailbox(
                principal.account_id,
                email.mailbox_id,
                email.id,
                target_mailbox.id,
                AuditEntryInput {
                    actor: principal.email.clone(),
                    action: "mapi-conversation-action-move".to_string(),
                    subject: format!("message:{}->{}", email.id, target_mailbox.id),
                },
            )
            .await?;
    }
    Ok(())
}

async fn apply_conversation_actions_to_new_message<S>(
    store: &S,
    principal: &AccountPrincipal,
    mailboxes: &[JmapMailbox],
    email: &JmapEmail,
    snapshot: &MapiMailStoreSnapshot,
) -> Result<()>
where
    S: ExchangeStore,
{
    for message in snapshot
        .conversation_action_messages()
        .iter()
        .filter(|message| message.action.conversation_id == email.thread_id)
    {
        apply_conversation_action_to_existing_messages(
            store,
            principal,
            &message.action,
            mailboxes,
            std::slice::from_ref(email),
        )
        .await?;
    }
    Ok(())
}

fn conversation_action_target_mailbox<'a>(
    action: &lpe_storage::ConversationAction,
    mailboxes: &'a [JmapMailbox],
) -> Option<&'a JmapMailbox> {
    if action.move_store_entry_id.is_some() {
        return None;
    }
    if let Some(mailbox_id) = action.move_target_mailbox_id {
        return mailboxes.iter().find(|mailbox| mailbox.id == mailbox_id);
    }
    match action.move_folder_entry_id.as_deref() {
        Some([]) => mailboxes.iter().find(|mailbox| mailbox.role == "trash"),
        Some(entry_id) => {
            let folder_id = crate::mapi::identity::object_id_from_folder_entry_id(entry_id)?;
            folder_row_for_id(folder_id, mailboxes)
        }
        None => None,
    }
}

fn conversation_action_target_mailbox_id(
    action: &lpe_storage::ConversationAction,
    mailboxes: &[JmapMailbox],
) -> Option<Uuid> {
    conversation_action_target_mailbox(action, mailboxes).map(|mailbox| mailbox.id)
}

async fn delete_conversation_action_properties<S>(
    store: &S,
    principal: &AccountPrincipal,
    conversation_action_id: u64,
    snapshot: &MapiMailStoreSnapshot,
    property_tags: &[u32],
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
) -> Result<()>
where
    S: ExchangeStore,
{
    let existing = snapshot
        .conversation_action_message_for_id(conversation_action_id)
        .ok_or_else(|| anyhow!("canonical MAPI conversation action was not found"))?;
    let mut properties = conversation_action_properties(&existing.action);
    for tag in property_tags {
        properties.remove(tag);
        properties.remove(&canonical_property_storage_tag(*tag));
    }
    let action = conversation_action_from_mapi_properties(&properties);
    let move_target_mailbox_id = conversation_action_target_mailbox_id(&action, mailboxes);
    let saved = store
        .upsert_conversation_action(lpe_storage::UpsertConversationActionInput {
            account_id: principal.account_id,
            conversation_id: action.conversation_id,
            subject: action.subject,
            categories_json: action.categories_json,
            move_folder_entry_id: action.move_folder_entry_id,
            move_store_entry_id: action.move_store_entry_id,
            move_target_mailbox_id,
            max_delivery_time: action.max_delivery_time,
            last_applied_time: action.last_applied_time,
            version: Some(action.version),
            processed: Some(action.processed),
        })
        .await?;
    apply_conversation_action_to_existing_messages(store, principal, &saved, mailboxes, emails)
        .await
}

async fn upsert_custom_property_values<S>(
    store: &S,
    principal: &AccountPrincipal,
    object_kind: MapiCustomPropertyObjectKind,
    canonical_id: Uuid,
    values: Vec<(u32, MapiValue)>,
) -> Result<()>
where
    S: ExchangeStore,
{
    if values.is_empty() {
        return Ok(());
    }
    let values = values
        .into_iter()
        .map(|(property_tag, value)| {
            let mut property_value = Vec::new();
            write_mapi_value(&mut property_value, property_tag, &value);
            MapiCustomPropertyValue {
                property_tag,
                property_type: MapiPropertyTag::new(property_tag).property_type_code(),
                property_value,
            }
        })
        .collect::<Vec<_>>();
    store
        .upsert_mapi_custom_property_values(
            principal.account_id,
            object_kind,
            canonical_id,
            &values,
        )
        .await
}

async fn fetch_custom_property_values_for_request<S>(
    store: &S,
    principal: &AccountPrincipal,
    object: Option<&MapiObject>,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    property_tags: &[u32],
) -> Result<HashMap<u32, Vec<u8>>>
where
    S: ExchangeStore,
{
    let tags = property_tags
        .iter()
        .copied()
        .filter(|tag| is_custom_property_tag(*tag))
        .collect::<Vec<_>>();
    if tags.is_empty() {
        return Ok(HashMap::new());
    }
    let Some((object_kind, canonical_id)) =
        custom_property_object_identity(object, mailboxes, emails, snapshot)
    else {
        return Ok(HashMap::new());
    };
    Ok(store
        .fetch_mapi_custom_property_values(principal.account_id, object_kind, canonical_id, &tags)
        .await?
        .into_iter()
        .map(|value| (value.property_tag, value.property_value))
        .collect())
}

async fn delete_custom_property_values<S>(
    store: &S,
    principal: &AccountPrincipal,
    object: Option<&MapiObject>,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    property_tags: &[u32],
) -> Result<()>
where
    S: ExchangeStore,
{
    let tags = property_tags
        .iter()
        .copied()
        .filter(|tag| is_custom_property_tag(*tag))
        .collect::<Vec<_>>();
    if tags.is_empty() {
        return Ok(());
    }
    let Some((object_kind, canonical_id)) =
        custom_property_object_identity(object, mailboxes, emails, snapshot)
    else {
        return Ok(());
    };
    store
        .delete_mapi_custom_property_values(principal.account_id, object_kind, canonical_id, &tags)
        .await
}

fn custom_property_object_identity(
    object: Option<&MapiObject>,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
) -> Option<(MapiCustomPropertyObjectKind, Uuid)> {
    match object? {
        MapiObject::Message {
            folder_id,
            message_id,
        } => message_for_id(*folder_id, *message_id, mailboxes, emails)
            .map(|email| (MapiCustomPropertyObjectKind::Message, email.id)),
        MapiObject::Contact {
            folder_id,
            contact_id,
        } => snapshot
            .contact_for_id(*folder_id, *contact_id)
            .map(|contact| (MapiCustomPropertyObjectKind::Contact, contact.canonical_id)),
        MapiObject::Event {
            folder_id,
            event_id,
        } => snapshot.event_for_id(*folder_id, *event_id).map(|event| {
            (
                MapiCustomPropertyObjectKind::CalendarEvent,
                event.canonical_id,
            )
        }),
        MapiObject::Task { folder_id, task_id } => snapshot
            .task_for_id(*folder_id, *task_id)
            .map(|task| (MapiCustomPropertyObjectKind::Task, task.canonical_id)),
        MapiObject::Note { folder_id, note_id } => snapshot
            .note_for_id(*folder_id, *note_id)
            .map(|note| (MapiCustomPropertyObjectKind::Note, note.canonical_id)),
        MapiObject::JournalEntry {
            folder_id,
            journal_entry_id,
        } => snapshot
            .journal_entry_for_id(*folder_id, *journal_entry_id)
            .map(|entry| {
                (
                    MapiCustomPropertyObjectKind::JournalEntry,
                    entry.canonical_id,
                )
            }),
        MapiObject::Attachment {
            folder_id,
            message_id,
            attach_num,
        } => snapshot
            .attachment_for_message(*folder_id, *message_id, *attach_num)
            .map(|attachment| {
                (
                    MapiCustomPropertyObjectKind::Attachment,
                    attachment.canonical_id,
                )
            }),
        _ => None,
    }
}

fn is_custom_property_tag(property_tag: u32) -> bool {
    let tag = MapiPropertyTag::new(property_tag);
    tag.property_id() >= FIRST_NAMED_PROPERTY_ID
        && tag.property_type().is_some()
        && !is_canonical_named_property_tag(property_tag)
}

fn is_canonical_named_property_tag(property_tag: u32) -> bool {
    matches!(
        canonical_property_storage_tag(property_tag),
        PID_LID_FLAG_REQUEST_W_TAG
            | PID_LID_TASK_START_DATE_TAG
            | PID_LID_TASK_DUE_DATE_TAG
            | PID_LID_GLOBAL_OBJECT_ID_TAG
            | PID_LID_CLEAN_GLOBAL_OBJECT_ID_TAG
            | PID_LID_BUSY_STATUS_TAG
            | PID_LID_LOCATION_W_TAG
            | PID_LID_APPOINTMENT_START_WHOLE_TAG
            | PID_LID_APPOINTMENT_END_WHOLE_TAG
            | PID_LID_APPOINTMENT_DURATION_TAG
            | PID_LID_APPOINTMENT_SUB_TYPE_TAG
            | PID_LID_APPOINTMENT_STATE_FLAGS_TAG
            | PID_LID_REMINDER_SET_TAG
            | PID_LID_REMINDER_TIME_TAG
            | PID_LID_REMINDER_SIGNAL_TIME_TAG
            | PID_LID_NOTE_COLOR_TAG
            | PID_LID_LOG_TYPE_W_TAG
            | PID_LID_COMPANIES_TAG
            | PID_LID_CONTACTS_TAG
            | PID_LID_CONVERSATION_ACTION_MOVE_FOLDER_EID_TAG
            | PID_LID_CONVERSATION_ACTION_MOVE_STORE_EID_TAG
            | PID_LID_CONVERSATION_ACTION_MAX_DELIVERY_TIME_TAG
            | PID_LID_CONVERSATION_ACTION_LAST_APPLIED_TIME_TAG
            | PID_LID_CONVERSATION_ACTION_VERSION_TAG
            | PID_LID_CONVERSATION_PROCESSED_TAG
            | PID_NAME_KEYWORDS_TAG
    )
}

pub(in crate::mapi) fn post_hierarchy_probe_folder_name(folder_id: u64) -> &'static str {
    match folder_id {
        ROOT_FOLDER_ID => "root",
        IPM_SUBTREE_FOLDER_ID => "ipm_subtree",
        DEFERRED_ACTION_FOLDER_ID => "deferred_action",
        SPOOLER_QUEUE_FOLDER_ID => "spooler_queue",
        INBOX_FOLDER_ID => "inbox",
        DRAFTS_FOLDER_ID => "drafts",
        SENT_FOLDER_ID => "sent",
        TRASH_FOLDER_ID => "trash",
        OUTBOX_FOLDER_ID => "outbox",
        COMMON_VIEWS_FOLDER_ID => "common_views",
        SCHEDULE_FOLDER_ID => "schedule",
        SEARCH_FOLDER_ID => "search",
        VIEWS_FOLDER_ID => "personal_views",
        CALENDAR_FOLDER_ID => "calendar",
        CONTACTS_FOLDER_ID => "contacts",
        JOURNAL_FOLDER_ID => "journal",
        NOTES_FOLDER_ID => "notes",
        TASKS_FOLDER_ID => "tasks",
        REMINDERS_FOLDER_ID => "reminders",
        SUGGESTED_CONTACTS_FOLDER_ID => "suggested_contacts",
        QUICK_CONTACTS_FOLDER_ID => "quick_contacts",
        IM_CONTACT_LIST_FOLDER_ID => "im_contact_list",
        CONTACTS_SEARCH_FOLDER_ID => "contacts_search",
        DOCUMENT_LIBRARIES_FOLDER_ID => "document_libraries",
        SYNC_ISSUES_FOLDER_ID => "sync_issues",
        CONFLICTS_FOLDER_ID => "conflicts",
        LOCAL_FAILURES_FOLDER_ID => "local_failures",
        SERVER_FAILURES_FOLDER_ID => "server_failures",
        JUNK_FOLDER_ID => "junk",
        RSS_FEEDS_FOLDER_ID => "rss_feeds",
        TRACKED_MAIL_PROCESSING_FOLDER_ID => "tracked_mail_processing",
        TODO_SEARCH_FOLDER_ID => "todo_search",
        CONVERSATION_ACTION_SETTINGS_FOLDER_ID => "conversation_action_settings",
        ARCHIVE_FOLDER_ID => "archive",
        FREEBUSY_DATA_FOLDER_ID => "freebusy_data",
        CONVERSATION_HISTORY_FOLDER_ID => "conversation_history",
        _ => "other",
    }
}

pub(in crate::mapi) fn debug_role_for_folder_id(folder_id: u64) -> &'static str {
    role_for_folder_id(folder_id).unwrap_or_else(|| post_hierarchy_probe_folder_name(folder_id))
}

pub(in crate::mapi) fn debug_container_class_for_folder_id(folder_id: u64) -> &'static str {
    match folder_id {
        COMMON_VIEWS_FOLDER_ID
        | SCHEDULE_FOLDER_ID
        | SEARCH_FOLDER_ID
        | VIEWS_FOLDER_ID
        | FREEBUSY_DATA_FOLDER_ID => "",
        CONTACTS_SEARCH_FOLDER_ID => "IPF.Contact",
        TODO_SEARCH_FOLDER_ID => "IPF.Task",
        REMINDERS_FOLDER_ID => "Outlook.Reminder",
        CONVERSATION_ACTION_SETTINGS_FOLDER_ID => "IPF.Configuration",
        _ => expected_special_folder_container_class(folder_id),
    }
}

fn format_debug_property_tags(tags: &[u32]) -> String {
    tags.iter()
        .map(|tag| format!("{tag:#010x}"))
        .collect::<Vec<_>>()
        .join(",")
}

fn upload_state_property_name(tag: u32) -> &'static str {
    match tag {
        0x4017_0003 | 0x4017_0102 => "MetaTagIdsetGiven",
        0x4018_0102 => "MetaTagIdsetDeleted",
        0x402D_0102 => "MetaTagIdsetRead",
        0x402E_0102 => "MetaTagIdsetUnread",
        0x6796_0102 => "MetaTagCnsetSeen",
        0x67DA_0102 => "MetaTagCnsetSeenFAI",
        0x67D2_0102 => "MetaTagCnsetRead",
        _ => "unknown",
    }
}

fn upload_state_marker_bit(tag: u32) -> u8 {
    match tag {
        0x4017_0003 | 0x4017_0102 => 0x01,
        0x6796_0102 => 0x02,
        0x67DA_0102 => 0x04,
        0x67D2_0102 => 0x08,
        _ => 0,
    }
}

fn uploaded_state_has_delta_anchor(marker_mask: u8) -> bool {
    marker_mask & 0x03 == 0x03
}

fn mark_uploaded_state_stream(marker_mask: &mut u8, property_tag: u32) {
    *marker_mask |= upload_state_marker_bit(property_tag);
}

fn uploaded_state_marker_summary(marker_mask: u8) -> String {
    let mut markers = Vec::new();
    if marker_mask & 0x01 != 0 {
        markers.push("MetaTagIdsetGiven");
    }
    if marker_mask & 0x02 != 0 {
        markers.push("MetaTagCnsetSeen");
    }
    if marker_mask & 0x04 != 0 {
        markers.push("MetaTagCnsetSeenFAI");
    }
    if marker_mask & 0x08 != 0 {
        markers.push("MetaTagCnsetRead");
    }
    markers.join(",")
}

fn sync_checkpoint_scope(
    folder_id: u64,
    checkpoint_mailbox_id: Option<Uuid>,
    special_objects: &[mapi_mailstore::SpecialMessageSyncFact],
) -> &'static str {
    let virtual_scope_id =
        mapi_mailstore::virtual_special_mailbox(folder_id).map(|mailbox| mailbox.id);
    if checkpoint_mailbox_id.is_some() && checkpoint_mailbox_id == virtual_scope_id {
        return "virtual_special_folder";
    }
    if checkpoint_mailbox_id.is_some() {
        "canonical_mailbox"
    } else if !special_objects.is_empty() {
        "virtual_special_folder"
    } else {
        "virtual_or_system_folder"
    }
}

fn debug_object_scope_for_id(
    object_id: Option<u64>,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
) -> &'static str {
    let Some(object_id) = object_id else {
        return "unparsed";
    };
    if is_advertised_special_folder(object_id) {
        return "advertised_special_folder";
    }
    if mailboxes
        .iter()
        .any(|mailbox| mapi_item_id_matches(&mailbox.id, object_id))
    {
        return "mailbox";
    }
    if emails
        .iter()
        .any(|email| mapi_item_id_matches(&email.id, object_id))
    {
        return "message";
    }
    if snapshot
        .event_for_id(CALENDAR_FOLDER_ID, object_id)
        .is_some()
        || snapshot
            .event_for_id(REMINDERS_FOLDER_ID, object_id)
            .is_some()
    {
        return "calendar_event";
    }
    if snapshot
        .contact_for_id(CONTACTS_FOLDER_ID, object_id)
        .is_some()
        || snapshot
            .contact_for_id(CONTACTS_SEARCH_FOLDER_ID, object_id)
            .is_some()
    {
        return "contact";
    }
    if snapshot.task_for_id(TASKS_FOLDER_ID, object_id).is_some()
        || snapshot
            .task_for_id(TODO_SEARCH_FOLDER_ID, object_id)
            .is_some()
        || snapshot
            .task_for_id(REMINDERS_FOLDER_ID, object_id)
            .is_some()
    {
        return "task";
    }
    if snapshot.note_for_id(NOTES_FOLDER_ID, object_id).is_some() {
        return "note";
    }
    if snapshot
        .journal_entry_for_id(JOURNAL_FOLDER_ID, object_id)
        .is_some()
    {
        return "journal_entry";
    }
    if snapshot
        .conversation_action_message_for_id(object_id)
        .is_some()
    {
        return "conversation_action";
    }
    "not_loaded"
}

fn long_term_id_from_id_scope_is_loaded(scope: &str) -> bool {
    scope != "unparsed" && scope != "not_loaded"
}

fn long_term_id_from_id_object_is_loaded(object_id: Option<u64>, scope: &str) -> bool {
    if long_term_id_from_id_scope_is_loaded(scope) {
        return true;
    }
    object_id
        .and_then(crate::mapi::identity::global_counter_from_store_id)
        .is_some_and(|counter| counter >= crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER)
}

fn rop_long_term_id_from_id_response_for_scope(
    request: &RopRequest,
    object_id: Option<u64>,
    scope: &str,
) -> Vec<u8> {
    if long_term_id_from_id_object_is_loaded(object_id, scope) {
        rop_long_term_id_from_id_response(request)
    } else {
        rop_error_response(
            RopId::LongTermIdFromId as u8,
            request.response_handle_index(),
            0x8004_010F,
        )
    }
}

fn summarize_request_rop_buffer(rop_buffer: &[u8]) -> RopRequestDebugSummary {
    let mut summary = RopRequestDebugSummary {
        extended: is_rpc_header_ext_rop_buffer(rop_buffer),
        ..RopRequestDebugSummary::default()
    };
    let Some((requests, handle_table)) = split_rop_buffer(rop_buffer) else {
        summary.parse_error = "invalid ROP buffer".to_string();
        return summary;
    };
    let handle_summary = summarize_handle_table(handle_table, &mut summary.parse_error);
    summary.handle_count = handle_summary.0;
    summary.handle_table_summary = handle_summary.1;
    summary.request_payload_bytes = requests.len();
    summary.handle_table_bytes = handle_table.len();

    let mut cursor = Cursor::new(requests);
    while cursor.remaining() > 0 && summary.ids.len() < MAX_ROP_DEBUG_ENTRIES {
        match read_rop_request(&mut cursor) {
            Ok(request) => summary.ids.push(request.typed().rop_id()),
            Err(error) => {
                let offset = cursor.position();
                let remaining = cursor.remaining();
                let preview = requests
                    .get(offset..)
                    .map(|bytes| hex_preview(bytes, 16))
                    .unwrap_or_default();
                summary.parse_error = format!(
                    "{};offset={offset};remaining={remaining};next={preview};parsed_rop_count={}",
                    error,
                    summary.ids.len()
                );
                break;
            }
        }
    }
    summary.ids_csv = rop_ids_csv(&summary.ids);
    let raw = summarize_request_rop_raw_frames(requests);
    summary.raw_frame_count = raw.0;
    summary.raw_frames = raw.1;
    summary
}

fn summarize_request_rop_raw_frames(requests: &[u8]) -> (usize, String) {
    let mut cursor = Cursor::new(requests);
    let mut frames = Vec::new();
    while cursor.remaining() > 0 && frames.len() < MAX_ROP_DEBUG_ENTRIES {
        let start = cursor.position();
        let rop_id = requests.get(start).copied().unwrap_or_default();
        let logon_id = requests.get(start + 1).copied().unwrap_or_default();
        match read_rop_request(&mut cursor) {
            Ok(request) => {
                let end = cursor.position();
                frames.push(format!(
                    "0x{rop_id:02x}@{start}..{end}:len={}:logon={logon_id}:in={}:out={}:payload={}:preview={}",
                    end.saturating_sub(start),
                    request
                        .input_handle_index
                        .map(|index| index.to_string())
                        .unwrap_or_else(|| "-".to_string()),
                    request
                        .output_handle_index
                        .map(|index| index.to_string())
                        .unwrap_or_else(|| "-".to_string()),
                    request.payload.len(),
                    hex_preview(&requests[start..end], 16)
                ));
            }
            Err(error) => {
                let offset = cursor.position();
                frames.push(format!(
                    "0x{rop_id:02x}@{start}..{offset}:error={error}:remaining={}:next={}",
                    cursor.remaining(),
                    requests
                        .get(offset..)
                        .map(|bytes| hex_preview(bytes, 16))
                        .unwrap_or_default()
                ));
                break;
            }
        }
    }
    if cursor.remaining() > 0 {
        frames.push(format!(
            "trailing@{}:bytes={}:preview={}",
            cursor.position(),
            cursor.remaining(),
            requests
                .get(cursor.position()..)
                .map(|bytes| hex_preview(bytes, 16))
                .unwrap_or_default()
        ));
    }
    (frames.len(), frames.join("|"))
}

fn summarize_response_rop_buffer(
    rop_buffer: &[u8],
    request_rop_ids: &[u8],
) -> RopResponseDebugSummary {
    let mut summary = RopResponseDebugSummary {
        extended: is_rpc_header_ext_rop_buffer(rop_buffer),
        buffer_layout: rop_buffer_layout_name(rop_buffer).to_string(),
        buffer_size_word: rop_buffer_size_word(rop_buffer)
            .map(|value| value.to_string())
            .unwrap_or_else(|| "invalid".to_string()),
        ..RopResponseDebugSummary::default()
    };
    let Some((responses, handle_table)) = split_rop_buffer(rop_buffer) else {
        summary.parse_error = "invalid ROP buffer".to_string();
        return summary;
    };
    summary.response_payload_bytes = responses.len();
    summary.handle_table_bytes = handle_table.len();
    let handle_summary = summarize_handle_table(handle_table, &mut summary.parse_error);
    summary.handle_count = handle_summary.0;
    summary.handle_table_summary = handle_summary.1;

    let mut offset = 0usize;
    let mut ids = Vec::new();
    let mut results = Vec::new();
    let expected_ids = request_rop_ids
        .iter()
        .copied()
        .filter(|rop_id| !rop_has_no_response(*rop_id))
        .take(MAX_ROP_DEBUG_ENTRIES)
        .collect::<Vec<_>>();
    let mut frames = Vec::new();
    for (expected_index, expected_rop_id) in expected_ids.iter().copied().enumerate() {
        let Some(found) = responses.get(offset..).and_then(|remaining| {
            remaining
                .iter()
                .position(|rop_id| *rop_id == expected_rop_id)
        }) else {
            break;
        };
        offset += found;
        let rop_id = responses[offset];
        ids.push(rop_id);
        let error_code = read_response_error_code(responses, offset);
        if let Some(error_code) = error_code {
            results.push(format!("{}:{error_code:#010x}", rop_id_hex(rop_id)));
        } else {
            results.push(format!("{}:truncated", rop_id_hex(rop_id)));
        }
        frames.push(summarize_response_rop_frame(
            responses,
            offset,
            error_code,
            expected_ids.get(expected_index + 1).copied(),
        ));
        offset = offset.saturating_add(6);
    }

    summary.count = ids.len();
    summary.ids_csv = rop_ids_csv(&ids);
    summary.results_csv = results.join(",");
    summary.frames = frames.join("|");
    summary
}

fn rop_has_no_response(rop_id: u8) -> bool {
    matches!(rop_id, 0x01)
}

fn execute_response_framing_context(request_rop_ids: &[u8]) -> Option<&'static str> {
    if request_rop_ids.contains(&0x70) || request_rop_ids.contains(&0x4E) {
        return Some("hierarchy_sync");
    }
    if request_rop_ids
        .iter()
        .all(|rop_id| matches!(*rop_id, 0x0A | 0x79))
        && request_rop_ids
            .iter()
            .any(|rop_id| matches!(*rop_id, 0x0A | 0x79))
    {
        return Some("setprops");
    }
    if request_rop_ids
        .iter()
        .all(|rop_id| matches!(*rop_id, 0x01 | 0x07))
        && request_rop_ids.contains(&0x07)
    {
        return Some("getprops_or_release_getprops");
    }
    if request_rop_ids.iter().all(|rop_id| matches!(*rop_id, 0x01))
        && request_rop_ids.contains(&0x01)
    {
        return Some("release_only");
    }
    None
}

fn summarize_response_rop_frame(
    responses: &[u8],
    start: usize,
    error_code: Option<u32>,
    next_expected_rop_id: Option<u8>,
) -> String {
    let end = next_expected_rop_id
        .and_then(|rop_id| {
            responses.get(start + 1..).and_then(|remaining| {
                remaining
                    .iter()
                    .position(|candidate| *candidate == rop_id)
                    .map(|found| start + 1 + found)
            })
        })
        .unwrap_or(responses.len());
    let rop_id = responses.get(start).copied().unwrap_or_default();
    let output_handle_index = responses
        .get(start + 1)
        .map(|value| value.to_string())
        .unwrap_or_else(|| "truncated".to_string());
    let result = error_code
        .map(|code| format!("{code:#010x}"))
        .unwrap_or_else(|| "truncated".to_string());
    let preview_end = end.min(start.saturating_add(16));
    let preview = responses
        .get(start..preview_end)
        .map(|bytes| bytes_to_hex(bytes))
        .unwrap_or_default();
    format!(
        "{}@{}..{}:len={}:out={}:rv={}:preview={}",
        rop_id_hex(rop_id),
        start,
        end,
        end.saturating_sub(start),
        output_handle_index,
        result,
        preview
    )
}

fn rop_buffer_size_word(rop_buffer: &[u8]) -> Option<u16> {
    let payload = rpc_header_ext_payload(rop_buffer).unwrap_or(rop_buffer);
    let bytes = payload.get(..2)?;
    Some(u16::from_le_bytes(bytes.try_into().ok()?))
}

fn rop_buffer_layout_name(rop_buffer: &[u8]) -> &'static str {
    let Some((responses, _handle_table)) = split_rop_buffer(rop_buffer) else {
        return "invalid";
    };
    let Some(size_word) = rop_buffer_size_word(rop_buffer).map(usize::from) else {
        return "invalid";
    };
    if is_rpc_header_ext_rop_buffer(rop_buffer) {
        if size_word == responses.len().saturating_add(2) {
            "rpc_header_ext_spec"
        } else {
            "rpc_header_ext_unknown"
        }
    } else if size_word == responses.len().saturating_add(2) {
        "spec"
    } else if size_word == responses.len() {
        "legacy"
    } else {
        "unknown"
    }
}

fn summarize_logon_response_rop(
    rop_buffer: &[u8],
    request_rop_ids: &[u8],
) -> LogonResponseDebugSummary {
    if !request_rop_ids.contains(&0xFE) {
        return LogonResponseDebugSummary::default();
    }
    let mut summary = LogonResponseDebugSummary {
        present: true,
        ..LogonResponseDebugSummary::default()
    };
    let Some((responses, _handle_table)) = split_rop_buffer(rop_buffer) else {
        summary.parse_error = "invalid ROP buffer".to_string();
        return summary;
    };
    let Some(offset) = responses.iter().position(|rop_id| *rop_id == 0xFE) else {
        summary.parse_error = "missing RopLogon response".to_string();
        return summary;
    };
    let result = (|| -> Result<()> {
        let mut cursor = Cursor::new(&responses[offset..]);
        let rop_id = cursor.read_u8()?;
        if rop_id != 0xFE {
            return Err(anyhow::anyhow!("unexpected ROP response"));
        }
        summary.output_handle_index = cursor.read_u8()?.to_string();
        let error_code = cursor.read_u32()?;
        summary.error_code = format!("{error_code:#010x}");
        if error_code != 0 {
            return Ok(());
        }
        summary.logon_flags = format!("{:#04x}", cursor.read_u8()?);
        let mut folder_ids = Vec::with_capacity(PRIVATE_LOGON_SPECIAL_FOLDER_IDS.len());
        for _ in PRIVATE_LOGON_SPECIAL_FOLDER_IDS {
            let bytes = cursor.read_bytes(8)?;
            let folder_id = crate::mapi::identity::object_id_from_wire_id(bytes)
                .unwrap_or_else(|| u64::from_le_bytes(bytes.try_into().unwrap_or_default()));
            folder_ids.push(format!("{folder_id:#018x}"));
        }
        summary.special_folder_ids = folder_ids.join(",");
        summary.response_flags = format!("{:#04x}", cursor.read_u8()?);
        summary.mailbox_guid = read_guid_le(&mut cursor)?;
        summary.replid = cursor.read_u16()?.to_string();
        summary.replica_guid = bytes_to_hex(cursor.read_bytes(16)?);
        cursor.read_bytes(8)?;
        read_u64(&mut cursor)?;
        cursor.read_u32()?;
        Ok(())
    })();
    if let Err(error) = result {
        summary.parse_error = error.to_string();
    }
    summary
}

fn read_u64(cursor: &mut Cursor<'_>) -> Result<u64> {
    let bytes = cursor.read_bytes(8)?;
    Ok(u64::from_le_bytes(bytes.try_into()?))
}

fn read_guid_le(cursor: &mut Cursor<'_>) -> Result<String> {
    let bytes = cursor.read_bytes(16)?;
    Ok(Uuid::from_bytes_le(bytes.try_into()?).to_string())
}

fn bytes_to_hex(bytes: &[u8]) -> String {
    bytes
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<Vec<_>>()
        .join("")
}

fn log_calendar_folder_contract(
    principal: &AccountPrincipal,
    folder_id: u64,
    mailbox_folder_found: bool,
    collaboration_folder_found: bool,
    advertised_special_folder: bool,
    snapshot: &MapiMailStoreSnapshot,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
) {
    if folder_id != CALENDAR_FOLDER_ID {
        return;
    }
    let entry_id = crate::mapi::identity::folder_entry_id_from_object_id(
        principal.account_id,
        CALENDAR_FOLDER_ID,
    )
    .unwrap_or_default();
    let decoded_entry_id = crate::mapi::identity::object_id_from_folder_entry_id(&entry_id);
    let decoded_entry_id_hex = decoded_entry_id
        .map(|folder_id| format!("0x{folder_id:016x}"))
        .unwrap_or_default();
    let source_key = mapi_mailstore::source_key_for_store_id(CALENDAR_FOLDER_ID);
    let parent_source_key = mapi_mailstore::source_key_for_store_id(IPM_SUBTREE_FOLDER_ID);
    let calendar_folder = snapshot.collaboration_folder_for_id(CALENDAR_FOLDER_ID);
    let calendar_collection_count = snapshot
        .collaboration_folders()
        .iter()
        .filter(|folder| folder.kind == MapiCollaborationFolderKind::Calendar)
        .count();
    let calendar_access = snapshot.folder_access_for_principal(folder_id, principal.account_id);
    log_calendar_identity_chain(
        principal,
        "open_folder",
        folder_id,
        None,
        None,
        Some(snapshot),
    );
    tracing::info!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = "emsmdb",
        mailbox = %principal.email,
        request_type = "Execute",
        request_rop_id = "0x02",
        folder_id = "0x0000000000100001",
        expected_parent_folder_id = "0x0000000000040001",
        expected_container_class = "IPF.Appointment",
        expected_item_message_class = "IPM.Appointment",
        default_entry_id_bytes = entry_id.len(),
        default_entry_id_preview = %hex_preview(&entry_id, 24),
        default_entry_id_decoded_folder_id = %decoded_entry_id_hex,
        default_entry_id_decodes_to_calendar = decoded_entry_id == Some(CALENDAR_FOLDER_ID),
        source_key = %bytes_to_hex(&source_key),
        parent_source_key = %bytes_to_hex(&parent_source_key),
        mailbox_folder_found = mailbox_folder_found,
        collaboration_folder_found = collaboration_folder_found,
        advertised_special_folder = advertised_special_folder,
        canonical_calendar_collection_count = calendar_collection_count,
        canonical_calendar_collection_id =
            calendar_folder.map(|folder| folder.collection.id.as_str()).unwrap_or(""),
        canonical_calendar_collection_name =
            calendar_folder.map(|folder| folder.collection.display_name.as_str()).unwrap_or(""),
        canonical_calendar_item_count =
            calendar_folder.map(|folder| folder.item_count).unwrap_or(0),
        projected_calendar_event_count = snapshot.events_for_folder(CALENDAR_FOLDER_ID).len(),
        projected_folder_content_count =
            folder_message_count(folder_id, mailboxes, emails, snapshot),
        mapi_folder_access_mask = %format!("0x{MAPI_FOLDER_ACCESS:08x}"),
        acl_access_row_present = calendar_access.is_some(),
        acl_may_read = calendar_access.map(|access| access.may_read).unwrap_or(true),
        acl_may_write = calendar_access.map(|access| access.may_write).unwrap_or(true),
        acl_may_delete = calendar_access.map(|access| access.may_delete).unwrap_or(true),
        message = "rca debug mapi calendar folder contract"
    );
}

fn log_calendar_identity_chain(
    principal: &AccountPrincipal,
    stage: &str,
    observed_folder_id: u64,
    checkpoint_mailbox_id: Option<Uuid>,
    sync_type: Option<u8>,
    snapshot: Option<&MapiMailStoreSnapshot>,
) {
    if observed_folder_id != CALENDAR_FOLDER_ID {
        return;
    }
    let default_entry_id = crate::mapi::identity::folder_entry_id_from_object_id(
        principal.account_id,
        CALENDAR_FOLDER_ID,
    )
    .unwrap_or_default();
    let default_entry_id_decoded =
        crate::mapi::identity::object_id_from_folder_entry_id(&default_entry_id);
    let source_key = mapi_mailstore::source_key_for_store_id(CALENDAR_FOLDER_ID);
    let source_key_decoded = crate::mapi::identity::object_id_from_source_key(&source_key);
    let parent_source_key = mapi_mailstore::source_key_for_store_id(IPM_SUBTREE_FOLDER_ID);
    let parent_source_key_decoded =
        crate::mapi::identity::object_id_from_source_key(&parent_source_key);
    let expected_checkpoint_mailbox_id =
        mapi_mailstore::virtual_special_mailbox(CALENDAR_FOLDER_ID).map(|mailbox| mailbox.id);
    let checkpoint_mailbox_id_matches_expected =
        checkpoint_mailbox_id.is_some() && checkpoint_mailbox_id == expected_checkpoint_mailbox_id;
    let checkpoint_identity_ok =
        checkpoint_mailbox_id.is_none() || checkpoint_mailbox_id_matches_expected;
    let calendar_identity_chain_complete = default_entry_id_decoded == Some(CALENDAR_FOLDER_ID)
        && source_key_decoded == Some(CALENDAR_FOLDER_ID)
        && parent_source_key_decoded == Some(IPM_SUBTREE_FOLDER_ID)
        && checkpoint_identity_ok;
    let calendar_folder =
        snapshot.and_then(|snapshot| snapshot.collaboration_folder_for_id(CALENDAR_FOLDER_ID));
    let projected_calendar_event_count = snapshot
        .map(|snapshot| snapshot.events_for_folder(CALENDAR_FOLDER_ID).len())
        .unwrap_or_default();
    tracing::info!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = "emsmdb",
        mailbox = %principal.email,
        request_type = "Execute",
        stage = stage,
        default_folder_property_tag = "0x36d00102",
        default_folder_property_name = "PidTagIpmAppointmentEntryId",
        default_entry_id_bytes = default_entry_id.len(),
        default_entry_id_preview = %hex_preview(&default_entry_id, 24),
        default_entry_id_decoded_folder_id =
            %format_optional_folder_id(default_entry_id_decoded),
        observed_folder_id = %format!("0x{observed_folder_id:016x}"),
        source_key = %bytes_to_hex(&source_key),
        source_key_decoded_folder_id = %format_optional_folder_id(source_key_decoded),
        parent_source_key = %bytes_to_hex(&parent_source_key),
        parent_source_key_decoded_folder_id =
            %format_optional_folder_id(parent_source_key_decoded),
        replica_guid = %bytes_to_hex(&crate::mapi::identity::STORE_REPLICA_GUID),
        replid = 1u16,
        sync_type = %sync_type.map(|value| format!("0x{value:02x}")).unwrap_or_default(),
        checkpoint_mailbox_id = %checkpoint_mailbox_id
            .map(|id| id.to_string())
            .unwrap_or_default(),
        expected_checkpoint_mailbox_id = %expected_checkpoint_mailbox_id
            .map(|id| id.to_string())
            .unwrap_or_default(),
        checkpoint_mailbox_id_matches_expected,
        checkpoint_identity_ok,
        canonical_calendar_collection_present = calendar_folder.is_some(),
        canonical_calendar_collection_id =
            calendar_folder.map(|folder| folder.collection.id.as_str()).unwrap_or(""),
        projected_calendar_event_count,
        calendar_identity_chain_complete,
        calendar_identity_chain_key = %format!(
            "entry={};open=0x{observed_folder_id:016x};source={};parent={};checkpoint={}",
            format_optional_folder_id(default_entry_id_decoded),
            format_optional_folder_id(source_key_decoded),
            format_optional_folder_id(parent_source_key_decoded),
            checkpoint_mailbox_id.map(|id| id.to_string()).unwrap_or_default(),
        ),
        message = "rca debug mapi calendar identity chain"
    );
}

fn log_special_folder_contract(
    principal: &AccountPrincipal,
    folder_id: u64,
    mailbox_folder_found: bool,
    collaboration_folder_found: bool,
    advertised_special_folder: bool,
    snapshot: &MapiMailStoreSnapshot,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
) {
    if folder_id == CALENDAR_FOLDER_ID || !is_rca_special_contract_folder(folder_id) {
        return;
    }
    let entry_id =
        crate::mapi::identity::folder_entry_id_from_object_id(principal.account_id, folder_id)
            .unwrap_or_default();
    let decoded_entry_id = crate::mapi::identity::object_id_from_folder_entry_id(&entry_id);
    let decoded_entry_id_hex = decoded_entry_id
        .map(|folder_id| format!("0x{folder_id:016x}"))
        .unwrap_or_default();
    let source_key = mapi_mailstore::source_key_for_store_id(folder_id);
    let parent_source_key = mapi_mailstore::source_key_for_store_id(IPM_SUBTREE_FOLDER_ID);
    let collaboration_folder = snapshot.collaboration_folder_for_id(folder_id);
    let canonical_collection_kind = collaboration_folder
        .map(|folder| format!("{:?}", folder.kind))
        .unwrap_or_default();
    let folder_access = snapshot.folder_access_for_principal(folder_id, principal.account_id);
    tracing::info!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = "emsmdb",
        mailbox = %principal.email,
        request_type = "Execute",
        request_rop_id = "0x02",
        folder_id = %format!("0x{folder_id:016x}"),
        folder_role = role_for_folder_id(folder_id).unwrap_or(""),
        expected_parent_folder_id = "0x0000000000040001",
        expected_container_class = expected_special_folder_container_class(folder_id),
        expected_item_message_class = expected_special_folder_item_message_class(folder_id),
        default_entry_id_bytes = entry_id.len(),
        default_entry_id_preview = %hex_preview(&entry_id, 24),
        default_entry_id_decoded_folder_id = %decoded_entry_id_hex,
        default_entry_id_matches_requested_folder = decoded_entry_id == Some(folder_id),
        source_key = %bytes_to_hex(&source_key),
        parent_source_key = %bytes_to_hex(&parent_source_key),
        mailbox_folder_found = mailbox_folder_found,
        collaboration_folder_found = collaboration_folder_found,
        advertised_special_folder = advertised_special_folder,
        canonical_collection_present = collaboration_folder.is_some(),
        canonical_collection_kind = %canonical_collection_kind,
        canonical_collection_id =
            collaboration_folder.map(|folder| folder.collection.id.as_str()).unwrap_or(""),
        canonical_collection_name =
            collaboration_folder.map(|folder| folder.collection.display_name.as_str()).unwrap_or(""),
        canonical_item_count = collaboration_folder.map(|folder| folder.item_count).unwrap_or(0),
        projected_special_object_count =
            special_sync_objects_for(folder_id, 0x01, snapshot, principal.account_id).len(),
        projected_folder_content_count =
            folder_message_count(folder_id, mailboxes, emails, snapshot),
        mapi_folder_access_mask = %format!("0x{MAPI_FOLDER_ACCESS:08x}"),
        acl_access_row_present = folder_access.is_some(),
        acl_may_read = folder_access.map(|access| access.may_read).unwrap_or(true),
        acl_may_write = folder_access.map(|access| access.may_write).unwrap_or(true),
        acl_may_delete = folder_access.map(|access| access.may_delete).unwrap_or(true),
        message = "rca debug mapi special folder contract"
    );
}

fn log_calendar_special_sync_objects(
    principal: &AccountPrincipal,
    folder_id: u64,
    sync_type: u8,
    objects: &[mapi_mailstore::SpecialMessageSyncFact],
) {
    if folder_id != CALENDAR_FOLDER_ID || sync_type != 0x01 {
        return;
    }
    let item_ids = objects
        .iter()
        .map(|object| format!("0x{:016x}", object.item_id))
        .collect::<Vec<_>>()
        .join(",");
    let source_keys = objects
        .iter()
        .map(|object| bytes_to_hex(&mapi_mailstore::source_key_for_store_id(object.item_id)))
        .collect::<Vec<_>>()
        .join(",");
    let canonical_ids = objects
        .iter()
        .map(|object| object.canonical_id.to_string())
        .collect::<Vec<_>>()
        .join(",");
    let message_classes = objects
        .iter()
        .map(|object| object.message_class.as_str())
        .collect::<Vec<_>>()
        .join(",");
    let subject_lengths = objects
        .iter()
        .map(|object| object.subject.chars().count().to_string())
        .collect::<Vec<_>>()
        .join(",");
    let body_lengths = objects
        .iter()
        .map(|object| object.body_text.chars().count().to_string())
        .collect::<Vec<_>>()
        .join(",");
    let message_sizes = objects
        .iter()
        .map(|object| object.message_size.to_string())
        .collect::<Vec<_>>()
        .join(",");
    let property_tag_count = objects
        .iter()
        .map(|object| object.named_properties.len())
        .sum::<usize>();
    let property_tags = objects
        .iter()
        .flat_map(|object| object.named_properties.iter().map(|(tag, _)| *tag))
        .map(|tag| format!("0x{tag:08x}"))
        .collect::<Vec<_>>()
        .join(",");
    let property_shapes = objects
        .iter()
        .flat_map(|object| {
            object
                .named_properties
                .iter()
                .map(|(tag, value)| format!("0x{tag:08x}:{}", special_property_shape(value)))
        })
        .collect::<Vec<_>>()
        .join(",");
    let required_tags = [
        PID_TAG_START_DATE,
        PID_TAG_END_DATE,
        PID_LID_BUSY_STATUS_TAG,
        PID_LID_LOCATION_W_TAG,
        PID_LID_APPOINTMENT_START_WHOLE_TAG,
        PID_LID_APPOINTMENT_END_WHOLE_TAG,
        PID_LID_APPOINTMENT_DURATION_TAG,
        PID_LID_TIME_ZONE_STRUCT_TAG,
        PID_LID_TIME_ZONE_DESCRIPTION_W_TAG,
        PID_LID_APPOINTMENT_TIME_ZONE_DEFINITION_START_DISPLAY_TAG,
        PID_LID_APPOINTMENT_TIME_ZONE_DEFINITION_END_DISPLAY_TAG,
        PID_LID_GLOBAL_OBJECT_ID_TAG,
        PID_LID_CLEAN_GLOBAL_OBJECT_ID_TAG,
    ];
    let missing_required_tags = required_tags
        .iter()
        .copied()
        .filter(|tag| {
            !objects.iter().any(|object| {
                object
                    .named_properties
                    .iter()
                    .any(|(present, _)| present == tag)
            })
        })
        .map(|tag| format!("0x{tag:08x}"))
        .collect::<Vec<_>>()
        .join(",");
    let start_end_order_ok = objects.iter().all(calendar_sync_object_start_end_order_ok);
    let global_object_id_lengths = objects
        .iter()
        .map(|object| {
            special_binary_property_len(object, PID_LID_GLOBAL_OBJECT_ID_TAG)
                .map(|len| len.to_string())
                .unwrap_or_else(|| "missing".to_string())
        })
        .collect::<Vec<_>>()
        .join(",");
    let clean_global_object_id_lengths = objects
        .iter()
        .map(|object| {
            special_binary_property_len(object, PID_LID_CLEAN_GLOBAL_OBJECT_ID_TAG)
                .map(|len| len.to_string())
                .unwrap_or_else(|| "missing".to_string())
        })
        .collect::<Vec<_>>()
        .join(",");
    tracing::info!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = "emsmdb",
        mailbox = %principal.email,
        request_type = "Execute",
        request_rop_id = "0x70",
        folder_id = "0x0000000000100001",
        sync_type = "0x01",
        calendar_object_count = objects.len(),
        calendar_item_ids = %item_ids,
        calendar_source_keys = %source_keys,
        calendar_canonical_ids = %canonical_ids,
        calendar_message_classes = %message_classes,
        calendar_subject_char_counts = %subject_lengths,
        calendar_body_char_counts = %body_lengths,
        calendar_message_sizes = %message_sizes,
        calendar_property_tag_count = property_tag_count,
        calendar_property_tags = %property_tags,
        calendar_property_shapes = %property_shapes,
        calendar_required_property_tags = %format_debug_property_tags(&required_tags),
        calendar_missing_required_property_tags = %missing_required_tags,
        calendar_required_properties_complete = missing_required_tags.is_empty(),
        calendar_start_end_order_ok = start_end_order_ok,
        calendar_global_object_id_lengths = %global_object_id_lengths,
        calendar_clean_global_object_id_lengths = %clean_global_object_id_lengths,
        message = "rca debug mapi calendar special sync objects"
    );
}

fn log_special_sync_objects(
    principal: &AccountPrincipal,
    folder_id: u64,
    sync_type: u8,
    objects: &[mapi_mailstore::SpecialMessageSyncFact],
) {
    if folder_id == CALENDAR_FOLDER_ID || sync_type != 0x01 || objects.is_empty() {
        return;
    }
    let item_ids = objects
        .iter()
        .map(|object| format!("0x{:016x}", object.item_id))
        .collect::<Vec<_>>()
        .join(",");
    let source_keys = objects
        .iter()
        .map(|object| bytes_to_hex(&mapi_mailstore::source_key_for_store_id(object.item_id)))
        .collect::<Vec<_>>()
        .join(",");
    let canonical_ids = objects
        .iter()
        .map(|object| object.canonical_id.to_string())
        .collect::<Vec<_>>()
        .join(",");
    let message_classes = objects
        .iter()
        .map(|object| object.message_class.as_str())
        .collect::<Vec<_>>()
        .join(",");
    let subject_lengths = objects
        .iter()
        .map(|object| object.subject.chars().count().to_string())
        .collect::<Vec<_>>()
        .join(",");
    let body_lengths = objects
        .iter()
        .map(|object| object.body_text.chars().count().to_string())
        .collect::<Vec<_>>()
        .join(",");
    let property_tag_count = objects
        .iter()
        .map(|object| object.named_properties.len())
        .sum::<usize>();
    let property_tags = objects
        .iter()
        .flat_map(|object| object.named_properties.iter().map(|(tag, _)| *tag))
        .map(|tag| format!("0x{tag:08x}"))
        .collect::<Vec<_>>()
        .join(",");
    let property_shapes = objects
        .iter()
        .flat_map(|object| {
            object
                .named_properties
                .iter()
                .map(|(tag, value)| format!("0x{tag:08x}:{}", special_property_shape(value)))
        })
        .collect::<Vec<_>>()
        .join(",");
    let associated_count = objects.iter().filter(|object| object.associated).count();
    tracing::info!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = "emsmdb",
        mailbox = %principal.email,
        request_type = "Execute",
        request_rop_id = "0x70",
        folder_id = %format!("0x{folder_id:016x}"),
        folder_role = role_for_folder_id(folder_id).unwrap_or(""),
        sync_type = "0x01",
        special_object_count = objects.len(),
        special_associated_object_count = associated_count,
        special_item_ids = %item_ids,
        special_source_keys = %source_keys,
        special_canonical_ids = %canonical_ids,
        special_message_classes = %message_classes,
        special_subject_char_counts = %subject_lengths,
        special_body_char_counts = %body_lengths,
        special_property_tag_count = property_tag_count,
        special_property_tags = %property_tags,
        special_property_shapes = %property_shapes,
        message = "rca debug mapi special sync objects"
    );
}

fn sync_mailboxes_with_collaboration_counts(
    mut mailboxes: Vec<JmapMailbox>,
    snapshot: &MapiMailStoreSnapshot,
) -> Vec<JmapMailbox> {
    for mailbox in &mut mailboxes {
        let Some(folder_id) = try_mapi_folder_id(mailbox) else {
            continue;
        };
        if let Some(folder) = snapshot.collaboration_folder_for_id(folder_id) {
            mailbox.total_emails = folder.item_count;
            mailbox.unread_emails = 0;
        }
    }
    mailboxes
}

fn is_rca_special_contract_folder(folder_id: u64) -> bool {
    matches!(
        folder_id,
        CONTACTS_FOLDER_ID
            | JOURNAL_FOLDER_ID
            | NOTES_FOLDER_ID
            | TASKS_FOLDER_ID
            | REMINDERS_FOLDER_ID
            | SUGGESTED_CONTACTS_FOLDER_ID
            | QUICK_CONTACTS_FOLDER_ID
            | IM_CONTACT_LIST_FOLDER_ID
            | CONTACTS_SEARCH_FOLDER_ID
            | TODO_SEARCH_FOLDER_ID
            | CONVERSATION_ACTION_SETTINGS_FOLDER_ID
    )
}

fn expected_special_folder_container_class(folder_id: u64) -> &'static str {
    match folder_id {
        CONTACTS_FOLDER_ID
        | SUGGESTED_CONTACTS_FOLDER_ID
        | QUICK_CONTACTS_FOLDER_ID
        | IM_CONTACT_LIST_FOLDER_ID
        | CONTACTS_SEARCH_FOLDER_ID => "IPF.Contact",
        CALENDAR_FOLDER_ID => "IPF.Appointment",
        JOURNAL_FOLDER_ID => "IPF.Journal",
        NOTES_FOLDER_ID => "IPF.StickyNote",
        TASKS_FOLDER_ID | TODO_SEARCH_FOLDER_ID => "IPF.Task",
        REMINDERS_FOLDER_ID => "Outlook.Reminder",
        CONVERSATION_ACTION_SETTINGS_FOLDER_ID => "IPF.Configuration",
        _ => "",
    }
}

fn expected_special_folder_item_message_class(folder_id: u64) -> &'static str {
    match folder_id {
        CONTACTS_FOLDER_ID
        | SUGGESTED_CONTACTS_FOLDER_ID
        | QUICK_CONTACTS_FOLDER_ID
        | IM_CONTACT_LIST_FOLDER_ID
        | CONTACTS_SEARCH_FOLDER_ID => "IPM.Contact",
        JOURNAL_FOLDER_ID => "IPM.Activity",
        NOTES_FOLDER_ID => "IPM.StickyNote",
        TASKS_FOLDER_ID | TODO_SEARCH_FOLDER_ID => "IPM.Task",
        REMINDERS_FOLDER_ID => "Outlook.Reminder",
        CONVERSATION_ACTION_SETTINGS_FOLDER_ID => "IPM.Configuration",
        _ => "",
    }
}

fn calendar_sync_object_start_end_order_ok(
    object: &mapi_mailstore::SpecialMessageSyncFact,
) -> bool {
    let start = special_i64_property(object, PID_TAG_START_DATE)
        .or_else(|| special_i64_property(object, PID_LID_APPOINTMENT_START_WHOLE_TAG));
    let end = special_i64_property(object, PID_TAG_END_DATE)
        .or_else(|| special_i64_property(object, PID_LID_APPOINTMENT_END_WHOLE_TAG));
    match (start, end) {
        (Some(start), Some(end)) => start < end,
        _ => false,
    }
}

fn special_i64_property(
    object: &mapi_mailstore::SpecialMessageSyncFact,
    property_tag: u32,
) -> Option<i64> {
    object
        .named_properties
        .iter()
        .find_map(|(tag, value)| match (*tag == property_tag, value) {
            (true, mapi_mailstore::SpecialMessagePropertyValue::I64(value)) => Some(*value),
            _ => None,
        })
}

fn special_binary_property_len(
    object: &mapi_mailstore::SpecialMessageSyncFact,
    property_tag: u32,
) -> Option<usize> {
    object
        .named_properties
        .iter()
        .find_map(|(tag, value)| match (*tag == property_tag, value) {
            (true, mapi_mailstore::SpecialMessagePropertyValue::Binary(value)) => Some(value.len()),
            _ => None,
        })
}

fn special_property_shape(value: &mapi_mailstore::SpecialMessagePropertyValue) -> String {
    match value {
        mapi_mailstore::SpecialMessagePropertyValue::Binary(value) => {
            format!("binary:bytes={}", value.len())
        }
        mapi_mailstore::SpecialMessagePropertyValue::Bool(value) => {
            format!("bool={value}")
        }
        mapi_mailstore::SpecialMessagePropertyValue::Guid(value) => {
            format!("guid={}", bytes_to_hex(value))
        }
        mapi_mailstore::SpecialMessagePropertyValue::I32(value) => {
            format!("i32={value}")
        }
        mapi_mailstore::SpecialMessagePropertyValue::I64(value) => {
            format!("i64={value}")
        }
        mapi_mailstore::SpecialMessagePropertyValue::U32(value) => {
            format!("u32={value}")
        }
        mapi_mailstore::SpecialMessagePropertyValue::U64(value) => {
            format!("u64={value}")
        }
        mapi_mailstore::SpecialMessagePropertyValue::String(value) => {
            format!("string:chars={}", value.chars().count())
        }
        mapi_mailstore::SpecialMessagePropertyValue::MultiString(values) => {
            format!("multistring:count={}", values.len())
        }
        mapi_mailstore::SpecialMessagePropertyValue::Time(value) => {
            format!("time:chars={}", value.chars().count())
        }
    }
}

fn summarize_handle_table(handle_table: &[u8], parse_error: &mut String) -> (usize, String) {
    match read_handle_table(handle_table) {
        Ok(handles) => {
            let handles_csv = handles
                .iter()
                .map(|handle| format!("0x{handle:08x}"))
                .collect::<Vec<_>>()
                .join(",");
            (
                handles.len(),
                format!("count={};handles={handles_csv}", handles.len()),
            )
        }
        Err(error) => {
            *parse_error = error.to_string();
            let count = handle_table.len() / 4;
            (
                count,
                format!(
                    "invalid;bytes={};best_effort_count={count}",
                    handle_table.len()
                ),
            )
        }
    }
}

fn read_response_error_code(responses: &[u8], offset: usize) -> Option<u32> {
    let bytes = responses.get(offset + 2..offset + 6)?;
    Some(u32::from_le_bytes(bytes.try_into().ok()?))
}

fn rop_ids_csv(rop_ids: &[u8]) -> String {
    rop_ids
        .iter()
        .map(|rop_id| rop_id_hex(*rop_id))
        .collect::<Vec<_>>()
        .join(",")
}

fn rop_id_hex(rop_id: u8) -> String {
    format!("0x{rop_id:02x}")
}

fn pending_recipient_upsert_count(changes: &[PendingRecipientChange]) -> usize {
    changes
        .iter()
        .filter(|change| matches!(change, PendingRecipientChange::Upsert(_)))
        .count()
}

fn pending_recipient_delete_count(changes: &[PendingRecipientChange]) -> usize {
    changes
        .iter()
        .filter(|change| matches!(change, PendingRecipientChange::Delete(_)))
        .count()
}

fn pending_recipient_types_summary(changes: &[PendingRecipientChange]) -> String {
    changes
        .iter()
        .filter_map(|change| match change {
            PendingRecipientChange::Upsert(recipient) => Some(recipient.recipient_type.to_string()),
            PendingRecipientChange::Delete(_) => None,
        })
        .collect::<Vec<_>>()
        .join(",")
}

fn pending_recipient_row_ids_summary(changes: &[PendingRecipientChange]) -> String {
    changes
        .iter()
        .map(|change| match change {
            PendingRecipientChange::Upsert(recipient) => recipient.row_id.to_string(),
            PendingRecipientChange::Delete(row_id) => format!("delete:{row_id}"),
        })
        .collect::<Vec<_>>()
        .join(",")
}

pub(in crate::mapi) async fn execute_rops<S, V>(
    store: &S,
    principal: &AccountPrincipal,
    session: &mut MapiSession,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    validator: &Validator<V>,
    rop_buffer: &[u8],
) -> Vec<u8>
where
    S: ExchangeStore,
    V: Detector,
{
    let Some((requests, handle_table)) = split_rop_buffer(rop_buffer) else {
        return rop_buffer_with_response(rop_parse_error_response(), &[]);
    };
    let extended = is_rpc_header_ext_rop_buffer(rop_buffer);
    let mut handle_slots = match read_handle_table(handle_table) {
        Ok(handle_slots) => handle_slots,
        Err(_) => {
            let response = if extended {
                rop_buffer_with_response_spec(rop_parse_error_response(), &[])
            } else {
                rop_buffer_with_response(rop_parse_error_response(), &[])
            };
            return if extended {
                rpc_header_ext_rop_buffer(response)
            } else {
                response
            };
        }
    };
    if let Some(max_input_handle) = handle_slots
        .iter()
        .copied()
        .filter(|handle| *handle != u32::MAX)
        .max()
    {
        session.next_handle = session.next_handle.max(max_input_handle.saturating_add(1));
    }

    let mut cursor = Cursor::new(requests);
    let mut responses = Vec::new();
    let mut output_handles = Vec::new();
    let mut post_hierarchy_release_events = Vec::new();
    let mut created_emails: Vec<JmapEmail> = Vec::new();
    let mut echo_input_handle_table = false;
    while cursor.remaining() > 0 {
        let request = match read_rop_request(&mut cursor) {
            Ok(request) => request,
            Err(_) => {
                responses.extend_from_slice(&rop_parse_error_response());
                break;
            }
        };
        let typed_request = request.typed();
        let mut completed_hierarchy_sync = None;
        let mut content_sync_configure_observed = false;
        if matches!(request.rop_id, 0x07 | 0x0B | 0x7A)
            && !property_tags_are_supported(&request.property_tags())
        {
            responses.extend_from_slice(&rop_error_response(
                request.rop_id,
                request.response_handle_index(),
                0x8004_0102,
            ));
            break;
        }
        match RopId::from_u8(typed_request.rop_id()) {
            Some(RopId::Release) => {
                let released_object = input_object(session, &handle_slots, &request);
                if session.hierarchy_sync_completed() {
                    let remaining_before = session.handles.len();
                    post_hierarchy_release_events.push(PostHierarchyReleaseDebugEvent {
                        input_handle_index: request.input_handle_index().unwrap_or(0),
                        handle: format_optional_debug_handle(input_handle(&handle_slots, &request)),
                        object_kind: mapi_object_debug_kind(released_object).to_string(),
                        folder_id: mapi_object_debug_folder_id(released_object),
                        remaining_before,
                        remaining_after: remaining_before,
                        logon_before_content_sync: matches!(
                            released_object,
                            Some(MapiObject::Logon)
                        ) && !session
                            .post_hierarchy_actions
                            .content_sync_configure_observed,
                    });
                }
                if matches!(released_object, Some(MapiObject::Logon)) {
                    session.record_logoff_after_hierarchy_completion();
                }
                release_handle_slot(session, &mut handle_slots, &request);
                if let Some(event) = post_hierarchy_release_events.last_mut() {
                    event.remaining_after = session.handles.len();
                }
            }
            Some(RopId::OpenFolder) => {
                let folder_id = request.folder_id().unwrap_or(ROOT_FOLDER_ID);
                let mailbox_folder_found = folder_row_for_id(folder_id, mailboxes).is_some();
                let collaboration_folder_found =
                    snapshot.collaboration_folder_for_id(folder_id).is_some();
                let advertised_special_folder = is_advertised_special_folder(folder_id);
                let open_folder_result = if mailbox_folder_found
                    || collaboration_folder_found
                    || advertised_special_folder
                {
                    "success"
                } else {
                    "not_found"
                };
                tracing::info!(
                    rca_debug = true,
                    adapter = "mapi",
                    mailbox = %principal.email,
                    request_type = "Execute",
                    request_rop_id = "0x02",
                    input_handle_index = request.input_handle_index().unwrap_or(0),
                    response_handle_index = request.output_handle_index.unwrap_or(0),
                    folder_id = format!("0x{folder_id:016x}"),
                    folder_name = post_hierarchy_probe_folder_name(folder_id),
                    role = debug_role_for_folder_id(folder_id),
                    container_class = debug_container_class_for_folder_id(folder_id),
                    mailbox_folder_found = mailbox_folder_found,
                    collaboration_folder_found = collaboration_folder_found,
                    advertised_special_folder = advertised_special_folder,
                    result = open_folder_result,
                    message = "rca debug mapi open folder"
                );
                log_calendar_folder_contract(
                    principal,
                    folder_id,
                    mailbox_folder_found,
                    collaboration_folder_found,
                    advertised_special_folder,
                    snapshot,
                    mailboxes,
                    emails,
                );
                log_special_folder_contract(
                    principal,
                    folder_id,
                    mailbox_folder_found,
                    collaboration_folder_found,
                    advertised_special_folder,
                    snapshot,
                    mailboxes,
                    emails,
                );
                if open_folder_result == "not_found" {
                    responses.extend_from_slice(&rop_error_response(
                        0x02,
                        request.output_handle_index.unwrap_or(0),
                        0x8004_010F,
                    ));
                    continue;
                }
                session.record_opened_folder(folder_id);
                let properties =
                    folder_properties_for_open(store, principal, session, folder_id).await;
                let handle = session.allocate_output_handle(
                    request.output_handle_index,
                    MapiObject::Folder {
                        folder_id,
                        properties,
                    },
                );
                set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                responses.extend_from_slice(&rop_open_folder_response(&request));
                output_handles.push(handle);
            }
            Some(RopId::OpenMessage) => {
                let folder_id = request.folder_id().unwrap_or(INBOX_FOLDER_ID);
                let message_id = request.message_id().unwrap_or(0);
                if let Some(email) = message_for_id(folder_id, message_id, mailboxes, emails) {
                    let handle = session.allocate_output_handle(
                        request.output_handle_index,
                        MapiObject::Message {
                            folder_id,
                            message_id,
                        },
                    );
                    set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                    responses.extend_from_slice(&rop_open_message_response(
                        &request,
                        &email.subject,
                        message_recipients(email).len(),
                    ));
                    output_handles.push(handle);
                } else if let Some(message) =
                    search_folder_message_for_id(snapshot, folder_id, message_id)
                {
                    let handle = session.allocate_output_handle(
                        request.output_handle_index,
                        MapiObject::Message {
                            folder_id,
                            message_id,
                        },
                    );
                    set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                    responses.extend_from_slice(&rop_open_message_response(
                        &request,
                        &message.email.subject,
                        message_recipients(&message.email).len(),
                    ));
                    output_handles.push(handle);
                } else if let Some(contact) = snapshot.contact_for_id(folder_id, message_id) {
                    let handle = session.allocate_output_handle(
                        request.output_handle_index,
                        MapiObject::Contact {
                            folder_id,
                            contact_id: message_id,
                        },
                    );
                    set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                    responses.extend_from_slice(&rop_open_message_response(
                        &request,
                        &contact.contact.name,
                        0,
                    ));
                    output_handles.push(handle);
                } else if let Some(event) = snapshot.event_for_id(folder_id, message_id) {
                    let handle = session.allocate_output_handle(
                        request.output_handle_index,
                        MapiObject::Event {
                            folder_id,
                            event_id: message_id,
                        },
                    );
                    set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                    responses.extend_from_slice(&rop_open_message_response(
                        &request,
                        &event.event.title,
                        0,
                    ));
                    output_handles.push(handle);
                } else if let Some(task) = snapshot.task_for_id(folder_id, message_id) {
                    let handle = session.allocate_output_handle(
                        request.output_handle_index,
                        MapiObject::Task {
                            folder_id,
                            task_id: message_id,
                        },
                    );
                    set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                    responses.extend_from_slice(&rop_open_message_response(
                        &request,
                        &task.task.title,
                        0,
                    ));
                    output_handles.push(handle);
                } else if let Some(note) = snapshot.note_for_id(folder_id, message_id) {
                    let handle = session.allocate_output_handle(
                        request.output_handle_index,
                        MapiObject::Note {
                            folder_id,
                            note_id: message_id,
                        },
                    );
                    set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                    responses.extend_from_slice(&rop_open_message_response(
                        &request,
                        &note.note.title,
                        0,
                    ));
                    output_handles.push(handle);
                } else if let Some(entry) = snapshot.journal_entry_for_id(folder_id, message_id) {
                    let handle = session.allocate_output_handle(
                        request.output_handle_index,
                        MapiObject::JournalEntry {
                            folder_id,
                            journal_entry_id: message_id,
                        },
                    );
                    set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                    responses.extend_from_slice(&rop_open_message_response(
                        &request,
                        &entry.entry.subject,
                        0,
                    ));
                    output_handles.push(handle);
                } else if folder_id == COMMON_VIEWS_FOLDER_ID {
                    if let Some(message) = snapshot.navigation_shortcut_message_for_id(message_id) {
                        let handle = session.allocate_output_handle(
                            request.output_handle_index,
                            MapiObject::NavigationShortcut {
                                folder_id,
                                shortcut_id: message_id,
                            },
                        );
                        set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                        responses.extend_from_slice(&rop_open_message_response(
                            &request,
                            &message.subject,
                            0,
                        ));
                        output_handles.push(handle);
                    } else {
                        responses.extend_from_slice(&rop_error_response(
                            0x03,
                            request.output_handle_index.unwrap_or(0),
                            0x8004_010F,
                        ));
                    }
                } else if folder_id == CONVERSATION_ACTION_SETTINGS_FOLDER_ID {
                    if let Some(message) = snapshot.conversation_action_message_for_id(message_id) {
                        let handle = session.allocate_output_handle(
                            request.output_handle_index,
                            MapiObject::ConversationAction {
                                folder_id,
                                conversation_action_id: message_id,
                            },
                        );
                        set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                        responses.extend_from_slice(&rop_open_message_response(
                            &request,
                            &conversation_action_subject(&message.action),
                            0,
                        ));
                        output_handles.push(handle);
                    } else {
                        responses.extend_from_slice(&rop_error_response(
                            0x03,
                            request.output_handle_index.unwrap_or(0),
                            0x8004_010F,
                        ));
                    }
                } else if folder_id == FREEBUSY_DATA_FOLDER_ID {
                    if let Some(message) = snapshot.delegate_freebusy_message_for_id(message_id) {
                        let handle = session.allocate_output_handle(
                            request.output_handle_index,
                            MapiObject::DelegateFreeBusyMessage {
                                folder_id,
                                message_id,
                            },
                        );
                        set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                        responses.extend_from_slice(&rop_open_message_response(
                            &request,
                            &message.message.subject,
                            0,
                        ));
                        output_handles.push(handle);
                    } else {
                        responses.extend_from_slice(&rop_error_response(
                            0x03,
                            request.output_handle_index.unwrap_or(0),
                            0x8004_010F,
                        ));
                    }
                } else {
                    responses.extend_from_slice(&rop_error_response(
                        0x03,
                        request.output_handle_index.unwrap_or(0),
                        0x8004_010F,
                    ));
                }
            }
            Some(RopId::GetHierarchyTable) => {
                if input_handle(&handle_slots, &request).is_none() {
                    responses.extend_from_slice(&rop_handle_index_error_response(&request));
                    continue;
                }
                let folder_id = input_object(session, &handle_slots, &request)
                    .and_then(|object| object.folder_id())
                    .unwrap_or(ROOT_FOLDER_ID);
                let columns = default_hierarchy_columns();
                let handle = session.allocate_output_handle(
                    request.output_handle_index,
                    MapiObject::HierarchyTable {
                        folder_id,
                        columns,
                        sort_orders: Vec::new(),
                        restriction: None,
                        bookmarks: HashMap::new(),
                        next_bookmark: 1,
                        position: 0,
                    },
                );
                set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                responses.extend_from_slice(&rop_get_hierarchy_table_response(
                    &request,
                    hierarchy_row_count(folder_id, mailboxes, snapshot),
                ));
                output_handles.push(handle);
            }
            Some(RopId::GetContentsTable) => {
                if input_handle(&handle_slots, &request).is_none() {
                    responses.extend_from_slice(&rop_handle_index_error_response(&request));
                    continue;
                }
                let folder_id = input_object(session, &handle_slots, &request)
                    .and_then(|object| object.folder_id())
                    .unwrap_or(INBOX_FOLDER_ID);
                if !snapshot
                    .folder_access_for_principal(folder_id, principal.account_id)
                    .map(|access| access.may_read)
                    .unwrap_or(true)
                {
                    responses.extend_from_slice(&rop_error_response(
                        0x05,
                        request.output_handle_index.unwrap_or(0),
                        0x8007_0005,
                    ));
                    continue;
                }
                let handle = session.allocate_output_handle(
                    request.output_handle_index,
                    MapiObject::ContentsTable {
                        folder_id,
                        associated: request
                            .payload
                            .first()
                            .is_some_and(|flags| flags & 0x02 != 0),
                        columns: Vec::new(),
                        sort_orders: Vec::new(),
                        restriction: None,
                        bookmarks: HashMap::new(),
                        next_bookmark: 1,
                        position: 0,
                    },
                );
                set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                responses.extend_from_slice(&rop_get_contents_table_response(
                    &request,
                    if request
                        .payload
                        .first()
                        .is_some_and(|flags| flags & 0x02 != 0)
                    {
                        associated_folder_message_count(folder_id, snapshot)
                    } else {
                        folder_message_count(folder_id, mailboxes, emails, snapshot)
                    },
                ));
                output_handles.push(handle);
            }
            Some(RopId::CreateMessage) => {
                let folder_id = request.folder_id().unwrap_or_else(|| {
                    input_object(session, &handle_slots, &request)
                        .and_then(MapiObject::folder_id)
                        .unwrap_or(INBOX_FOLDER_ID)
                });
                if !snapshot
                    .folder_access_for_principal(folder_id, principal.account_id)
                    .map(|access| access.may_write)
                    .unwrap_or(true)
                {
                    responses.extend_from_slice(&rop_error_response(
                        0x06,
                        request.output_handle_index.unwrap_or(0),
                        0x8007_0005,
                    ));
                    continue;
                }
                if snapshot.collaboration_folder_for_id(folder_id).is_none()
                    && folder_row_for_id(folder_id, mailboxes).is_none()
                    && !matches!(
                        folder_id,
                        INBOX_FOLDER_ID
                            | DRAFTS_FOLDER_ID
                            | SENT_FOLDER_ID
                            | TRASH_FOLDER_ID
                            | OUTBOX_FOLDER_ID
                            | NOTES_FOLDER_ID
                            | JOURNAL_FOLDER_ID
                            | COMMON_VIEWS_FOLDER_ID
                            | CONVERSATION_ACTION_SETTINGS_FOLDER_ID
                    )
                {
                    responses.extend_from_slice(&rop_error_response(
                        0x06,
                        request.output_handle_index.unwrap_or(0),
                        0x8004_010F,
                    ));
                    continue;
                }

                let pending_object = match snapshot
                    .collaboration_folder_for_id(folder_id)
                    .map(|folder| folder.kind)
                {
                    Some(MapiCollaborationFolderKind::Contacts) => MapiObject::PendingContact {
                        folder_id,
                        properties: HashMap::new(),
                    },
                    Some(MapiCollaborationFolderKind::Calendar) => MapiObject::PendingEvent {
                        folder_id,
                        properties: HashMap::new(),
                    },
                    Some(MapiCollaborationFolderKind::Task) => MapiObject::PendingTask {
                        folder_id,
                        properties: HashMap::new(),
                    },
                    _ if folder_id == NOTES_FOLDER_ID => MapiObject::PendingNote {
                        folder_id,
                        properties: HashMap::new(),
                    },
                    _ if folder_id == JOURNAL_FOLDER_ID => MapiObject::PendingJournalEntry {
                        folder_id,
                        properties: HashMap::new(),
                    },
                    _ if folder_id == CONVERSATION_ACTION_SETTINGS_FOLDER_ID => {
                        MapiObject::PendingConversationAction {
                            folder_id,
                            properties: HashMap::new(),
                        }
                    }
                    _ if folder_id == COMMON_VIEWS_FOLDER_ID => {
                        MapiObject::PendingNavigationShortcut {
                            folder_id,
                            properties: HashMap::new(),
                        }
                    }
                    _ => MapiObject::PendingMessage {
                        folder_id,
                        properties: HashMap::new(),
                        recipients: Vec::new(),
                    },
                };
                let handle =
                    session.allocate_output_handle(request.output_handle_index, pending_object);
                set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                responses.extend_from_slice(&rop_create_message_response(&request));
                output_handles.push(handle);
            }
            Some(RopId::GetPropertiesSpecific) => {
                echo_input_handle_table = true;
                let object = input_object(session, &handle_slots, &request);
                let visible_emails;
                let emails_for_request = if created_emails.is_empty() {
                    emails
                } else {
                    tracing::info!(
                        rca_debug = true,
                        adapter = "mapi",
                        endpoint = "emsmdb",
                        mailbox = %principal.email,
                        request_type = "Execute",
                        request_rop_id = "0x07",
                        object_kind = mapi_object_debug_kind(object),
                        folder_id = %mapi_object_debug_folder_id(object),
                        same_execute_created_email_count = created_emails.len(),
                        base_snapshot_email_count = emails.len(),
                        "rca debug mapi same execute created message visibility"
                    );
                    visible_emails = emails
                        .iter()
                        .chain(created_emails.iter())
                        .cloned()
                        .collect::<Vec<_>>();
                    &visible_emails
                };
                let custom_values = fetch_custom_property_values_for_request(
                    store,
                    principal,
                    object,
                    mailboxes,
                    emails_for_request,
                    snapshot,
                    &request.property_tags(),
                )
                .await
                .unwrap_or_default();
                responses.extend_from_slice(&rop_get_properties_specific_response_with_custom(
                    &request,
                    object,
                    principal,
                    mailboxes,
                    emails_for_request,
                    snapshot,
                    &custom_values,
                ));
            }
            Some(RopId::GetPropertiesAll) => {
                responses.extend_from_slice(&rop_get_properties_all_response(
                    &request,
                    input_object(session, &handle_slots, &request),
                    principal,
                    mailboxes,
                    emails,
                    snapshot,
                ))
            }
            Some(RopId::GetPropertiesList) => {
                responses.extend_from_slice(&rop_get_properties_list_response(
                    &request,
                    input_object(session, &handle_slots, &request),
                ))
            }
            Some(RopId::SetProperties | RopId::SetPropertiesNoReplicate) => {
                echo_input_handle_table = true;
                let set_properties_object = input_object(session, &handle_slots, &request).cloned();
                let set_properties_probe = set_properties_probe_request(&request);
                log_set_properties_specific_debug(
                    principal,
                    &request,
                    set_properties_object.as_ref(),
                    &set_properties_probe,
                );
                let values = match request.property_values() {
                    Ok(values) => values,
                    Err(_) => {
                        responses.extend_from_slice(&rop_error_response(
                            request.rop_id,
                            request.response_handle_index(),
                            0x8004_0102,
                        ));
                        break;
                    }
                };
                let set_result = match set_properties_object {
                    Some(
                        object @ (MapiObject::Message { .. }
                        | MapiObject::Contact { .. }
                        | MapiObject::Event { .. }
                        | MapiObject::Task { .. }
                        | MapiObject::Note { .. }
                        | MapiObject::JournalEntry { .. }
                        | MapiObject::ConversationAction { .. }
                        | MapiObject::NavigationShortcut { .. }
                        | MapiObject::DelegateFreeBusyMessage { .. }
                        | MapiObject::Attachment { .. }),
                    ) => {
                        apply_supported_object_property_values(
                            store, principal, &object, values, mailboxes, emails, snapshot,
                        )
                        .await
                    }
                    object @ Some(MapiObject::Folder { .. }) => {
                        let problems = folder_set_property_problems(object.as_ref(), &values);
                        if !problems.is_empty() {
                            responses.extend_from_slice(&rop_set_properties_problem_response(
                                &request, &problems,
                            ));
                            continue;
                        }
                        let values = default_folder_identification_safe_property_values(
                            principal,
                            object.as_ref(),
                            values,
                        );
                        let result = apply_mapi_property_values(
                            input_object_mut(session, &handle_slots, &request),
                            values.clone(),
                        );
                        if result.is_ok() {
                            if let Some(MapiObject::Folder { folder_id, .. }) = object {
                                if persist_profile_folder_property_values(
                                    store, principal, folder_id, &values,
                                )
                                .await
                                .is_err()
                                {
                                    tracing::warn!(
                                        adapter = "mapi",
                                        endpoint = "emsmdb",
                                        mailbox = %principal.email,
                                        folder_id = format_args!("0x{folder_id:016x}"),
                                        property_tags = %format_debug_property_tags(
                                            &values.iter().map(|(tag, _value)| *tag).collect::<Vec<_>>()
                                        ),
                                        "accepted MAPI folder property write but failed to persist profile state"
                                    );
                                }
                            }
                        }
                        result
                    }
                    _object => apply_mapi_property_values(
                        input_object_mut(session, &handle_slots, &request),
                        values,
                    ),
                };
                match set_result {
                    Ok(()) => responses.extend_from_slice(&rop_set_properties_response(&request)),
                    Err(_) => responses.extend_from_slice(&rop_error_response(
                        request.rop_id,
                        request.response_handle_index(),
                        0x8004_0102,
                    )),
                }
            }
            Some(RopId::DeleteProperties | RopId::DeletePropertiesNoReplicate) => {
                let property_tags = request.property_tags();
                let object = input_object(session, &handle_slots, &request).cloned();
                let delete_result = if let Some(MapiObject::ConversationAction {
                    conversation_action_id,
                    ..
                }) = object
                {
                    delete_conversation_action_properties(
                        store,
                        principal,
                        conversation_action_id,
                        snapshot,
                        &property_tags,
                        mailboxes,
                        emails,
                    )
                    .await
                } else {
                    delete_custom_property_values(
                        store,
                        principal,
                        object.as_ref(),
                        mailboxes,
                        emails,
                        snapshot,
                        &property_tags,
                    )
                    .await
                    .and_then(|_| {
                        delete_mapi_properties(
                            input_object_mut(session, &handle_slots, &request),
                            &property_tags,
                        )
                        .or_else(|error| {
                            if property_tags.iter().all(|tag| is_custom_property_tag(*tag)) {
                                Ok(())
                            } else {
                                Err(error)
                            }
                        })
                    })
                };
                match delete_result {
                    Ok(()) => {
                        responses.extend_from_slice(&rop_delete_properties_response(&request))
                    }
                    Err(_) => responses.extend_from_slice(&rop_error_response(
                        request.rop_id,
                        request.response_handle_index(),
                        0x8004_0102,
                    )),
                }
            }
            Some(RopId::SaveChangesMessage) => {
                let Some(handle) = input_handle(&handle_slots, &request) else {
                    responses.extend_from_slice(&rop_error_response(
                        0x0C,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                match session.handles.get(&handle).cloned() {
                    Some(MapiObject::PendingContact {
                        folder_id,
                        properties,
                    }) => {
                        let Some(folder) = snapshot.collaboration_folder_for_id(folder_id) else {
                            responses.extend_from_slice(&rop_error_response(
                                0x0C,
                                request.response_handle_index(),
                                0x8004_010F,
                            ));
                            continue;
                        };
                        let input = contact_input_from_mapi(
                            principal.account_id,
                            None,
                            &default_contact_for_mapping(
                                principal.account_id,
                                &folder.collection.id,
                            ),
                            &properties,
                        );
                        match store
                            .create_accessible_contact(
                                principal.account_id,
                                Some(&folder.collection.id),
                                input,
                            )
                            .await
                        {
                            Ok(contact) => {
                                let contact_id = match remember_created_mapi_identity(
                                    store,
                                    principal,
                                    MapiIdentityObjectKind::Contact,
                                    contact.id,
                                    None,
                                )
                                .await
                                {
                                    Ok(contact_id) => contact_id,
                                    Err(_) => {
                                        responses.extend_from_slice(&rop_error_response(
                                            0x0C,
                                            request.response_handle_index(),
                                            0x8004_010F,
                                        ));
                                        continue;
                                    }
                                };
                                session.handles.insert(
                                    handle,
                                    MapiObject::Contact {
                                        folder_id,
                                        contact_id,
                                    },
                                );
                                session.record_notification(MapiNotificationEvent::content(
                                    folder_id,
                                    Some(contact_id),
                                ));
                                responses.extend_from_slice(&rop_save_changes_message_response(
                                    &request, contact_id,
                                ));
                            }
                            Err(_) => responses.extend_from_slice(&rop_error_response(
                                0x0C,
                                request.response_handle_index(),
                                0x8004_010F,
                            )),
                        }
                        continue;
                    }
                    Some(MapiObject::PendingEvent {
                        folder_id,
                        properties,
                    }) => {
                        let Some(folder) = snapshot.collaboration_folder_for_id(folder_id) else {
                            responses.extend_from_slice(&rop_error_response(
                                0x0C,
                                request.response_handle_index(),
                                0x8004_010F,
                            ));
                            continue;
                        };
                        let input = match event_input_from_mapi(
                            principal.account_id,
                            None,
                            &default_event_for_mapping(principal.account_id, &folder.collection.id),
                            &properties,
                        ) {
                            Ok(input) => input,
                            Err(_) => {
                                responses.extend_from_slice(&rop_error_response(
                                    0x0C,
                                    request.response_handle_index(),
                                    0x8004_0102,
                                ));
                                continue;
                            }
                        };
                        match store
                            .create_accessible_event(
                                principal.account_id,
                                Some(&folder.collection.id),
                                input,
                            )
                            .await
                        {
                            Ok(event) => {
                                let event_id = match remember_created_mapi_identity(
                                    store,
                                    principal,
                                    MapiIdentityObjectKind::CalendarEvent,
                                    event.id,
                                    None,
                                )
                                .await
                                {
                                    Ok(event_id) => event_id,
                                    Err(_) => {
                                        responses.extend_from_slice(&rop_error_response(
                                            0x0C,
                                            request.response_handle_index(),
                                            0x8004_010F,
                                        ));
                                        continue;
                                    }
                                };
                                session.handles.insert(
                                    handle,
                                    MapiObject::Event {
                                        folder_id,
                                        event_id,
                                    },
                                );
                                session.record_notification(MapiNotificationEvent::content(
                                    folder_id,
                                    Some(event_id),
                                ));
                                responses.extend_from_slice(&rop_save_changes_message_response(
                                    &request, event_id,
                                ));
                            }
                            Err(_) => responses.extend_from_slice(&rop_error_response(
                                0x0C,
                                request.response_handle_index(),
                                0x8004_010F,
                            )),
                        }
                        continue;
                    }
                    Some(MapiObject::PendingTask {
                        folder_id,
                        properties,
                    }) => {
                        let Some(folder) = snapshot.collaboration_folder_for_id(folder_id) else {
                            responses.extend_from_slice(&rop_error_response(
                                0x0C,
                                request.response_handle_index(),
                                0x8004_010F,
                            ));
                            continue;
                        };
                        let input = task_input_from_mapi(
                            principal.account_id,
                            None,
                            &default_task_for_mapping(principal.account_id, &folder.collection.id),
                            Some(&folder.collection.id),
                            &properties,
                        );
                        match store
                            .create_accessible_task(principal.account_id, input)
                            .await
                        {
                            Ok(task) => {
                                let task_id = match remember_created_mapi_identity(
                                    store,
                                    principal,
                                    MapiIdentityObjectKind::Task,
                                    task.id,
                                    None,
                                )
                                .await
                                {
                                    Ok(task_id) => task_id,
                                    Err(_) => {
                                        responses.extend_from_slice(&rop_error_response(
                                            0x0C,
                                            request.response_handle_index(),
                                            0x8004_010F,
                                        ));
                                        continue;
                                    }
                                };
                                session
                                    .handles
                                    .insert(handle, MapiObject::Task { folder_id, task_id });
                                session.record_notification(MapiNotificationEvent::content(
                                    folder_id,
                                    Some(task_id),
                                ));
                                responses.extend_from_slice(&rop_save_changes_message_response(
                                    &request, task_id,
                                ));
                            }
                            Err(_) => responses.extend_from_slice(&rop_error_response(
                                0x0C,
                                request.response_handle_index(),
                                0x8004_010F,
                            )),
                        }
                        continue;
                    }
                    Some(MapiObject::PendingNote {
                        folder_id,
                        properties,
                    }) => {
                        let input = note_input_from_mapi(
                            principal.account_id,
                            None,
                            &default_note_for_mapping(),
                            &properties,
                        );
                        match store.upsert_mapi_note(input).await {
                            Ok(note) => {
                                let note_id = match remember_created_mapi_identity(
                                    store,
                                    principal,
                                    MapiIdentityObjectKind::Note,
                                    note.id,
                                    None,
                                )
                                .await
                                {
                                    Ok(note_id) => note_id,
                                    Err(_) => {
                                        responses.extend_from_slice(&rop_error_response(
                                            0x0C,
                                            request.response_handle_index(),
                                            0x8004_010F,
                                        ));
                                        continue;
                                    }
                                };
                                session
                                    .handles
                                    .insert(handle, MapiObject::Note { folder_id, note_id });
                                session.record_notification(MapiNotificationEvent::content(
                                    folder_id,
                                    Some(note_id),
                                ));
                                responses.extend_from_slice(&rop_save_changes_message_response(
                                    &request, note_id,
                                ));
                            }
                            Err(_) => responses.extend_from_slice(&rop_error_response(
                                0x0C,
                                request.response_handle_index(),
                                0x8004_010F,
                            )),
                        }
                        continue;
                    }
                    Some(MapiObject::PendingJournalEntry {
                        folder_id,
                        properties,
                    }) => {
                        let input = journal_entry_input_from_mapi(
                            principal.account_id,
                            None,
                            &default_journal_entry_for_mapping(),
                            &properties,
                        );
                        match store.upsert_mapi_journal_entry(input).await {
                            Ok(entry) => {
                                let journal_entry_id = match remember_created_mapi_identity(
                                    store,
                                    principal,
                                    MapiIdentityObjectKind::JournalEntry,
                                    entry.id,
                                    None,
                                )
                                .await
                                {
                                    Ok(journal_entry_id) => journal_entry_id,
                                    Err(_) => {
                                        responses.extend_from_slice(&rop_error_response(
                                            0x0C,
                                            request.response_handle_index(),
                                            0x8004_010F,
                                        ));
                                        continue;
                                    }
                                };
                                session.handles.insert(
                                    handle,
                                    MapiObject::JournalEntry {
                                        folder_id,
                                        journal_entry_id,
                                    },
                                );
                                session.record_notification(MapiNotificationEvent::content(
                                    folder_id,
                                    Some(journal_entry_id),
                                ));
                                responses.extend_from_slice(&rop_save_changes_message_response(
                                    &request,
                                    journal_entry_id,
                                ));
                            }
                            Err(_) => responses.extend_from_slice(&rop_error_response(
                                0x0C,
                                request.response_handle_index(),
                                0x8004_010F,
                            )),
                        }
                        continue;
                    }
                    Some(MapiObject::PendingConversationAction {
                        folder_id,
                        properties,
                    }) => {
                        let action = conversation_action_from_mapi_properties(&properties);
                        if action.conversation_id.is_nil() {
                            responses.extend_from_slice(&rop_error_response(
                                0x0C,
                                request.response_handle_index(),
                                0x8004_0102,
                            ));
                            continue;
                        }
                        let move_target_mailbox_id =
                            conversation_action_target_mailbox_id(&action, mailboxes);
                        let input = lpe_storage::UpsertConversationActionInput {
                            account_id: principal.account_id,
                            conversation_id: action.conversation_id,
                            subject: action.subject,
                            categories_json: action.categories_json,
                            move_folder_entry_id: action.move_folder_entry_id,
                            move_store_entry_id: action.move_store_entry_id,
                            move_target_mailbox_id,
                            max_delivery_time: action.max_delivery_time,
                            last_applied_time: action.last_applied_time,
                            version: Some(action.version),
                            processed: Some(action.processed),
                        };
                        match store.upsert_conversation_action(input).await {
                            Ok(saved) => {
                                let conversation_action_id = match remember_created_mapi_identity(
                                    store,
                                    principal,
                                    MapiIdentityObjectKind::ConversationAction,
                                    saved.id,
                                    None,
                                )
                                .await
                                {
                                    Ok(conversation_action_id) => conversation_action_id,
                                    Err(_) => {
                                        responses.extend_from_slice(&rop_error_response(
                                            0x0C,
                                            request.response_handle_index(),
                                            0x8004_010F,
                                        ));
                                        continue;
                                    }
                                };
                                if apply_conversation_action_to_existing_messages(
                                    store, principal, &saved, mailboxes, emails,
                                )
                                .await
                                .is_err()
                                {
                                    responses.extend_from_slice(&rop_error_response(
                                        0x0C,
                                        request.response_handle_index(),
                                        0x8004_010F,
                                    ));
                                    continue;
                                }
                                session.handles.insert(
                                    handle,
                                    MapiObject::ConversationAction {
                                        folder_id,
                                        conversation_action_id,
                                    },
                                );
                                session.record_notification(MapiNotificationEvent::content(
                                    folder_id,
                                    Some(conversation_action_id),
                                ));
                                responses.extend_from_slice(&rop_save_changes_message_response(
                                    &request,
                                    conversation_action_id,
                                ));
                            }
                            Err(_) => responses.extend_from_slice(&rop_error_response(
                                0x0C,
                                request.response_handle_index(),
                                0x8004_010F,
                            )),
                        }
                        continue;
                    }
                    Some(MapiObject::PendingNavigationShortcut {
                        folder_id,
                        properties,
                    }) => {
                        let shortcut = navigation_shortcut_from_mapi_properties(
                            principal.account_id,
                            None,
                            &properties,
                        );
                        let input = UpsertMapiNavigationShortcutInput {
                            id: None,
                            account_id: principal.account_id,
                            subject: shortcut.subject,
                            target_folder_id: shortcut.target_folder_id,
                            shortcut_type: shortcut.shortcut_type,
                            flags: shortcut.flags,
                            section: shortcut.section,
                            ordinal: shortcut.ordinal,
                        };
                        match store.upsert_mapi_navigation_shortcut(input).await {
                            Ok(saved) => {
                                let shortcut_id = match remember_created_mapi_identity(
                                    store,
                                    principal,
                                    MapiIdentityObjectKind::NavigationShortcut,
                                    saved.id,
                                    None,
                                )
                                .await
                                {
                                    Ok(shortcut_id) => shortcut_id,
                                    Err(_) => {
                                        responses.extend_from_slice(&rop_error_response(
                                            0x0C,
                                            request.response_handle_index(),
                                            0x8004_010F,
                                        ));
                                        continue;
                                    }
                                };
                                session.handles.insert(
                                    handle,
                                    MapiObject::NavigationShortcut {
                                        folder_id,
                                        shortcut_id,
                                    },
                                );
                                session.record_notification(MapiNotificationEvent::content(
                                    folder_id,
                                    Some(shortcut_id),
                                ));
                                responses.extend_from_slice(&rop_save_changes_message_response(
                                    &request,
                                    shortcut_id,
                                ));
                            }
                            Err(_) => responses.extend_from_slice(&rop_error_response(
                                0x0C,
                                request.response_handle_index(),
                                0x8004_010F,
                            )),
                        }
                        continue;
                    }
                    Some(MapiObject::Contact { contact_id, .. })
                    | Some(MapiObject::Event {
                        event_id: contact_id,
                        ..
                    })
                    | Some(MapiObject::Task {
                        task_id: contact_id,
                        ..
                    })
                    | Some(MapiObject::Note {
                        note_id: contact_id,
                        ..
                    })
                    | Some(MapiObject::JournalEntry {
                        journal_entry_id: contact_id,
                        ..
                    })
                    | Some(MapiObject::ConversationAction {
                        conversation_action_id: contact_id,
                        ..
                    })
                    | Some(MapiObject::NavigationShortcut {
                        shortcut_id: contact_id,
                        ..
                    })
                    | Some(MapiObject::DelegateFreeBusyMessage {
                        message_id: contact_id,
                        ..
                    }) => {
                        responses.extend_from_slice(&rop_save_changes_message_response(
                            &request, contact_id,
                        ));
                        continue;
                    }
                    _ => {}
                }
                let Some(MapiObject::PendingMessage {
                    folder_id,
                    properties,
                    recipients,
                }) = session.handles.get(&handle).cloned()
                else {
                    responses.extend_from_slice(&rop_error_response(
                        0x0C,
                        request.response_handle_index(),
                        0x8004_0102,
                    ));
                    continue;
                };
                let Some(mailbox) = folder_row_for_id(folder_id, mailboxes) else {
                    responses.extend_from_slice(&rop_error_response(
                        0x0C,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                if pending_message_is_sync_metadata_only(&properties, &recipients) {
                    let response_message_id = pending_source_key_message_id(&properties);
                    tracing::info!(
                        rca_debug = true,
                        adapter = "mapi",
                        endpoint = "emsmdb",
                        mailbox = %principal.email,
                        request_type = "Execute",
                        request_rop_id = "0x0c",
                        input_handle_index = request.input_handle_index.unwrap_or(0),
                        response_handle_index = request.response_handle_index(),
                        object_kind = "pending_message",
                        folder_id = %format!("{folder_id:#018x}"),
                        folder_role = role_for_folder_id(folder_id).unwrap_or(""),
                        property_tag_count = properties.len(),
                        property_tags = %format_debug_property_tags(
                            &properties.keys().copied().collect::<Vec<_>>()
                        ),
                        save_skipped_reason = "sync_metadata_only",
                        "rca debug mapi save changes message"
                    );
                    responses.extend_from_slice(&rop_save_changes_message_response(
                        &request,
                        response_message_id,
                    ));
                    continue;
                }
                if pending_message_is_unbacked_trash_sync_upload(folder_id, &properties) {
                    let response_message_id = pending_source_key_message_id(&properties);
                    tracing::info!(
                        rca_debug = true,
                        adapter = "mapi",
                        endpoint = "emsmdb",
                        mailbox = %principal.email,
                        request_type = "Execute",
                        request_rop_id = "0x0c",
                        input_handle_index = request.input_handle_index.unwrap_or(0),
                        response_handle_index = request.response_handle_index(),
                        object_kind = "pending_message",
                        folder_id = %format!("{folder_id:#018x}"),
                        folder_role = role_for_folder_id(folder_id).unwrap_or(""),
                        property_tag_count = properties.len(),
                        property_tags = %format_debug_property_tags(
                            &properties.keys().copied().collect::<Vec<_>>()
                        ),
                        import_source_key_global_counter = pending_source_key_global_counter(&properties)
                            .map(|counter| counter.to_string())
                            .unwrap_or_default(),
                        response_message_id = %format!("{response_message_id:#018x}"),
                        save_skipped_reason = "unbacked_client_trash_sync_upload",
                        "rca debug mapi save changes message"
                    );
                    responses.extend_from_slice(&rop_save_changes_message_response(
                        &request,
                        response_message_id,
                    ));
                    continue;
                }
                let input =
                    jmap_import_from_pending_message(principal, mailbox, &properties, &recipients);
                let reserved_global_counter =
                    imported_source_key_reserved_global_counter(&properties);
                match store
                    .import_jmap_email(
                        input,
                        AuditEntryInput {
                            actor: principal.email.clone(),
                            action: "mapi-save-message".to_string(),
                            subject: format!("folder:{}", mailbox.id),
                        },
                    )
                    .await
                {
                    Ok(email) => {
                        if apply_conversation_actions_to_new_message(
                            store, principal, mailboxes, &email, snapshot,
                        )
                        .await
                        .is_err()
                        {
                            responses.extend_from_slice(&rop_error_response(
                                0x0C,
                                request.response_handle_index(),
                                0x8004_010F,
                            ));
                            continue;
                        }
                        let (message_id, preserved_import_source_key, identity_fallback_reason) =
                            match remember_created_message_mapi_identity(
                                store,
                                principal,
                                email.id,
                                reserved_global_counter,
                            )
                            .await
                            {
                                Ok(result) => result,
                                Err(error) => {
                                    tracing::info!(
                                        rca_debug = true,
                                        adapter = "mapi",
                                        endpoint = "emsmdb",
                                        mailbox = %principal.email,
                                        request_type = "Execute",
                                        request_rop_id = "0x0c",
                                        input_handle_index = request.input_handle_index.unwrap_or(0),
                                        response_handle_index = request.response_handle_index(),
                                        object_kind = "message",
                                        folder_id = %format!("{folder_id:#018x}"),
                                        folder_role = role_for_folder_id(folder_id).unwrap_or(""),
                                        reserved_global_counter = reserved_global_counter
                                            .map(|counter| counter.to_string())
                                            .unwrap_or_default(),
                                        identity_error = %error,
                                        "rca debug mapi save changes message identity"
                                    );
                                    responses.extend_from_slice(&rop_error_response(
                                        0x0C,
                                        request.response_handle_index(),
                                        0x8004_010F,
                                    ));
                                    continue;
                                }
                            };
                        session.handles.insert(
                            handle,
                            MapiObject::Message {
                                folder_id,
                                message_id,
                            },
                        );
                        created_emails.push(email);
                        session.record_notification(MapiNotificationEvent::content(
                            folder_id,
                            Some(message_id),
                        ));
                        tracing::info!(
                            rca_debug = true,
                            adapter = "mapi",
                            endpoint = "emsmdb",
                            mailbox = %principal.email,
                            request_type = "Execute",
                            request_rop_id = "0x0c",
                            input_handle_index = request.input_handle_index.unwrap_or(0),
                            response_handle_index = request.response_handle_index(),
                            object_kind = "message",
                            folder_id = %format!("{folder_id:#018x}"),
                            folder_role = role_for_folder_id(folder_id).unwrap_or(""),
                            item_id = %format!("{message_id:#018x}"),
                            reserved_global_counter = reserved_global_counter
                                .map(|counter| counter.to_string())
                                .unwrap_or_default(),
                            preserved_import_source_key,
                            identity_fallback_reason = %identity_fallback_reason,
                            "rca debug mapi save changes message"
                        );
                        responses.extend_from_slice(&rop_save_changes_message_response(
                            &request, message_id,
                        ));
                    }
                    Err(error) => {
                        tracing::info!(
                            rca_debug = true,
                            adapter = "mapi",
                            endpoint = "emsmdb",
                            mailbox = %principal.email,
                            request_type = "Execute",
                            request_rop_id = "0x0c",
                            input_handle_index = request.input_handle_index.unwrap_or(0),
                            response_handle_index = request.response_handle_index(),
                            object_kind = "pending_message",
                            folder_id = %format!("{folder_id:#018x}"),
                            folder_role = role_for_folder_id(folder_id).unwrap_or(""),
                            recipient_count = recipients.len(),
                            save_error = %error,
                            "rca debug mapi save changes message"
                        );
                        responses.extend_from_slice(&rop_error_response(
                            0x0C,
                            request.response_handle_index(),
                            0x8004_010F,
                        ))
                    }
                }
            }
            Some(RopId::RemoveAllRecipients) => {
                match input_object_mut(session, &handle_slots, &request) {
                    Some(MapiObject::PendingMessage { recipients, .. }) => {
                        recipients.clear();
                        responses.extend_from_slice(&rop_simple_success_response(&request));
                    }
                    _ => responses.extend_from_slice(&rop_error_response(
                        0x0D,
                        request.response_handle_index(),
                        0x8004_0102,
                    )),
                }
            }
            Some(RopId::ModifyRecipients) => {
                match input_object_mut(session, &handle_slots, &request) {
                    Some(MapiObject::PendingMessage { recipients, .. }) => {
                        let address_book_entries = store
                            .fetch_address_book_entries(principal)
                            .await
                            .unwrap_or_default();
                        match request.modify_recipients(principal, &address_book_entries) {
                            Ok(changes) => {
                                tracing::info!(
                                    rca_debug = true,
                                    adapter = "mapi",
                                    endpoint = "emsmdb",
                                    mailbox = %principal.email,
                                    request_type = "Execute",
                                    request_rop_id = "0x0e",
                                    input_handle_index = request.input_handle_index.unwrap_or(0),
                                    response_handle_index = request.response_handle_index(),
                                    existing_recipient_count = recipients.len(),
                                    recipient_change_count = changes.len(),
                                    recipient_upsert_count = pending_recipient_upsert_count(&changes),
                                    recipient_delete_count = pending_recipient_delete_count(&changes),
                                    recipient_types = %pending_recipient_types_summary(&changes),
                                    recipient_row_ids = %pending_recipient_row_ids_summary(&changes),
                                    parse_error = "",
                                    "rca debug mapi modify recipients"
                                );
                                apply_pending_recipient_changes(recipients, changes);
                                responses.extend_from_slice(&rop_simple_success_response(&request));
                            }
                            Err(error) => {
                                tracing::info!(
                                    rca_debug = true,
                                    adapter = "mapi",
                                    endpoint = "emsmdb",
                                    mailbox = %principal.email,
                                    request_type = "Execute",
                                    request_rop_id = "0x0e",
                                    input_handle_index = request.input_handle_index.unwrap_or(0),
                                    response_handle_index = request.response_handle_index(),
                                    existing_recipient_count = recipients.len(),
                                    recipient_payload_bytes = request.payload.len(),
                                    recipient_payload_preview = %hex_preview(&request.payload, 48),
                                    parse_error = %error,
                                    "rca debug mapi modify recipients"
                                );
                                responses.extend_from_slice(&rop_error_response(
                                    0x0E,
                                    request.response_handle_index(),
                                    0x8004_0102,
                                ));
                            }
                        }
                    }
                    _ => responses.extend_from_slice(&rop_error_response(
                        0x0E,
                        request.response_handle_index(),
                        0x8004_0102,
                    )),
                }
            }
            Some(RopId::ReadRecipients) => {
                responses.extend_from_slice(&rop_read_recipients_response(
                    &request,
                    input_object(session, &handle_slots, &request),
                    mailboxes,
                    emails,
                    snapshot,
                ))
            }
            Some(RopId::ReloadCachedInformation) => {
                responses.extend_from_slice(&rop_reload_cached_information_response(
                    &request,
                    input_object(session, &handle_slots, &request),
                    mailboxes,
                    emails,
                    snapshot,
                ))
            }
            Some(RopId::SetMessageReadFlag) => {
                let Some(MapiObject::Message {
                    folder_id,
                    message_id,
                }) = input_object(session, &handle_slots, &request)
                else {
                    responses.extend_from_slice(&rop_error_response(
                        0x11,
                        request.response_handle_index(),
                        0x0000_04B9,
                    ));
                    continue;
                };
                let Some(email) = message_for_id(*folder_id, *message_id, mailboxes, emails) else {
                    responses.extend_from_slice(&rop_error_response(
                        0x11,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                let unread = unread_from_read_flags(request.read_flags());
                let changed = unread.is_some_and(|unread| unread != email.unread);
                if let Some(unread) = unread {
                    if !snapshot
                        .folder_access_for_principal(*folder_id, principal.account_id)
                        .map(|access| access.may_write)
                        .unwrap_or(true)
                    {
                        responses.extend_from_slice(&rop_error_response(
                            0x11,
                            request.response_handle_index(),
                            0x8007_0005,
                        ));
                        continue;
                    }
                    if store
                        .update_jmap_email_flags(
                            principal.account_id,
                            email.id,
                            Some(unread),
                            None,
                            AuditEntryInput {
                                actor: principal.email.clone(),
                                action: "mapi-set-message-read-flag".to_string(),
                                subject: format!("message:{}", email.id),
                            },
                        )
                        .await
                        .is_err()
                    {
                        responses.extend_from_slice(&rop_error_response(
                            0x11,
                            request.response_handle_index(),
                            0x8004_010F,
                        ));
                        continue;
                    }
                }
                if changed {
                    session.record_notification(MapiNotificationEvent::content(*folder_id, None));
                }
                responses.extend_from_slice(&rop_set_message_read_flag_response(&request, changed));
            }
            Some(RopId::SetColumns) => match input_object_mut(session, &handle_slots, &request) {
                Some(MapiObject::HierarchyTable { columns, .. })
                | Some(MapiObject::ContentsTable { columns, .. })
                | Some(MapiObject::AttachmentTable { columns, .. })
                | Some(MapiObject::PermissionTable { columns, .. }) => {
                    if !property_tags_are_supported(&request.property_tags()) {
                        responses.extend_from_slice(&rop_error_response(
                            0x12,
                            request.response_handle_index(),
                            0x8004_0102,
                        ));
                        break;
                    }
                    *columns = request.property_tags();
                    responses.extend_from_slice(&rop_set_columns_response(&request));
                }
                _ => responses.extend_from_slice(&rop_error_response(
                    0x12,
                    request.response_handle_index(),
                    0x8004_0102,
                )),
            },
            Some(RopId::SortTable) => match input_object_mut(session, &handle_slots, &request) {
                Some(MapiObject::HierarchyTable {
                    sort_orders,
                    position,
                    bookmarks,
                    ..
                })
                | Some(MapiObject::ContentsTable {
                    sort_orders,
                    position,
                    bookmarks,
                    ..
                })
                | Some(MapiObject::AttachmentTable {
                    sort_orders,
                    position,
                    bookmarks,
                    ..
                }) => {
                    *sort_orders = request.sort_orders();
                    *position = 0;
                    bookmarks.clear();
                    responses.extend_from_slice(&rop_sort_table_response(&request));
                }
                _ => responses.extend_from_slice(&rop_error_response(
                    0x13,
                    request.response_handle_index(),
                    0x8004_0102,
                )),
            },
            Some(RopId::Restrict) => match input_object_mut(session, &handle_slots, &request) {
                Some(MapiObject::HierarchyTable {
                    restriction,
                    position,
                    bookmarks,
                    ..
                })
                | Some(MapiObject::ContentsTable {
                    restriction,
                    position,
                    bookmarks,
                    ..
                })
                | Some(MapiObject::AttachmentTable {
                    restriction,
                    position,
                    bookmarks,
                    ..
                }) => match request.restriction() {
                    Ok(parsed) => {
                        *restriction = parsed;
                        *position = 0;
                        bookmarks.clear();
                        responses.extend_from_slice(&rop_restrict_response(&request));
                    }
                    Err(_) => {
                        responses.extend_from_slice(&rop_error_response(
                            0x14,
                            request.response_handle_index(),
                            0x8004_0102,
                        ));
                        break;
                    }
                },
                _ => responses.extend_from_slice(&rop_error_response(
                    0x14,
                    request.response_handle_index(),
                    0x8004_0102,
                )),
            },
            Some(RopId::QueryRows) => responses.extend_from_slice(&rop_query_rows_response(
                &request,
                input_object_mut(session, &handle_slots, &request),
                mailboxes,
                emails,
                snapshot,
            )),
            Some(RopId::GetStatus) => responses.extend_from_slice(&rop_get_status_response(
                &request,
                input_object(session, &handle_slots, &request),
            )),
            Some(RopId::QueryPosition) => {
                responses.extend_from_slice(&rop_query_position_response(
                    &request,
                    input_object(session, &handle_slots, &request),
                    mailboxes,
                    emails,
                    snapshot,
                ))
            }
            Some(RopId::SeekRow) => responses.extend_from_slice(&rop_seek_row_response(
                &request,
                input_object_mut(session, &handle_slots, &request),
                mailboxes,
                emails,
                snapshot,
            )),
            Some(RopId::SeekRowBookmark) => {
                responses.extend_from_slice(&rop_seek_row_bookmark_response(
                    &request,
                    input_object_mut(session, &handle_slots, &request),
                    mailboxes,
                    emails,
                    snapshot,
                ))
            }
            Some(RopId::SeekRowFractional) => {
                responses.extend_from_slice(&rop_seek_row_fractional_response(
                    &request,
                    input_object_mut(session, &handle_slots, &request),
                    mailboxes,
                    emails,
                    snapshot,
                ))
            }
            Some(RopId::CreateBookmark) => {
                responses.extend_from_slice(&rop_create_bookmark_response(
                    &request,
                    input_object_mut(session, &handle_slots, &request),
                    mailboxes,
                    emails,
                    snapshot,
                ))
            }
            Some(RopId::QueryColumnsAll) => {
                responses.extend_from_slice(&rop_query_columns_all_response(
                    &request,
                    input_object(session, &handle_slots, &request),
                    snapshot,
                ))
            }
            Some(RopId::CreateFolder) => {
                let parent_folder_id = match input_object(session, &handle_slots, &request)
                    .and_then(MapiObject::folder_id)
                {
                    Some(folder_id) => folder_id,
                    None => {
                        responses.extend_from_slice(&rop_error_response(
                            0x1C,
                            request.output_handle_index.unwrap_or(0),
                            0x0000_04B9,
                        ));
                        continue;
                    }
                };
                if !is_root_hierarchy_folder(parent_folder_id)
                    && folder_row_for_id(parent_folder_id, mailboxes).is_none()
                    && role_for_folder_id(parent_folder_id).is_none()
                {
                    responses.extend_from_slice(&rop_error_response(
                        0x1C,
                        request.output_handle_index.unwrap_or(0),
                        0x8004_010F,
                    ));
                    continue;
                }

                let display_name = request.create_folder_display_name();
                let display_name = display_name.trim();
                if display_name.is_empty() || request.create_folder_type() == 0 {
                    responses.extend_from_slice(&rop_error_response(
                        0x1C,
                        request.output_handle_index.unwrap_or(0),
                        0x8004_0102,
                    ));
                    continue;
                }

                if request.create_folder_open_existing() {
                    if let Some(existing) = mailboxes
                        .iter()
                        .find(|mailbox| mailbox.name.eq_ignore_ascii_case(display_name))
                    {
                        let folder_id = mapi_folder_id(existing);
                        let properties =
                            folder_properties_for_open(store, principal, session, folder_id).await;
                        let handle = session.allocate_output_handle(
                            request.output_handle_index,
                            MapiObject::Folder {
                                folder_id,
                                properties,
                            },
                        );
                        set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                        responses.extend_from_slice(&rop_create_folder_response(
                            &request, folder_id, true,
                        ));
                        output_handles.push(handle);
                        continue;
                    }
                }

                match store
                    .create_jmap_mailbox(
                        JmapMailboxCreateInput {
                            account_id: principal.account_id,
                            name: display_name.to_string(),
                            parent_id: None,
                            sort_order: None,
                            is_subscribed: true,
                        },
                        AuditEntryInput {
                            actor: principal.email.clone(),
                            action: "mapi-create-folder".to_string(),
                            subject: display_name.to_string(),
                        },
                    )
                    .await
                {
                    Ok(mailbox) => {
                        let folder_id = match remember_created_mapi_identity(
                            store,
                            principal,
                            MapiIdentityObjectKind::Mailbox,
                            mailbox.id,
                            None,
                        )
                        .await
                        {
                            Ok(folder_id) => folder_id,
                            Err(_) => {
                                responses.extend_from_slice(&rop_error_response(
                                    0x1C,
                                    request.output_handle_index.unwrap_or(0),
                                    0x8004_0102,
                                ));
                                continue;
                            }
                        };
                        let handle = session.allocate_output_handle(
                            request.output_handle_index,
                            MapiObject::Folder {
                                folder_id,
                                properties: HashMap::new(),
                            },
                        );
                        set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                        responses.extend_from_slice(&rop_create_folder_response(
                            &request, folder_id, false,
                        ));
                        session.record_notification(MapiNotificationEvent::hierarchy(
                            parent_folder_id,
                            Some(folder_id),
                        ));
                        output_handles.push(handle);
                    }
                    Err(_) => responses.extend_from_slice(&rop_error_response(
                        0x1C,
                        request.output_handle_index.unwrap_or(0),
                        0x8004_0102,
                    )),
                }
            }
            Some(RopId::DeleteFolder) => {
                let Some(_parent_folder_id) =
                    input_object(session, &handle_slots, &request).and_then(MapiObject::folder_id)
                else {
                    responses.extend_from_slice(&rop_error_response(
                        0x1D,
                        request.response_handle_index(),
                        0x0000_04B9,
                    ));
                    continue;
                };
                let Some(folder_id) = request.delete_folder_id() else {
                    responses.extend_from_slice(&rop_error_response(
                        0x1D,
                        request.response_handle_index(),
                        0x8004_0102,
                    ));
                    continue;
                };
                let Some(mailbox) = folder_row_for_id(folder_id, mailboxes) else {
                    responses.extend_from_slice(&rop_error_response(
                        0x1D,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                if mailbox.role != "custom" {
                    responses.extend_from_slice(&rop_error_response(
                        0x1D,
                        request.response_handle_index(),
                        0x8007_0005,
                    ));
                    continue;
                }

                let partial_completion = store
                    .destroy_jmap_mailbox(
                        principal.account_id,
                        mailbox.id,
                        AuditEntryInput {
                            actor: principal.email.clone(),
                            action: "mapi-delete-folder".to_string(),
                            subject: format!("folder:{}", mailbox.id),
                        },
                    )
                    .await
                    .is_err();
                if !partial_completion {
                    session.record_notification(MapiNotificationEvent::hierarchy(
                        _parent_folder_id,
                        Some(folder_id),
                    ));
                }
                responses.extend_from_slice(&rop_partial_completion_response(
                    0x1D,
                    request.response_handle_index(),
                    partial_completion,
                ));
            }
            Some(RopId::DeleteMessages | RopId::HardDeleteMessages) | Some(RopId::ExpandRow)
                if request.rop_id != RopId::ExpandRow.as_u8()
                    || !request.message_ids().is_empty() =>
            {
                let folder_id = match input_object(session, &handle_slots, &request) {
                    Some(MapiObject::Folder { folder_id, .. }) => *folder_id,
                    _ if request.rop_id == RopId::ExpandRow.as_u8()
                        || request.rop_id == RopId::HardDeleteMessages.as_u8() =>
                    {
                        responses.extend_from_slice(&unsupported_rop_response(
                            request.rop_id,
                            request.response_handle_index(),
                        ));
                        continue;
                    }
                    _ => {
                        responses.extend_from_slice(&rop_error_response(
                            request.rop_id,
                            request.response_handle_index(),
                            0x0000_04B9,
                        ));
                        continue;
                    }
                };
                let mut partial_completion = false;
                if !snapshot
                    .folder_access_for_principal(folder_id, principal.account_id)
                    .map(|access| access.may_delete)
                    .unwrap_or(true)
                {
                    responses.extend_from_slice(&rop_error_response(
                        request.rop_id,
                        request.response_handle_index(),
                        0x8007_0005,
                    ));
                    continue;
                }
                for message_id in request.message_ids() {
                    if let Some(contact) = snapshot.contact_for_id(folder_id, message_id) {
                        if store
                            .delete_accessible_contact(principal.account_id, contact.canonical_id)
                            .await
                            .is_err()
                        {
                            partial_completion = true;
                        }
                        continue;
                    }
                    if let Some(event) = snapshot.event_for_id(folder_id, message_id) {
                        if store
                            .delete_accessible_event(principal.account_id, event.canonical_id)
                            .await
                            .is_err()
                        {
                            partial_completion = true;
                        }
                        continue;
                    }
                    if let Some(task) = snapshot.task_for_id(folder_id, message_id) {
                        if store
                            .delete_accessible_task(principal.account_id, task.canonical_id)
                            .await
                            .is_err()
                        {
                            partial_completion = true;
                        }
                        continue;
                    }
                    if let Some(note) = snapshot.note_for_id(folder_id, message_id) {
                        if store
                            .delete_mapi_note(principal.account_id, note.canonical_id)
                            .await
                            .is_err()
                        {
                            partial_completion = true;
                        }
                        continue;
                    }
                    if let Some(entry) = snapshot.journal_entry_for_id(folder_id, message_id) {
                        if store
                            .delete_mapi_journal_entry(principal.account_id, entry.canonical_id)
                            .await
                            .is_err()
                        {
                            partial_completion = true;
                        }
                        continue;
                    }
                    if let Some(message) = snapshot.conversation_action_message_for_id(message_id) {
                        if store
                            .delete_conversation_action(principal.account_id, message.canonical_id)
                            .await
                            .is_err()
                        {
                            partial_completion = true;
                        }
                        continue;
                    }
                    let Some(email) = message_for_id(folder_id, message_id, mailboxes, emails)
                    else {
                        partial_completion = true;
                        continue;
                    };
                    let result = if request.rop_id == 0x59
                        || request.rop_id == 0x91
                        || email.mailbox_role == "trash"
                    {
                        store
                            .delete_jmap_email_from_mailbox(
                                principal.account_id,
                                email.mailbox_id,
                                email.id,
                                AuditEntryInput {
                                    actor: principal.email.clone(),
                                    action: "mapi-delete-message".to_string(),
                                    subject: format!("message:{}", email.id),
                                },
                            )
                            .await
                            .map(|_| ())
                    } else if let Some(trash_mailbox) =
                        mailboxes.iter().find(|mailbox| mailbox.role == "trash")
                    {
                        store
                            .move_jmap_email_from_mailbox(
                                principal.account_id,
                                email.mailbox_id,
                                email.id,
                                trash_mailbox.id,
                                AuditEntryInput {
                                    actor: principal.email.clone(),
                                    action: "mapi-move-message-to-trash".to_string(),
                                    subject: format!("message:{}->{}", email.id, trash_mailbox.id),
                                },
                            )
                            .await
                            .map(|_| ())
                    } else {
                        store
                            .delete_jmap_email_from_mailbox(
                                principal.account_id,
                                email.mailbox_id,
                                email.id,
                                AuditEntryInput {
                                    actor: principal.email.clone(),
                                    action: "mapi-delete-message-without-trash".to_string(),
                                    subject: format!("message:{}", email.id),
                                },
                            )
                            .await
                            .map(|_| ())
                    };
                    if result.is_err() {
                        partial_completion = true;
                    }
                }
                if !partial_completion {
                    session.record_notification(MapiNotificationEvent::content(folder_id, None));
                }
                responses.extend_from_slice(&rop_partial_completion_response(
                    request.rop_id,
                    request.response_handle_index(),
                    partial_completion,
                ));
            }
            Some(RopId::GetMessageStatus | RopId::SetMessageStatus) => {
                let folder_id = match input_object(session, &handle_slots, &request)
                    .and_then(MapiObject::folder_id)
                {
                    Some(folder_id) => folder_id,
                    None => {
                        responses.extend_from_slice(&rop_error_response(
                            0x20,
                            request.response_handle_index(),
                            0x0000_04B9,
                        ));
                        continue;
                    }
                };
                let message_id = request.status_message_id().unwrap_or(0);
                if message_for_id(folder_id, message_id, mailboxes, emails)
                    .or_else(|| {
                        emails
                            .iter()
                            .find(|email| mapi_item_id_matches(&email.id, message_id))
                    })
                    .is_none()
                {
                    responses.extend_from_slice(&rop_error_response(
                        0x20,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                }
                let key = (folder_id, message_id);
                let old_status = session.message_statuses.get(&key).copied().unwrap_or(0);
                if request.rop_id == 0x20 {
                    let mask = request.message_status_mask();
                    let new_status = (old_status & !mask) | (request.message_status_flags() & mask);
                    if new_status == 0 {
                        session.message_statuses.remove(&key);
                    } else {
                        session.message_statuses.insert(key, new_status);
                    }
                }
                responses.extend_from_slice(&rop_message_status_response(&request, old_status));
            }
            Some(RopId::FindRow) => responses.extend_from_slice(&rop_find_row_response(
                &request,
                input_object_mut(session, &handle_slots, &request),
                mailboxes,
                emails,
                snapshot,
            )),
            Some(RopId::GetValidAttachments) => {
                responses.extend_from_slice(&rop_get_valid_attachments_response(
                    &request,
                    input_object(session, &handle_slots, &request),
                    snapshot,
                ))
            }
            Some(RopId::GetAttachmentTable) => {
                let (folder_id, message_id) = match input_object(session, &handle_slots, &request) {
                    Some(MapiObject::Message {
                        folder_id,
                        message_id,
                    })
                    | Some(MapiObject::Event {
                        folder_id,
                        event_id: message_id,
                    }) => (*folder_id, *message_id),
                    _ => {
                        responses.extend_from_slice(&rop_error_response(
                            0x21,
                            request.output_handle_index.unwrap_or(0),
                            0x8004_010F,
                        ));
                        continue;
                    }
                };
                let row_count = snapshot
                    .attachments_for_message(folder_id, message_id)
                    .unwrap_or_default()
                    .len() as u32;
                let handle = session.allocate_output_handle(
                    request.output_handle_index,
                    MapiObject::AttachmentTable {
                        folder_id,
                        message_id,
                        columns: Vec::new(),
                        sort_orders: Vec::new(),
                        restriction: None,
                        bookmarks: HashMap::new(),
                        next_bookmark: 1,
                        position: 0,
                    },
                );
                set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                responses
                    .extend_from_slice(&rop_get_attachment_table_response(&request, row_count));
                output_handles.push(handle);
            }
            Some(RopId::OpenAttachment) => {
                let (folder_id, message_id) = match input_object(session, &handle_slots, &request) {
                    Some(MapiObject::Message {
                        folder_id,
                        message_id,
                    })
                    | Some(MapiObject::Event {
                        folder_id,
                        event_id: message_id,
                    }) => (*folder_id, *message_id),
                    _ => {
                        responses.extend_from_slice(&rop_error_response(
                            0x22,
                            request.output_handle_index.unwrap_or(0),
                            0x8004_010F,
                        ));
                        continue;
                    }
                };
                let attach_num = request.attach_num().unwrap_or(u32::MAX);
                if snapshot
                    .attachment_for_message(folder_id, message_id, attach_num)
                    .is_some()
                {
                    let handle = session.allocate_output_handle(
                        request.output_handle_index,
                        MapiObject::Attachment {
                            folder_id,
                            message_id,
                            attach_num,
                        },
                    );
                    set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                    responses.extend_from_slice(&rop_open_attachment_response(&request));
                    output_handles.push(handle);
                } else {
                    responses.extend_from_slice(&rop_error_response(
                        0x22,
                        request.output_handle_index.unwrap_or(0),
                        0x8004_010F,
                    ));
                }
            }
            Some(RopId::CreateAttachment) => {
                let (folder_id, message_id, is_calendar_event) =
                    match input_object(session, &handle_slots, &request) {
                        Some(MapiObject::Message {
                            folder_id,
                            message_id,
                        }) => (*folder_id, *message_id, false),
                        Some(MapiObject::Event {
                            folder_id,
                            event_id,
                        }) => (*folder_id, *event_id, true),
                        _ => {
                            responses.extend_from_slice(&rop_error_response(
                                0x23,
                                request.output_handle_index.unwrap_or(0),
                                0x0000_04B9,
                            ));
                            continue;
                        }
                    };
                if !is_calendar_event
                    && message_for_id(folder_id, message_id, mailboxes, emails).is_none()
                {
                    responses.extend_from_slice(&rop_error_response(
                        0x23,
                        request.output_handle_index.unwrap_or(0),
                        0x8004_010F,
                    ));
                    continue;
                }
                if is_calendar_event && snapshot.event_for_id(folder_id, message_id).is_none() {
                    responses.extend_from_slice(&rop_error_response(
                        0x23,
                        request.output_handle_index.unwrap_or(0),
                        0x8004_010F,
                    ));
                    continue;
                }

                let attach_num =
                    next_pending_attachment_num(session, folder_id, message_id, snapshot);
                let handle = session.allocate_output_handle(
                    request.output_handle_index,
                    MapiObject::PendingAttachment {
                        folder_id,
                        message_id,
                        attach_num,
                        properties: HashMap::new(),
                        data: Vec::new(),
                    },
                );
                set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                responses.extend_from_slice(&rop_create_attachment_response(&request, attach_num));
                output_handles.push(handle);
            }
            Some(RopId::DeleteAttachment) => {
                let Some(MapiObject::Message {
                    folder_id,
                    message_id,
                }) = input_object(session, &handle_slots, &request)
                else {
                    responses.extend_from_slice(&rop_error_response(
                        0x24,
                        request.response_handle_index(),
                        0x0000_04B9,
                    ));
                    continue;
                };
                let attach_num = request.attach_num().unwrap_or(u32::MAX);
                let Some(attachment) =
                    snapshot.attachment_for_message(*folder_id, *message_id, attach_num)
                else {
                    responses.extend_from_slice(&rop_error_response(
                        0x24,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                match store
                    .delete_message_attachment(
                        principal.account_id,
                        &attachment.file_reference,
                        AuditEntryInput {
                            actor: principal.email.clone(),
                            action: "mapi-delete-attachment".to_string(),
                            subject: attachment.file_reference.clone(),
                        },
                    )
                    .await
                {
                    Ok(Some(_)) => {
                        responses.extend_from_slice(&rop_simple_success_response(&request))
                    }
                    _ => responses.extend_from_slice(&rop_error_response(
                        0x24,
                        request.response_handle_index(),
                        0x8004_010F,
                    )),
                }
            }
            Some(RopId::SaveChangesAttachment) => {
                let Some(handle) = input_handle(&handle_slots, &request) else {
                    responses.extend_from_slice(&rop_error_response(
                        0x25,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                let Some(MapiObject::PendingAttachment {
                    folder_id,
                    message_id,
                    attach_num,
                    properties,
                    data,
                }) = session.handles.get(&handle).cloned()
                else {
                    responses.extend_from_slice(&rop_error_response(
                        0x25,
                        request.response_handle_index(),
                        0x0000_04B9,
                    ));
                    continue;
                };
                let attachment = pending_attachment_upload(attach_num, &properties, data);
                let validation = validator.validate_bytes(
                    ValidationRequest {
                        ingress_context: IngressContext::ExchangeAttachment,
                        declared_mime: Some(attachment.media_type.clone()),
                        filename: Some(attachment.file_name.clone()),
                        expected_kind: mapi_expected_attachment_kind(
                            &attachment.media_type,
                            &attachment.file_name,
                        ),
                    },
                    &attachment.blob_bytes,
                );
                let Ok(outcome) = validation else {
                    responses.extend_from_slice(&rop_error_response(
                        0x25,
                        request.response_handle_index(),
                        0x8004_0102,
                    ));
                    continue;
                };
                if outcome.policy_decision != PolicyDecision::Accept {
                    responses.extend_from_slice(&rop_error_response(
                        0x25,
                        request.response_handle_index(),
                        0x8004_0102,
                    ));
                    continue;
                }
                let mut attachment = attachment;
                if attachment.media_type == "application/octet-stream"
                    && !outcome.detected_mime.trim().is_empty()
                {
                    attachment.media_type = outcome.detected_mime;
                }
                if let Some(email) = message_for_id(folder_id, message_id, mailboxes, emails) {
                    match store
                        .add_message_attachment(
                            principal.account_id,
                            email.id,
                            attachment,
                            AuditEntryInput {
                                actor: principal.email.clone(),
                                action: "mapi-save-attachment".to_string(),
                                subject: format!("message:{}", email.id),
                            },
                        )
                        .await
                    {
                        Ok(Some((_email, stored))) => {
                            session.handles.insert(
                                handle,
                                MapiObject::SavedAttachment {
                                    folder_id,
                                    message_id,
                                    attach_num,
                                    file_reference: stored.file_reference,
                                    file_name: stored.file_name,
                                    media_type: stored.media_type,
                                    size_octets: stored.size_octets,
                                },
                            );
                            responses.extend_from_slice(&rop_simple_success_response(&request));
                        }
                        _ => responses.extend_from_slice(&rop_error_response(
                            0x25,
                            request.response_handle_index(),
                            0x8004_010F,
                        )),
                    }
                } else if let Some(event) = snapshot.event_for_id(folder_id, message_id) {
                    match store
                        .add_calendar_event_attachment(
                            principal.account_id,
                            event.canonical_id,
                            attachment,
                            AuditEntryInput {
                                actor: principal.email.clone(),
                                action: "mapi-save-calendar-attachment".to_string(),
                                subject: format!("calendar-event:{}", event.canonical_id),
                            },
                        )
                        .await
                    {
                        Ok(Some(stored)) => {
                            session.handles.insert(
                                handle,
                                MapiObject::SavedAttachment {
                                    folder_id,
                                    message_id,
                                    attach_num,
                                    file_reference: stored.file_reference,
                                    file_name: stored.file_name,
                                    media_type: stored.media_type,
                                    size_octets: stored.size_octets,
                                },
                            );
                            responses.extend_from_slice(&rop_simple_success_response(&request));
                        }
                        _ => responses.extend_from_slice(&rop_error_response(
                            0x25,
                            request.response_handle_index(),
                            0x8004_010F,
                        )),
                    }
                } else {
                    responses.extend_from_slice(&rop_error_response(
                        0x25,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                }
            }
            Some(RopId::OpenStream) => {
                let Some(input_handle) = input_handle(&handle_slots, &request) else {
                    responses.extend_from_slice(&rop_error_response(
                        0x2B,
                        request.output_handle_index.unwrap_or(0),
                        0x8004_010F,
                    ));
                    continue;
                };
                let Some((stream_data, writable_target)) = open_stream_data(
                    store,
                    principal,
                    session,
                    input_handle,
                    request.stream_property_tag().unwrap_or(0),
                    request.stream_open_mode().unwrap_or(0),
                    mailboxes,
                    emails,
                    snapshot,
                )
                .await
                else {
                    responses.extend_from_slice(&rop_error_response(
                        0x2B,
                        request.output_handle_index.unwrap_or(0),
                        0x8004_010F,
                    ));
                    continue;
                };
                let stream_size = stream_data.len();
                let handle = session.allocate_output_handle(
                    request.output_handle_index,
                    MapiObject::AttachmentStream {
                        data: stream_data,
                        position: 0,
                        writable_target,
                    },
                );
                set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                responses.extend_from_slice(&rop_open_stream_response(&request, stream_size));
                output_handles.push(handle);
            }
            Some(RopId::ReadStream) => {
                let Some(stream) = input_object_mut(session, &handle_slots, &request) else {
                    responses.extend_from_slice(&rop_error_response(
                        0x2C,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                responses.extend_from_slice(&rop_read_stream_response(&request, stream));
            }
            Some(RopId::SeekStream) => {
                let Some(stream) = input_object_mut(session, &handle_slots, &request) else {
                    responses.extend_from_slice(&rop_error_response(
                        0x2E,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                responses.extend_from_slice(&rop_seek_stream_response(&request, stream));
            }
            Some(RopId::SetStreamSize) => {
                let Some(stream_handle) = input_handle(&handle_slots, &request) else {
                    responses.extend_from_slice(&rop_error_response(
                        0x2F,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                match set_attachment_stream_size(
                    session,
                    stream_handle,
                    request.stream_size().unwrap_or(u64::MAX),
                ) {
                    Some(()) => responses.extend_from_slice(&rop_simple_success_response(&request)),
                    None => responses.extend_from_slice(&rop_error_response(
                        0x2F,
                        request.response_handle_index(),
                        0x8004_0102,
                    )),
                }
            }
            Some(RopId::WriteStream | RopId::WriteAndCommitStream | RopId::WriteStreamExtended) => {
                let Some(stream_handle) = input_handle(&handle_slots, &request) else {
                    responses.extend_from_slice(&rop_error_response(
                        request.rop_id,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                match write_stream(session, stream_handle, request.stream_write_data()) {
                    Some(written) => {
                        responses.extend_from_slice(&rop_write_stream_response(&request, written))
                    }
                    None => {
                        let error_code = stream_write_error_code(
                            stream_write_error(session, stream_handle)
                                .unwrap_or(StreamWriteError::NotFound),
                        );
                        responses.extend_from_slice(&rop_error_response(
                            request.rop_id,
                            request.response_handle_index(),
                            error_code,
                        ))
                    }
                }
            }
            Some(RopId::CopyToStream) => {
                let Some(source_handle) = input_handle(&handle_slots, &request) else {
                    responses.extend_from_slice(&rop_error_response(
                        0x3A,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                let Some(destination_handle) = request.move_copy_target_handle(&handle_slots)
                else {
                    responses.extend_from_slice(&rop_error_response(
                        0x3A,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                match copy_stream(
                    session,
                    source_handle,
                    destination_handle,
                    request.stream_size().unwrap_or(u64::MAX),
                ) {
                    Some((read, written)) => responses
                        .extend_from_slice(&rop_copy_to_stream_response(&request, read, written)),
                    None => responses.extend_from_slice(&rop_error_response(
                        0x3A,
                        request.response_handle_index(),
                        0x8004_0102,
                    )),
                }
            }
            Some(RopId::GetStreamSize) => match input_object(session, &handle_slots, &request) {
                Some(MapiObject::AttachmentStream { data, .. }) => {
                    responses.extend_from_slice(&rop_get_stream_size_response(&request, data.len()))
                }
                _ => responses.extend_from_slice(&rop_error_response(
                    0x5E,
                    request.response_handle_index(),
                    0x8004_010F,
                )),
            },
            Some(RopId::CloneStream) => {
                match input_object(session, &handle_slots, &request).cloned() {
                    Some(MapiObject::AttachmentStream {
                        data,
                        position,
                        writable_target: None,
                    }) => {
                        let handle = session.allocate_output_handle(
                            request.output_handle_index,
                            MapiObject::AttachmentStream {
                                data,
                                position,
                                writable_target: None,
                            },
                        );
                        set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                        responses.extend_from_slice(&rop_simple_success_response(&request));
                        output_handles.push(handle);
                    }
                    Some(MapiObject::AttachmentStream { .. }) => responses.extend_from_slice(
                        &rop_error_response(0x3B, request.response_handle_index(), 0x8004_0102),
                    ),
                    _ => responses.extend_from_slice(&rop_error_response(
                        0x3B,
                        request.response_handle_index(),
                        0x8004_010F,
                    )),
                }
            }
            Some(RopId::LockRegionStream | RopId::UnlockRegionStream) => responses
                .extend_from_slice(&rop_error_response(
                    request.rop_id,
                    request.response_handle_index(),
                    0x8004_0102,
                )),
            Some(RopId::CommitStream) => match input_object(session, &handle_slots, &request) {
                Some(MapiObject::AttachmentStream { .. }) => {
                    responses.extend_from_slice(&rop_simple_success_response(&request))
                }
                _ => responses.extend_from_slice(&rop_error_response(
                    0x5D,
                    request.response_handle_index(),
                    0x8004_010F,
                )),
            },
            Some(RopId::SubmitMessage | RopId::TransportSend) => {
                let Some(handle) = input_handle(&handle_slots, &request) else {
                    responses.extend_from_slice(&rop_error_response(
                        request.rop_id,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                let Some(object) = session.handles.get(&handle).cloned() else {
                    responses.extend_from_slice(&rop_error_response(
                        request.rop_id,
                        request.response_handle_index(),
                        0x0000_04B9,
                    ));
                    continue;
                };
                let input = match object {
                    MapiObject::PendingMessage {
                        properties,
                        recipients,
                        ..
                    } => mapi_submit_from_pending_message(principal, &properties, &recipients),
                    MapiObject::Message {
                        folder_id,
                        message_id,
                    } => {
                        let Some(email) = message_for_id(folder_id, message_id, mailboxes, emails)
                        else {
                            responses.extend_from_slice(&rop_error_response(
                                request.rop_id,
                                request.response_handle_index(),
                                0x8004_010F,
                            ));
                            continue;
                        };
                        if email.mailbox_role != "drafts" {
                            responses.extend_from_slice(&rop_error_response(
                                request.rop_id,
                                request.response_handle_index(),
                                0x8004_0102,
                            ));
                            continue;
                        }
                        let protected_emails = match store
                            .fetch_jmap_emails_with_protected_bcc(principal.account_id, &[email.id])
                            .await
                        {
                            Ok(emails) => emails,
                            Err(error) => {
                                warn!(
                                    error = %error,
                                    "failed to load protected Bcc recipients for MAPI draft submit"
                                );
                                responses.extend_from_slice(&rop_error_response(
                                    request.rop_id,
                                    request.response_handle_index(),
                                    0x8004_010F,
                                ));
                                continue;
                            }
                        };
                        let protected_email =
                            protected_emails.iter().find(|loaded| loaded.id == email.id);
                        let source_email = protected_email.unwrap_or(email);
                        let attachments = match mapi_submit_attachments_from_email(
                            store,
                            principal.account_id,
                            source_email,
                        )
                        .await
                        {
                            Ok(attachments) => attachments,
                            Err(error) => {
                                warn!(
                                    error = %error,
                                    "failed to load attachments for MAPI draft submit"
                                );
                                responses.extend_from_slice(&rop_error_response(
                                    request.rop_id,
                                    request.response_handle_index(),
                                    0x8004_010F,
                                ));
                                continue;
                            }
                        };
                        mapi_submit_from_email(principal, source_email, attachments)
                    }
                    _ => {
                        responses.extend_from_slice(&rop_error_response(
                            request.rop_id,
                            request.response_handle_index(),
                            0x0000_04B9,
                        ));
                        continue;
                    }
                };
                match store
                    .submit_message(
                        input,
                        AuditEntryInput {
                            actor: principal.email.clone(),
                            action: "mapi-submit-message".to_string(),
                            subject: format!("handle:{handle}"),
                        },
                    )
                    .await
                {
                    Ok(submitted) => {
                        let message_id = match remember_created_mapi_identity(
                            store,
                            principal,
                            MapiIdentityObjectKind::Message,
                            submitted.message_id,
                            None,
                        )
                        .await
                        {
                            Ok(message_id) => message_id,
                            Err(_) => {
                                responses.extend_from_slice(&rop_error_response(
                                    request.rop_id,
                                    request.response_handle_index(),
                                    0x8004_010F,
                                ));
                                continue;
                            }
                        };
                        session.handles.insert(
                            handle,
                            MapiObject::Message {
                                folder_id: submitted_mapi_folder_id(&submitted, mailboxes),
                                message_id,
                            },
                        );
                        if request.rop_id == 0x4A {
                            responses
                                .extend_from_slice(&rop_transport_send_success_response(&request));
                        } else {
                            responses.extend_from_slice(&rop_simple_success_response(&request));
                        }
                    }
                    Err(_) => responses.extend_from_slice(&rop_error_response(
                        request.rop_id,
                        request.response_handle_index(),
                        0x8004_010F,
                    )),
                }
            }
            Some(RopId::MoveCopyMessages) => {
                let source_folder_id = match input_object(session, &handle_slots, &request) {
                    Some(MapiObject::Folder { folder_id, .. }) => *folder_id,
                    _ => {
                        responses.extend_from_slice(&rop_error_response(
                            0x33,
                            request.response_handle_index(),
                            0x0000_04B9,
                        ));
                        continue;
                    }
                };
                let target_folder_id = match request
                    .move_copy_target_handle(&handle_slots)
                    .and_then(|handle| {
                        session
                            .handles
                            .get(&handle)
                            .and_then(|object| object.folder_id())
                    }) {
                    Some(folder_id) => folder_id,
                    None => {
                        responses.extend_from_slice(&rop_error_response(
                            0x33,
                            request.response_handle_index(),
                            0x8004_010F,
                        ));
                        continue;
                    }
                };
                if matches!(source_folder_id, NOTES_FOLDER_ID | JOURNAL_FOLDER_ID) {
                    let mut partial_completion = false;
                    for message_id in request.move_copy_message_ids() {
                        if source_folder_id == NOTES_FOLDER_ID {
                            let Some(note) = snapshot.note_for_id(source_folder_id, message_id)
                            else {
                                partial_completion = true;
                                continue;
                            };
                            if target_folder_id != NOTES_FOLDER_ID {
                                partial_completion = true;
                                continue;
                            }
                            if request.move_copy_want_copy() {
                                match store
                                    .upsert_mapi_note(UpsertClientNoteInput {
                                        id: None,
                                        account_id: principal.account_id,
                                        title: note.note.title.clone(),
                                        body_text: note.note.body_text.clone(),
                                        color: note.note.color.clone(),
                                        categories_json: note.note.categories_json.clone(),
                                    })
                                    .await
                                {
                                    Ok(copied) => {
                                        if remember_created_mapi_identity(
                                            store,
                                            principal,
                                            MapiIdentityObjectKind::Note,
                                            copied.id,
                                            None,
                                        )
                                        .await
                                        .is_err()
                                        {
                                            partial_completion = true;
                                        }
                                    }
                                    Err(_) => partial_completion = true,
                                }
                            }
                            continue;
                        }
                        let Some(entry) =
                            snapshot.journal_entry_for_id(source_folder_id, message_id)
                        else {
                            partial_completion = true;
                            continue;
                        };
                        if target_folder_id != JOURNAL_FOLDER_ID {
                            partial_completion = true;
                            continue;
                        }
                        if request.move_copy_want_copy() {
                            match store
                                .upsert_mapi_journal_entry(UpsertJournalEntryInput {
                                    id: None,
                                    account_id: principal.account_id,
                                    subject: entry.entry.subject.clone(),
                                    body_text: entry.entry.body_text.clone(),
                                    entry_type: entry.entry.entry_type.clone(),
                                    message_class: entry.entry.message_class.clone(),
                                    starts_at: entry.entry.starts_at.clone(),
                                    ends_at: entry.entry.ends_at.clone(),
                                    occurred_at: entry.entry.occurred_at.clone(),
                                    companies_json: entry.entry.companies_json.clone(),
                                    contacts_json: entry.entry.contacts_json.clone(),
                                })
                                .await
                            {
                                Ok(copied) => {
                                    if remember_created_mapi_identity(
                                        store,
                                        principal,
                                        MapiIdentityObjectKind::JournalEntry,
                                        copied.id,
                                        None,
                                    )
                                    .await
                                    .is_err()
                                    {
                                        partial_completion = true;
                                    }
                                }
                                Err(_) => partial_completion = true,
                            }
                        }
                    }
                    responses.extend_from_slice(&rop_partial_completion_response(
                        0x33,
                        request.response_handle_index(),
                        partial_completion,
                    ));
                    continue;
                }
                let Some(target_mailbox) = folder_row_for_id(target_folder_id, mailboxes) else {
                    responses.extend_from_slice(&rop_error_response(
                        0x33,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                let mut partial_completion = false;
                for message_id in request.move_copy_message_ids() {
                    let Some(email) =
                        message_for_id(source_folder_id, message_id, mailboxes, emails)
                    else {
                        partial_completion = true;
                        continue;
                    };
                    let result = if request.move_copy_want_copy() {
                        store
                            .copy_jmap_email(
                                principal.account_id,
                                email.id,
                                target_mailbox.id,
                                AuditEntryInput {
                                    actor: principal.email.clone(),
                                    action: "mapi-copy-message".to_string(),
                                    subject: format!("message:{}->{}", email.id, target_mailbox.id),
                                },
                            )
                            .await
                            .map(|_| ())
                    } else {
                        store
                            .move_jmap_email(
                                principal.account_id,
                                email.id,
                                target_mailbox.id,
                                AuditEntryInput {
                                    actor: principal.email.clone(),
                                    action: "mapi-move-message".to_string(),
                                    subject: format!("message:{}->{}", email.id, target_mailbox.id),
                                },
                            )
                            .await
                            .map(|_| ())
                    };
                    if result.is_err() {
                        partial_completion = true;
                    }
                }
                responses.extend_from_slice(&rop_partial_completion_response(
                    0x33,
                    request.response_handle_index(),
                    partial_completion,
                ));
            }
            Some(RopId::SetReceiveFolder) => responses.extend_from_slice(&rop_error_response(
                0x26,
                request.response_handle_index(),
                0x8004_0102,
            )),
            Some(RopId::GetReceiveFolder) => {
                echo_input_handle_table = true;
                let Some(message_class) = request.receive_folder_message_class() else {
                    responses.extend_from_slice(&rop_error_response(
                        0x27,
                        request.response_handle_index(),
                        0x8007_0057,
                    ));
                    continue;
                };
                if !valid_receive_folder_message_class(message_class) {
                    responses.extend_from_slice(&rop_error_response(
                        0x27,
                        request.response_handle_index(),
                        0x8007_0057,
                    ));
                    continue;
                }
                if session.hierarchy_sync_completed() {
                    let response_folder_id = receive_folder_id_for_message_class(message_class);
                    tracing::info!(
                        rca_debug = true,
                        adapter = "mapi",
                        endpoint = "emsmdb",
                        account_id = %principal.account_id,
                        mailbox = %principal.email,
                        input_handle_index = request.input_handle_index().unwrap_or(0),
                        response_handle_index = request.response_handle_index(),
                        requested_message_class = %message_class,
                        response_message_class =
                            %explicit_receive_folder_message_class(message_class),
                        response_folder_id = %format!("0x{response_folder_id:016x}"),
                        "rca debug mapi post hierarchy get receive folder"
                    );
                }
                let response_folder_id = receive_folder_id_for_message_class(message_class);
                responses.extend_from_slice(&rop_get_receive_folder_response(
                    &request,
                    response_folder_id,
                    explicit_receive_folder_message_class(message_class),
                ));
            }
            Some(RopId::SetReadFlags) => {
                let folder_id = match input_object(session, &handle_slots, &request) {
                    Some(MapiObject::Folder { folder_id, .. }) => *folder_id,
                    _ => {
                        responses.extend_from_slice(&rop_error_response(
                            0x66,
                            request.response_handle_index(),
                            0x0000_04B9,
                        ));
                        continue;
                    }
                };
                let unread = unread_from_read_flags(request.read_flags());
                let mut partial_completion = false;
                for message_id in request.message_ids() {
                    let Some(email) = message_for_id(folder_id, message_id, mailboxes, emails)
                        .or_else(|| {
                            emails
                                .iter()
                                .find(|email| mapi_item_id_matches(&email.id, message_id))
                        })
                    else {
                        partial_completion = true;
                        continue;
                    };
                    if let Some(unread) = unread {
                        if store
                            .update_jmap_email_flags(
                                principal.account_id,
                                email.id,
                                Some(unread),
                                None,
                                AuditEntryInput {
                                    actor: principal.email.clone(),
                                    action: "mapi-set-read-flags".to_string(),
                                    subject: format!("message:{}", email.id),
                                },
                            )
                            .await
                            .is_err()
                        {
                            partial_completion = true;
                        }
                    }
                }
                responses
                    .extend_from_slice(&rop_set_read_flags_response(&request, partial_completion));
            }
            Some(RopId::SynchronizationConfigure) => {
                let Some(folder_id) =
                    input_object(session, &handle_slots, &request).and_then(MapiObject::folder_id)
                else {
                    responses.extend_from_slice(&rop_error_response(
                        0x70,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                let sync_type = request.sync_type();
                if MapiSyncType::from_u8(sync_type).is_none() {
                    responses.extend_from_slice(&rop_error_response(
                        0x70,
                        request.response_handle_index(),
                        0x8004_0102,
                    ));
                    break;
                }
                let sync_flags = request.sync_flags();
                let sync_extra_flags = request.sync_extra_flags();
                let sync_property_tags = request.sync_property_tags();
                if !property_tags_are_supported(&sync_property_tags) {
                    responses.extend_from_slice(&rop_error_response(
                        0x70,
                        request.response_handle_index(),
                        0x8004_0102,
                    ));
                    break;
                }
                let sync_property_tags_hex = sync_property_tags
                    .iter()
                    .map(|tag| format!("0x{tag:08x}"))
                    .collect::<Vec<_>>()
                    .join(",");
                let checkpoint_kind = sync_checkpoint_kind(sync_type);
                let checkpoint_mailbox_id =
                    sync_checkpoint_mailbox_id(folder_id, sync_type, mailboxes);
                log_calendar_identity_chain(
                    principal,
                    "sync_configure",
                    folder_id,
                    checkpoint_mailbox_id,
                    Some(sync_type),
                    Some(snapshot),
                );
                let folder_role = debug_role_for_folder_id(folder_id);
                let folder_container_class = debug_container_class_for_folder_id(folder_id);
                let checkpoint = match store
                    .fetch_mapi_sync_checkpoint(
                        principal.account_id,
                        checkpoint_mailbox_id,
                        checkpoint_kind,
                    )
                    .await
                {
                    Ok(checkpoint) => checkpoint,
                    Err(_) => {
                        responses.extend_from_slice(&rop_error_response(
                            0x70,
                            request.response_handle_index(),
                            0x8004_0102,
                        ));
                        continue;
                    }
                };
                let checkpoint_status = checkpoint
                    .as_ref()
                    .map(|checkpoint| {
                        hierarchy_checkpoint_status(checkpoint_kind, folder_id, checkpoint)
                    })
                    .unwrap_or("missing");
                let checkpoint_cursor_source = checkpoint
                    .as_ref()
                    .and_then(|checkpoint| checkpoint.cursor_json.get("source"))
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("")
                    .to_string();
                let checkpoint_cursor_sync_root_folder_id = checkpoint
                    .as_ref()
                    .and_then(|checkpoint| checkpoint.cursor_json.get("syncRootFolderId"))
                    .and_then(serde_json::Value::as_u64)
                    .map(|id| format!("0x{id:016x}"))
                    .unwrap_or_default();
                let checkpoint_cursor_hierarchy_sync_version = checkpoint
                    .as_ref()
                    .and_then(|checkpoint| checkpoint.cursor_json.get("hierarchySyncVersion"))
                    .and_then(serde_json::Value::as_u64)
                    .map(|version| version.to_string())
                    .unwrap_or_default();
                let checkpoint_cursor_change_sequence = checkpoint
                    .as_ref()
                    .map(|checkpoint| checkpoint.last_change_sequence)
                    .unwrap_or_default();
                let checkpoint_cursor_modseq = checkpoint
                    .as_ref()
                    .map(|checkpoint| checkpoint.last_modseq)
                    .unwrap_or_default();
                let checkpoint = checkpoint.filter(|_| checkpoint_status == "usable");
                let since = checkpoint
                    .as_ref()
                    .map(|checkpoint| checkpoint.last_change_sequence)
                    .unwrap_or(0);
                let changes = match store
                    .fetch_mapi_sync_changes(
                        principal.account_id,
                        checkpoint_mailbox_id,
                        checkpoint_kind,
                        since,
                    )
                    .await
                {
                    Ok(changes) => changes,
                    Err(_) => {
                        responses.extend_from_slice(&rop_error_response(
                            0x70,
                            request.response_handle_index(),
                            0x8004_0102,
                        ));
                        continue;
                    }
                };
                let all_sync_mailboxes = sync_mailboxes_with_collaboration_counts(
                    sync_mailboxes_for(folder_id, sync_type, mailboxes),
                    snapshot,
                );
                let state_sync_mailboxes = sync_mailboxes_with_collaboration_counts(
                    sync_state_mailboxes_for(folder_id, sync_type, mailboxes),
                    snapshot,
                );
                let all_sync_emails = sync_emails_for(folder_id, sync_type, mailboxes, emails);
                let all_special_sync_objects =
                    special_sync_objects_for(folder_id, sync_type, snapshot, principal.account_id);
                log_calendar_special_sync_objects(
                    principal,
                    folder_id,
                    sync_type,
                    &all_special_sync_objects,
                );
                log_special_sync_objects(
                    principal,
                    folder_id,
                    sync_type,
                    &all_special_sync_objects,
                );
                let available_sync_mailbox_count = all_sync_mailboxes.len();
                let available_sync_email_count = all_sync_emails.len();
                let available_special_sync_object_count = all_special_sync_objects.len();
                let (delta_sync_mailboxes, delta_sync_emails, delta_special_sync_objects) =
                    if checkpoint.is_some() {
                        let changed_special_ids =
                            changed_special_ids_for_folder(folder_id, snapshot, &changes);
                        (
                            changed_sync_mailboxes(
                                all_sync_mailboxes.clone(),
                                &changes.changed_mailbox_ids,
                            ),
                            changed_sync_emails(
                                all_sync_emails.clone(),
                                &changes.changed_message_ids,
                            ),
                            changed_special_sync_objects(
                                all_special_sync_objects.clone(),
                                changed_special_ids,
                            ),
                        )
                    } else {
                        (
                            all_sync_mailboxes.clone(),
                            all_sync_emails.clone(),
                            all_special_sync_objects.clone(),
                        )
                    };
                let sync_attachment_facts =
                    sync_attachment_facts_for(folder_id, &all_sync_emails, snapshot);
                let delta_attachment_facts =
                    sync_attachment_facts_for(folder_id, &delta_sync_emails, snapshot);
                let aggregate_sync_emails = if sync_type == 0x02 {
                    emails.to_vec()
                } else {
                    all_sync_emails.clone()
                };
                let state_attachment_facts =
                    sync_attachment_facts_for(folder_id, &all_sync_emails, snapshot);
                let aggregate_attachment_facts =
                    sync_attachment_facts_for(folder_id, &aggregate_sync_emails, snapshot);
                let mut deleted_message_ids = if checkpoint.is_some() {
                    mapi_message_ids_for_deleted_changes(
                        store,
                        principal,
                        &changes.deleted_message_ids,
                    )
                    .await
                    .unwrap_or_default()
                } else {
                    Vec::new()
                };
                if checkpoint.is_some() && folder_id == NOTES_FOLDER_ID {
                    deleted_message_ids.extend(
                        mapi_object_ids_for_deleted_changes(
                            store,
                            principal,
                            MapiIdentityObjectKind::Note,
                            &changes.deleted_note_ids,
                        )
                        .await
                        .unwrap_or_default(),
                    );
                }
                if checkpoint.is_some() && folder_id == JOURNAL_FOLDER_ID {
                    deleted_message_ids.extend(
                        mapi_object_ids_for_deleted_changes(
                            store,
                            principal,
                            MapiIdentityObjectKind::JournalEntry,
                            &changes.deleted_journal_entry_ids,
                        )
                        .await
                        .unwrap_or_default(),
                    );
                }
                if checkpoint.is_some() {
                    deleted_message_ids.extend(
                        deleted_special_object_ids_for_folder(
                            store, principal, folder_id, snapshot, &changes,
                        )
                        .await,
                    );
                }
                if checkpoint.is_some() && folder_id == CONVERSATION_ACTION_SETTINGS_FOLDER_ID {
                    deleted_message_ids.extend(
                        mapi_object_ids_for_deleted_changes(
                            store,
                            principal,
                            MapiIdentityObjectKind::ConversationAction,
                            &changes.deleted_conversation_action_ids,
                        )
                        .await
                        .unwrap_or_default(),
                    );
                }
                let state = mapi_mailstore::sync_state_token_with_special_objects(
                    sync_type,
                    sync_flags,
                    folder_id,
                    &state_sync_mailboxes,
                    &all_sync_emails,
                    &state_attachment_facts,
                    &all_special_sync_objects,
                );
                let transfer_buffer =
                    mapi_mailstore::sync_manifest_buffer_with_special_objects_and_final_state(
                        principal.account_id,
                        sync_type,
                        sync_flags,
                        sync_extra_flags,
                        &sync_property_tags,
                        folder_id,
                        &all_sync_mailboxes,
                        &all_sync_emails,
                        &sync_attachment_facts,
                        &all_special_sync_objects,
                        &[],
                        mailboxes,
                        &state_sync_mailboxes,
                        &all_sync_emails,
                        &state_attachment_facts,
                        &all_special_sync_objects,
                        &aggregate_sync_emails,
                        &aggregate_attachment_facts,
                        changes.current_change_sequence,
                    );
                mapi_mailstore::log_hierarchy_transfer_debug(
                    sync_type,
                    sync_flags,
                    sync_extra_flags,
                    folder_id,
                    &sync_property_tags,
                    &transfer_buffer,
                );
                let incremental_transfer_buffer = checkpoint.as_ref().map(|_| {
                    mapi_mailstore::sync_manifest_buffer_with_special_objects_and_final_state(
                        principal.account_id,
                        sync_type,
                        sync_flags,
                        sync_extra_flags,
                        &sync_property_tags,
                        folder_id,
                        &delta_sync_mailboxes,
                        &delta_sync_emails,
                        &delta_attachment_facts,
                        &delta_special_sync_objects,
                        &deleted_message_ids,
                        mailboxes,
                        &state_sync_mailboxes,
                        &all_sync_emails,
                        &state_attachment_facts,
                        &all_special_sync_objects,
                        &aggregate_sync_emails,
                        &aggregate_attachment_facts,
                        changes.current_change_sequence,
                    )
                });
                let checkpoint_delta_mailbox_count = delta_sync_mailboxes.len();
                let checkpoint_delta_email_count = delta_sync_emails.len();
                let checkpoint_deleted_message_count = deleted_message_ids.len();
                let incremental_transfer_buffer_bytes = incremental_transfer_buffer
                    .as_ref()
                    .map(|buffer| buffer.len())
                    .unwrap_or_default();
                let scope_flags_present = sync_type != 0x01 || sync_flags & 0x0030 != 0;
                let normal_scope_requested =
                    sync_type != 0x01 || !scope_flags_present || sync_flags & 0x0020 != 0;
                let fai_scope_requested =
                    sync_type != 0x01 || !scope_flags_present || sync_flags & 0x0010 != 0;
                let wire_sync_email_count = if normal_scope_requested {
                    all_sync_emails.len()
                } else {
                    0
                };
                let wire_sync_special_object_count = all_special_sync_objects
                    .iter()
                    .filter(|object| {
                        if object.associated {
                            fai_scope_requested
                        } else {
                            normal_scope_requested
                        }
                    })
                    .count();
                let suppressed_normal_sync_object_count =
                    all_sync_emails.len().saturating_sub(wire_sync_email_count)
                        + all_special_sync_objects
                            .iter()
                            .filter(|object| !object.associated && !normal_scope_requested)
                            .count();
                let suppressed_fai_sync_object_count = all_special_sync_objects
                    .iter()
                    .filter(|object| {
                        object.associated
                            && !fai_scope_requested
                            && object.canonical_id != CALENDAR_BOOTSTRAP_FAI_CANONICAL_ID
                    })
                    .count();
                let checkpoint_store_allowed = suppressed_normal_sync_object_count == 0
                    && suppressed_fai_sync_object_count == 0;
                let checkpoint_skip_reason = if checkpoint_store_allowed {
                    ""
                } else {
                    "partial_content_scope_suppressed_objects"
                };
                tracing::info!(
                    rca_debug = true,
                    adapter = "mapi",
                    endpoint = "emsmdb",
                    mailbox = %principal.email,
                    request_type = "Execute",
                    request_rop_id = "0x70",
                    folder_id = format_args!("0x{folder_id:016x}"),
                    folder_role,
                    folder_container_class,
                    sync_type = format_args!("0x{sync_type:02x}"),
                    sync_flags = format_args!("0x{sync_flags:04x}"),
                    sync_extra_flags = format_args!("0x{sync_extra_flags:08x}"),
                    sync_property_tag_count = sync_property_tags.len(),
                    sync_property_tags = %sync_property_tags_hex,
                    sync_property_filter_mode =
                        sync_property_filter_mode(sync_flags, &sync_property_tags),
                    checkpoint_loaded = checkpoint.is_some(),
                    checkpoint_kind = checkpoint_kind.as_str(),
                    checkpoint_mailbox_id = checkpoint_mailbox_id
                        .map(|id| id.to_string())
                        .unwrap_or_default(),
                    checkpoint_scope = sync_checkpoint_scope(
                        folder_id,
                        checkpoint_mailbox_id,
                        &all_special_sync_objects
                    ),
                    checkpoint_status,
                    checkpoint_cursor_source,
                    checkpoint_cursor_sync_root_folder_id = %checkpoint_cursor_sync_root_folder_id,
                    checkpoint_cursor_hierarchy_sync_version =
                        %checkpoint_cursor_hierarchy_sync_version,
                    checkpoint_cursor_change_sequence,
                    checkpoint_cursor_modseq,
                    snapshot_mailbox_count = mailboxes.len(),
                    snapshot_email_count = emails.len(),
                    available_sync_mailbox_count,
                    available_sync_email_count,
                    available_special_sync_object_count,
                    sync_mailbox_count = all_sync_mailboxes.len(),
                    sync_state_mailbox_count = state_sync_mailboxes.len(),
                    sync_email_count = all_sync_emails.len(),
                    sync_special_object_count = all_special_sync_objects.len(),
                    normal_scope_requested,
                    fai_scope_requested,
                    wire_sync_email_count,
                    wire_sync_special_object_count,
                    suppressed_normal_sync_object_count,
                    suppressed_fai_sync_object_count,
                    checkpoint_store_allowed,
                    checkpoint_skip_reason,
                    checkpoint_delta_mailbox_count,
                    checkpoint_delta_email_count,
                    checkpoint_delta_special_object_count = delta_special_sync_objects.len(),
                    checkpoint_changed_contact_count = changes.changed_contact_ids.len(),
                    checkpoint_changed_calendar_event_count =
                        changes.changed_calendar_event_ids.len(),
                    checkpoint_changed_task_count = changes.changed_task_ids.len(),
                    checkpoint_deleted_contact_count = changes.deleted_contact_ids.len(),
                    checkpoint_deleted_calendar_event_count =
                        changes.deleted_calendar_event_ids.len(),
                    checkpoint_deleted_task_count = changes.deleted_task_ids.len(),
                    checkpoint_deleted_message_count,
                    current_change_sequence = changes.current_change_sequence,
                    generated_sync_state_summary =
                        %mapi_mailstore::final_sync_state_debug_summary(&state),
                    transfer_buffer_bytes = transfer_buffer.len(),
                    incremental_transfer_buffer_bytes,
                    "rca debug mapi sync configure"
                );
                let handle = session.allocate_output_handle(
                    request.output_handle_index,
                    MapiObject::SynchronizationSource {
                        folder_id,
                        mailbox_id: checkpoint_mailbox_id,
                        checkpoint_kind,
                        checkpoint_change_sequence: changes.current_change_sequence,
                        checkpoint_modseq: changes.current_modseq,
                        checkpoint_store_allowed,
                        checkpoint_skip_reason,
                        sync_type,
                        state,
                        state_upload_property_tag: None,
                        state_upload_buffer: Vec::new(),
                        client_state_uploaded_bytes: 0,
                        client_state_uploaded_marker_mask: 0,
                        incremental_transfer_buffer,
                        transfer_buffer,
                        transfer_position: 0,
                    },
                );
                set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                responses.extend_from_slice(&rop_synchronization_configure_response(&request));
                output_handles.push(handle);
                content_sync_configure_observed = sync_type == 0x01;
            }
            Some(RopId::FastTransferSourceCopyMessages) => {
                let Some(folder_id) =
                    input_object(session, &handle_slots, &request).and_then(MapiObject::folder_id)
                else {
                    responses.extend_from_slice(&rop_error_response(
                        0x4B,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                let requested_ids = request.fast_transfer_message_ids();
                let mut selected = emails_for_folder(folder_id, mailboxes, emails)
                    .into_iter()
                    .filter(|email| requested_ids.contains(&mapi_message_id(email)))
                    .cloned()
                    .collect::<Vec<_>>();
                selected.sort_by(|left, right| left.id.cmp(&right.id));
                let sync_attachment_facts =
                    sync_attachment_facts_for(folder_id, &selected, snapshot);
                let transfer_buffer =
                    mapi_mailstore::fast_transfer_manifest_buffer_with_attachments(
                        folder_id,
                        &[],
                        &selected,
                        &sync_attachment_facts,
                    );
                let handle = session.allocate_output_handle(
                    request.output_handle_index,
                    MapiObject::SynchronizationSource {
                        folder_id,
                        mailbox_id: None,
                        checkpoint_kind: MapiCheckpointKind::Content,
                        checkpoint_change_sequence: 0,
                        checkpoint_modseq: 1,
                        checkpoint_store_allowed: true,
                        checkpoint_skip_reason: "",
                        sync_type: 0,
                        state: Vec::new(),
                        state_upload_property_tag: None,
                        state_upload_buffer: Vec::new(),
                        client_state_uploaded_bytes: 0,
                        client_state_uploaded_marker_mask: 0,
                        incremental_transfer_buffer: None,
                        transfer_buffer,
                        transfer_position: 0,
                    },
                );
                set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                responses.extend_from_slice(&rop_fast_transfer_source_copy_response(&request));
                output_handles.push(handle);
            }
            Some(RopId::FastTransferDestinationPutBuffer)
                if first_fast_transfer_marker(&request)
                    .is_some_and(|marker| FastTransferMarker::from_u32(marker).is_none()) =>
            {
                responses.extend_from_slice(&rop_error_response(
                    0x54,
                    request.response_handle_index(),
                    0x8004_0102,
                ));
                break;
            }
            Some(
                RopId::FastTransferSourceCopyFolder
                | RopId::FastTransferSourceCopyTo
                | RopId::FastTransferSourceCopyProperties,
            ) => {
                let Some(object) = input_object(session, &handle_slots, &request).cloned() else {
                    responses.extend_from_slice(&rop_error_response(
                        request.rop_id,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                let Some((folder_id, transfer_buffer)) =
                    fast_transfer_manifest_for_object(&object, mailboxes, emails, snapshot)
                else {
                    responses.extend_from_slice(&rop_error_response(
                        request.rop_id,
                        request.response_handle_index(),
                        0x8004_0102,
                    ));
                    continue;
                };
                let handle = session.allocate_output_handle(
                    request.output_handle_index,
                    MapiObject::SynchronizationSource {
                        folder_id,
                        mailbox_id: None,
                        checkpoint_kind: MapiCheckpointKind::Content,
                        checkpoint_change_sequence: 0,
                        checkpoint_modseq: 1,
                        checkpoint_store_allowed: true,
                        checkpoint_skip_reason: "",
                        sync_type: 0,
                        state: Vec::new(),
                        state_upload_property_tag: None,
                        state_upload_buffer: Vec::new(),
                        client_state_uploaded_bytes: 0,
                        client_state_uploaded_marker_mask: 0,
                        incremental_transfer_buffer: None,
                        transfer_buffer,
                        transfer_position: 0,
                    },
                );
                set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                responses.extend_from_slice(&rop_fast_transfer_source_copy_response(&request));
                output_handles.push(handle);
            }
            Some(RopId::FastTransferSourceGetBuffer) => {
                match input_object_mut(session, &handle_slots, &request) {
                    Some(MapiObject::SynchronizationSource {
                        folder_id,
                        mailbox_id,
                        checkpoint_kind,
                        checkpoint_change_sequence,
                        checkpoint_modseq,
                        checkpoint_store_allowed,
                        checkpoint_skip_reason,
                        sync_type,
                        state,
                        state_upload_buffer,
                        client_state_uploaded_bytes,
                        client_state_uploaded_marker_mask,
                        incremental_transfer_buffer,
                        transfer_buffer,
                        transfer_position,
                        ..
                    }) => {
                        let requested_buffer_bytes = request.fast_transfer_buffer_size();
                        let previous_transfer_position = *transfer_position;
                        let response = rop_fast_transfer_source_get_buffer_response(
                            &request,
                            transfer_buffer,
                            transfer_position,
                        );
                        let completed = *transfer_position >= transfer_buffer.len();
                        let response_debug =
                            summarize_fast_transfer_get_buffer_response(&response, completed);
                        if completed && *sync_type == 0x02 {
                            let hierarchy_close_summary =
                                mapi_mailstore::hierarchy_transfer_close_summary(
                                    *sync_type,
                                    *folder_id,
                                    transfer_buffer,
                                );
                            completed_hierarchy_sync = Some((
                                *folder_id,
                                format!(
                                    "folder=0x{:016x};checkpoint_kind={};checkpoint_mailbox={};seq={};modseq={};state={};state_summary={};upload_buffer={};client_state={};incremental={};requested={};response={};payload={};status={};completed={};position={}/{};{}",
                                    *folder_id,
                                    checkpoint_kind.as_str(),
                                    (*mailbox_id)
                                        .map(|id| id.to_string())
                                        .unwrap_or_default(),
                                    *checkpoint_change_sequence,
                                    *checkpoint_modseq,
                                    state.len(),
                                    mapi_mailstore::final_sync_state_debug_summary(state),
                                    state_upload_buffer.len(),
                                    *client_state_uploaded_bytes,
                                    incremental_transfer_buffer.is_some(),
                                    requested_buffer_bytes,
                                    response.len(),
                                    response_debug.transfer_payload_bytes,
                                    response_debug.transfer_status,
                                    completed,
                                    *transfer_position,
                                    transfer_buffer.len(),
                                    hierarchy_close_summary
                                ),
                            ));
                        }
                        tracing::info!(
                            rca_debug = true,
                            adapter = "mapi",
                            endpoint = "emsmdb",
                            mailbox = %principal.email,
                            request_type = "Execute",
                            request_rop_id = "0x4e",
                            folder_id = format_args!("0x{:016x}", *folder_id),
                            folder_role = debug_role_for_folder_id(*folder_id),
                            folder_container_class = debug_container_class_for_folder_id(*folder_id),
                            sync_type = format_args!("0x{:02x}", *sync_type),
                            checkpoint_kind = checkpoint_kind.as_str(),
                            checkpoint_mailbox_id = (*mailbox_id)
                                .map(|id| id.to_string())
                                .unwrap_or_default(),
                            checkpoint_change_sequence = *checkpoint_change_sequence,
                            checkpoint_modseq = *checkpoint_modseq,
                            sync_state_bytes = state.len(),
                            sync_state_summary =
                                %mapi_mailstore::final_sync_state_debug_summary(state),
                            upload_state_buffer_bytes = state_upload_buffer.len(),
                            upload_state_client_bytes = *client_state_uploaded_bytes,
                            upload_state_marker_mask =
                                format_args!("0x{:02x}", *client_state_uploaded_marker_mask),
                            upload_state_markers =
                                %uploaded_state_marker_summary(*client_state_uploaded_marker_mask),
                            upload_state_has_delta_anchor =
                                uploaded_state_has_delta_anchor(*client_state_uploaded_marker_mask),
                            incremental_transfer_available = incremental_transfer_buffer.is_some(),
                            incremental_transfer_buffer_bytes = incremental_transfer_buffer
                                .as_ref()
                                .map(|buffer| buffer.len())
                                .unwrap_or_default(),
                            requested_buffer_bytes,
                            transfer_position_before = previous_transfer_position,
                            transfer_position_after = *transfer_position,
                            transfer_buffer_bytes = transfer_buffer.len(),
                            transfer_chunk_bytes =
                                (*transfer_position).saturating_sub(previous_transfer_position),
                            transfer_completed = completed,
                            transfer_status = if completed { "0x0003" } else { "0x0001" },
                            get_buffer_response_bytes = response.len(),
                            get_buffer_response_header_bytes = response_debug.header_bytes,
                            get_buffer_response_rop_id = %response_debug.rop_id,
                            get_buffer_response_rop_id_matches = response_debug.rop_id_matches,
                            get_buffer_response_handle_index = response_debug.handle_index,
                            get_buffer_return_value = %response_debug.return_value,
                            get_buffer_transfer_status_wire = %response_debug.transfer_status,
                            get_buffer_transfer_status_matches_completed =
                                response_debug.transfer_status_matches_completed,
                            get_buffer_in_progress_count = response_debug.in_progress_count,
                            get_buffer_total_step_count = response_debug.total_step_count,
                            get_buffer_reserved_byte = response_debug.reserved_byte,
                            get_buffer_reserved_zero = response_debug.reserved_zero,
                            get_buffer_transfer_buffer_size_wire =
                                response_debug.transfer_buffer_size,
                            get_buffer_transfer_payload_bytes = response_debug.transfer_payload_bytes,
                            get_buffer_transfer_buffer_size_matches_payload =
                                response_debug.transfer_buffer_size_matches_payload,
                            get_buffer_transfer_payload_preview_hex =
                                %response_debug.transfer_payload_preview_hex,
                            get_buffer_transfer_payload_tail_hex =
                                %response_debug.transfer_payload_tail_hex,
                            get_buffer_response_parse_error = %response_debug.parse_error,
                            "rca debug mapi fast transfer get buffer"
                        );
                        mapi_mailstore::log_hierarchy_get_buffer_payload_summary(
                            *sync_type,
                            *folder_id,
                            if completed { "0x0003" } else { "0x0001" },
                            transfer_buffer,
                        );
                        let checkpoint = (
                            *mailbox_id,
                            *checkpoint_kind,
                            *checkpoint_change_sequence,
                            *checkpoint_modseq,
                            *sync_type,
                            *folder_id,
                        );
                        responses.extend_from_slice(&response);
                        if completed && matches!(checkpoint.4, 0x01 | 0x02) {
                            let mut cursor_json = serde_json::json!({
                                "syncType": checkpoint.4,
                                "syncRootFolderId": checkpoint.5,
                                "source": "emsmdb-ics-download"
                            });
                            if checkpoint.1 == MapiCheckpointKind::Hierarchy {
                                cursor_json["hierarchySyncVersion"] =
                                    serde_json::json!(HIERARCHY_SYNC_CURSOR_VERSION);
                            }
                            if checkpoint.1 != MapiCheckpointKind::Hierarchy
                                && checkpoint.0.is_none()
                            {
                                tracing::info!(
                                    rca_debug = true,
                                    adapter = "mapi",
                                    endpoint = "emsmdb",
                                    mailbox = %principal.email,
                                    request_type = "Execute",
                                    request_rop_id = "0x4e",
                                    folder_id = format_args!("0x{:016x}", *folder_id),
                                    folder_role = debug_role_for_folder_id(*folder_id),
                                    folder_container_class =
                                        debug_container_class_for_folder_id(*folder_id),
                                    sync_type = format_args!("0x{:02x}", checkpoint.4),
                                    checkpoint_kind = checkpoint.1.as_str(),
                                    checkpoint_mailbox_id = "",
                                    checkpoint_change_sequence = checkpoint.2,
                                    checkpoint_modseq = checkpoint.3,
                                    sync_state_bytes = state.len(),
                                    upload_state_buffer_bytes = state_upload_buffer.len(),
                                    upload_state_client_bytes = *client_state_uploaded_bytes,
                                    incremental_transfer_available = incremental_transfer_buffer.is_some(),
                                    transfer_buffer_bytes = transfer_buffer.len(),
                                    transfer_position = *transfer_position,
                                    checkpoint_store_status = "skipped_no_mailbox_id",
                                    checkpoint_skip_reason =
                                        "content_or_read_state_sync_without_canonical_mailbox_id",
                                    "rca debug mapi sync checkpoint store"
                                );
                                session.record_completed_sync_checkpoint(
                                    checkpoint.5,
                                    debug_role_for_folder_id(checkpoint.5),
                                    debug_container_class_for_folder_id(checkpoint.5),
                                    checkpoint.1.as_str(),
                                    checkpoint.4,
                                    "skipped_no_mailbox_id",
                                );
                            } else if !*checkpoint_store_allowed {
                                tracing::info!(
                                    rca_debug = true,
                                    adapter = "mapi",
                                    endpoint = "emsmdb",
                                    mailbox = %principal.email,
                                    request_type = "Execute",
                                    request_rop_id = "0x4e",
                                    folder_id = format_args!("0x{:016x}", *folder_id),
                                    folder_role = debug_role_for_folder_id(*folder_id),
                                    folder_container_class =
                                        debug_container_class_for_folder_id(*folder_id),
                                    sync_type = format_args!("0x{:02x}", checkpoint.4),
                                    checkpoint_kind = checkpoint.1.as_str(),
                                    checkpoint_mailbox_id = checkpoint
                                        .0
                                        .map(|id| id.to_string())
                                        .unwrap_or_default(),
                                    checkpoint_change_sequence = checkpoint.2,
                                    checkpoint_modseq = checkpoint.3,
                                    sync_state_bytes = state.len(),
                                    upload_state_buffer_bytes = state_upload_buffer.len(),
                                    upload_state_client_bytes = *client_state_uploaded_bytes,
                                    incremental_transfer_available = incremental_transfer_buffer.is_some(),
                                    transfer_buffer_bytes = transfer_buffer.len(),
                                    transfer_position = *transfer_position,
                                    checkpoint_store_status = "skipped_partial_scope",
                                    checkpoint_skip_reason = *checkpoint_skip_reason,
                                    "rca debug mapi sync checkpoint store"
                                );
                                session.record_completed_sync_checkpoint(
                                    checkpoint.5,
                                    debug_role_for_folder_id(checkpoint.5),
                                    debug_container_class_for_folder_id(checkpoint.5),
                                    checkpoint.1.as_str(),
                                    checkpoint.4,
                                    "skipped_partial_scope",
                                );
                            } else {
                                let checkpoint_result = store
                                    .store_mapi_sync_checkpoint(
                                        principal.account_id,
                                        checkpoint.0,
                                        checkpoint.1,
                                        checkpoint.2,
                                        checkpoint.3,
                                        cursor_json,
                                    )
                                    .await;
                                match checkpoint_result {
                                    Ok(stored_checkpoint) => {
                                        tracing::info!(
                                            rca_debug = true,
                                            adapter = "mapi",
                                            endpoint = "emsmdb",
                                            mailbox = %principal.email,
                                            request_type = "Execute",
                                            request_rop_id = "0x4e",
                                            folder_id = format_args!("0x{:016x}", *folder_id),
                                            folder_role = debug_role_for_folder_id(*folder_id),
                                            folder_container_class =
                                                debug_container_class_for_folder_id(*folder_id),
                                            sync_type = format_args!("0x{:02x}", checkpoint.4),
                                            checkpoint_kind = checkpoint.1.as_str(),
                                            checkpoint_mailbox_id = checkpoint
                                                .0
                                                .map(|id| id.to_string())
                                                .unwrap_or_default(),
                                            checkpoint_change_sequence = checkpoint.2,
                                            checkpoint_modseq = checkpoint.3,
                                            stored_change_sequence = stored_checkpoint.last_change_sequence,
                                            stored_modseq = stored_checkpoint.last_modseq,
                                            sync_state_bytes = state.len(),
                                            upload_state_buffer_bytes = state_upload_buffer.len(),
                                            upload_state_client_bytes = *client_state_uploaded_bytes,
                                            incremental_transfer_available = incremental_transfer_buffer.is_some(),
                                            transfer_buffer_bytes = transfer_buffer.len(),
                                            transfer_position = *transfer_position,
                                            checkpoint_store_status = "ok",
                                            checkpoint_skip_reason = "",
                                            "rca debug mapi sync checkpoint store"
                                        );
                                        session.record_completed_sync_checkpoint(
                                            checkpoint.5,
                                            debug_role_for_folder_id(checkpoint.5),
                                            debug_container_class_for_folder_id(checkpoint.5),
                                            checkpoint.1.as_str(),
                                            checkpoint.4,
                                            "ok",
                                        );
                                    }
                                    Err(error) => {
                                        tracing::warn!(
                                            rca_debug = true,
                                            adapter = "mapi",
                                            endpoint = "emsmdb",
                                            mailbox = %principal.email,
                                            request_type = "Execute",
                                            request_rop_id = "0x4e",
                                            folder_id = format_args!("0x{:016x}", *folder_id),
                                            folder_role = debug_role_for_folder_id(*folder_id),
                                            folder_container_class =
                                                debug_container_class_for_folder_id(*folder_id),
                                            sync_type = format_args!("0x{:02x}", checkpoint.4),
                                            checkpoint_kind = checkpoint.1.as_str(),
                                            checkpoint_mailbox_id = checkpoint
                                                .0
                                                .map(|id| id.to_string())
                                                .unwrap_or_default(),
                                            checkpoint_change_sequence = checkpoint.2,
                                            checkpoint_modseq = checkpoint.3,
                                            sync_state_bytes = state.len(),
                                            upload_state_buffer_bytes = state_upload_buffer.len(),
                                            upload_state_client_bytes = *client_state_uploaded_bytes,
                                            incremental_transfer_available = incremental_transfer_buffer.is_some(),
                                            transfer_buffer_bytes = transfer_buffer.len(),
                                            transfer_position = *transfer_position,
                                            checkpoint_store_status = "error",
                                            checkpoint_skip_reason = "",
                                            error = %error,
                                            "rca debug mapi sync checkpoint store"
                                        );
                                        session.record_completed_sync_checkpoint(
                                            checkpoint.5,
                                            debug_role_for_folder_id(checkpoint.5),
                                            debug_container_class_for_folder_id(checkpoint.5),
                                            checkpoint.1.as_str(),
                                            checkpoint.4,
                                            "error",
                                        );
                                    }
                                }
                            }
                        }
                    }
                    _ => responses.extend_from_slice(&rop_error_response(
                        0x4E,
                        request.response_handle_index(),
                        0x8004_0102,
                    )),
                }
            }
            Some(RopId::TellVersion) => match input_object(session, &handle_slots, &request) {
                Some(MapiObject::SynchronizationSource { .. })
                | Some(MapiObject::SynchronizationCollector { .. }) => {
                    responses.extend_from_slice(&rop_simple_success_response(&request));
                }
                _ => responses.extend_from_slice(&rop_error_response(
                    0x86,
                    request.response_handle_index(),
                    0x8004_0102,
                )),
            },
            Some(RopId::SynchronizationUploadStateStreamBegin) => {
                match input_object_mut(session, &handle_slots, &request) {
                    Some(MapiObject::SynchronizationSource {
                        folder_id,
                        mailbox_id,
                        checkpoint_kind,
                        sync_type,
                        state_upload_property_tag,
                        state_upload_buffer,
                        ..
                    }) => {
                        let property_tag = request.upload_state_property_tag().unwrap_or_default();
                        let declared_bytes =
                            request.upload_state_transfer_size().unwrap_or_default();
                        *state_upload_property_tag = Some(property_tag);
                        state_upload_buffer.clear();
                        tracing::info!(
                            rca_debug = true,
                            adapter = "mapi",
                            endpoint = "emsmdb",
                            mailbox = %principal.email,
                            request_type = "Execute",
                            request_rop_id = "0x75",
                            sync_context_kind = "source",
                            folder_id = format_args!("0x{:016x}", *folder_id),
                            folder_role = debug_role_for_folder_id(*folder_id),
                            folder_container_class = debug_container_class_for_folder_id(*folder_id),
                            sync_type = format_args!("0x{:02x}", *sync_type),
                            checkpoint_kind = checkpoint_kind.as_str(),
                            checkpoint_mailbox_id = (*mailbox_id)
                                .map(|id| id.to_string())
                                .unwrap_or_default(),
                            upload_state_property_tag = format_args!("0x{property_tag:08x}"),
                            upload_state_property_name = upload_state_property_name(property_tag),
                            upload_state_declared_bytes = declared_bytes,
                            upload_state_empty_declared = declared_bytes == 0,
                            "rca debug mapi sync upload state begin"
                        );
                        responses.extend_from_slice(&rop_upload_state_success_response(&request));
                    }
                    Some(MapiObject::SynchronizationCollector {
                        folder_id,
                        mailbox_id,
                        checkpoint_kind,
                        state_upload_buffer,
                        ..
                    }) => {
                        let property_tag = request.upload_state_property_tag().unwrap_or_default();
                        let declared_bytes =
                            request.upload_state_transfer_size().unwrap_or_default();
                        state_upload_buffer.clear();
                        tracing::info!(
                            rca_debug = true,
                            adapter = "mapi",
                            endpoint = "emsmdb",
                            mailbox = %principal.email,
                            request_type = "Execute",
                            request_rop_id = "0x75",
                            sync_context_kind = "collector",
                            folder_id = format_args!("0x{:016x}", *folder_id),
                            folder_role = debug_role_for_folder_id(*folder_id),
                            folder_container_class = debug_container_class_for_folder_id(*folder_id),
                            checkpoint_kind = checkpoint_kind.as_str(),
                            checkpoint_mailbox_id = (*mailbox_id)
                                .map(|id| id.to_string())
                                .unwrap_or_default(),
                            upload_state_property_tag = format_args!("0x{property_tag:08x}"),
                            upload_state_property_name = upload_state_property_name(property_tag),
                            upload_state_declared_bytes = declared_bytes,
                            upload_state_empty_declared = declared_bytes == 0,
                            "rca debug mapi sync upload state begin"
                        );
                        responses.extend_from_slice(&rop_upload_state_success_response(&request));
                    }
                    _ => responses.extend_from_slice(&rop_error_response(
                        0x75,
                        request.response_handle_index(),
                        0x8004_0102,
                    )),
                }
            }
            Some(RopId::SynchronizationUploadStateStreamContinue) => {
                match input_object_mut(session, &handle_slots, &request) {
                    Some(MapiObject::SynchronizationSource {
                        folder_id,
                        mailbox_id,
                        checkpoint_kind,
                        sync_type,
                        state_upload_buffer,
                        ..
                    }) => {
                        let stream_data = request.stream_data();
                        state_upload_buffer.extend_from_slice(stream_data);
                        tracing::info!(
                            rca_debug = true,
                            adapter = "mapi",
                            endpoint = "emsmdb",
                            mailbox = %principal.email,
                            request_type = "Execute",
                            request_rop_id = "0x76",
                            sync_context_kind = "source",
                            folder_id = format_args!("0x{:016x}", *folder_id),
                            folder_role = debug_role_for_folder_id(*folder_id),
                            folder_container_class = debug_container_class_for_folder_id(*folder_id),
                            sync_type = format_args!("0x{:02x}", *sync_type),
                            checkpoint_kind = checkpoint_kind.as_str(),
                            checkpoint_mailbox_id = (*mailbox_id)
                                .map(|id| id.to_string())
                                .unwrap_or_default(),
                            upload_state_chunk_bytes = stream_data.len(),
                            upload_state_chunk_preview = %hex_preview(stream_data, 16),
                            upload_state_buffer_bytes = state_upload_buffer.len(),
                            "rca debug mapi sync upload state continue"
                        );
                        responses.extend_from_slice(&rop_upload_state_success_response(&request));
                    }
                    Some(MapiObject::SynchronizationCollector {
                        folder_id,
                        mailbox_id,
                        checkpoint_kind,
                        state_upload_buffer,
                        ..
                    }) => {
                        let stream_data = request.stream_data();
                        state_upload_buffer.extend_from_slice(stream_data);
                        tracing::info!(
                            rca_debug = true,
                            adapter = "mapi",
                            endpoint = "emsmdb",
                            mailbox = %principal.email,
                            request_type = "Execute",
                            request_rop_id = "0x76",
                            sync_context_kind = "collector",
                            folder_id = format_args!("0x{:016x}", *folder_id),
                            folder_role = debug_role_for_folder_id(*folder_id),
                            folder_container_class = debug_container_class_for_folder_id(*folder_id),
                            checkpoint_kind = checkpoint_kind.as_str(),
                            checkpoint_mailbox_id = (*mailbox_id)
                                .map(|id| id.to_string())
                                .unwrap_or_default(),
                            upload_state_chunk_bytes = stream_data.len(),
                            upload_state_chunk_preview = %hex_preview(stream_data, 16),
                            upload_state_buffer_bytes = state_upload_buffer.len(),
                            "rca debug mapi sync upload state continue"
                        );
                        responses.extend_from_slice(&rop_upload_state_success_response(&request));
                    }
                    _ => responses.extend_from_slice(&rop_error_response(
                        0x76,
                        request.response_handle_index(),
                        0x8004_0102,
                    )),
                }
            }
            Some(RopId::SynchronizationUploadStateStreamEnd) => {
                match input_object_mut(session, &handle_slots, &request) {
                    Some(MapiObject::SynchronizationSource {
                        folder_id,
                        mailbox_id,
                        checkpoint_kind,
                        sync_type,
                        state,
                        state_upload_property_tag,
                        state_upload_buffer,
                        client_state_uploaded_bytes,
                        client_state_uploaded_marker_mask,
                        incremental_transfer_buffer,
                        transfer_buffer,
                        transfer_position,
                        ..
                    }) => {
                        let uploaded_bytes = state_upload_buffer.len();
                        let upload_state_stream_summary = if uploaded_bytes == 0 {
                            "bytes=0;empty=true".to_string()
                        } else {
                            mapi_mailstore::replguid_globset_debug_summary(state_upload_buffer)
                        };
                        let property_tag = state_upload_property_tag.take().unwrap_or_default();
                        let upload_state_empty_stream_after_client_state =
                            uploaded_bytes == 0 && *client_state_uploaded_bytes > 0;
                        if uploaded_bytes > 0 {
                            mark_uploaded_state_stream(
                                client_state_uploaded_marker_mask,
                                property_tag,
                            );
                        }
                        state_upload_buffer.clear();
                        *client_state_uploaded_bytes =
                            (*client_state_uploaded_bytes).saturating_add(uploaded_bytes);
                        let mut selected_checkpoint_delta = false;
                        let has_delta_anchor =
                            uploaded_state_has_delta_anchor(*client_state_uploaded_marker_mask);
                        if has_delta_anchor {
                            if let Some(buffer) = incremental_transfer_buffer.take() {
                                *transfer_buffer = buffer;
                                *transfer_position = 0;
                                selected_checkpoint_delta = true;
                            }
                        }
                        tracing::info!(
                            rca_debug = true,
                            adapter = "mapi",
                            endpoint = "emsmdb",
                            mailbox = %principal.email,
                            request_type = "Execute",
                            request_rop_id = "0x77",
                            sync_context_kind = "source",
                            folder_id = format_args!("0x{:016x}", *folder_id),
                            folder_role = debug_role_for_folder_id(*folder_id),
                            folder_container_class = debug_container_class_for_folder_id(*folder_id),
                            sync_type = format_args!("0x{:02x}", *sync_type),
                            checkpoint_kind = checkpoint_kind.as_str(),
                            checkpoint_mailbox_id = (*mailbox_id)
                                .map(|id| id.to_string())
                                .unwrap_or_default(),
                            upload_state_total_bytes = state.len(),
                            upload_state_stream_bytes = uploaded_bytes,
                            upload_state_empty_stream = uploaded_bytes == 0,
                            upload_state_empty_stream_expected = uploaded_bytes == 0,
                            upload_state_empty_stream_after_client_state,
                            upload_state_property_tag = format_args!("0x{property_tag:08x}"),
                            upload_state_property_name = upload_state_property_name(property_tag),
                            upload_state_stream_summary = %upload_state_stream_summary,
                            upload_state_client_bytes = *client_state_uploaded_bytes,
                            upload_state_marker_mask =
                                format_args!("0x{:02x}", *client_state_uploaded_marker_mask),
                            upload_state_markers =
                                %uploaded_state_marker_summary(*client_state_uploaded_marker_mask),
                            upload_state_has_delta_anchor = has_delta_anchor,
                            upload_state_selected_checkpoint_delta = selected_checkpoint_delta,
                            transfer_buffer_bytes = transfer_buffer.len(),
                            transfer_position = *transfer_position,
                            "rca debug mapi sync upload state end"
                        );
                        responses.extend_from_slice(&rop_upload_state_success_response(&request));
                    }
                    Some(MapiObject::SynchronizationCollector {
                        folder_id,
                        mailbox_id,
                        checkpoint_kind,
                        state,
                        state_upload_buffer,
                        ..
                    }) => {
                        let uploaded_bytes = state_upload_buffer.len();
                        commit_uploaded_sync_state(state, state_upload_buffer);
                        tracing::info!(
                            rca_debug = true,
                            adapter = "mapi",
                            endpoint = "emsmdb",
                            mailbox = %principal.email,
                            request_type = "Execute",
                            request_rop_id = "0x77",
                            sync_context_kind = "collector",
                            folder_id = format_args!("0x{:016x}", *folder_id),
                            folder_role = debug_role_for_folder_id(*folder_id),
                            folder_container_class = debug_container_class_for_folder_id(*folder_id),
                            checkpoint_kind = checkpoint_kind.as_str(),
                            checkpoint_mailbox_id = (*mailbox_id)
                                .map(|id| id.to_string())
                                .unwrap_or_default(),
                            upload_state_total_bytes = state.len(),
                            upload_state_stream_bytes = uploaded_bytes,
                            upload_state_empty_stream = uploaded_bytes == 0,
                            "rca debug mapi sync upload state end"
                        );
                        responses.extend_from_slice(&rop_upload_state_success_response(&request));
                    }
                    _ => responses.extend_from_slice(&rop_error_response(
                        0x77,
                        request.response_handle_index(),
                        0x8004_0102,
                    )),
                }
            }
            Some(RopId::SynchronizationOpenCollector) => {
                let Some(folder_id) =
                    input_object(session, &handle_slots, &request).and_then(MapiObject::folder_id)
                else {
                    responses.extend_from_slice(&rop_error_response(
                        0x7E,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                let handle = session.allocate_output_handle(
                    request.output_handle_index,
                    MapiObject::SynchronizationCollector {
                        folder_id,
                        mailbox_id: sync_checkpoint_mailbox_id(folder_id, 0x01, mailboxes),
                        checkpoint_kind: MapiCheckpointKind::Content,
                        state: Vec::new(),
                        state_upload_buffer: Vec::new(),
                    },
                );
                set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                responses.extend_from_slice(&rop_simple_success_response(&request));
                output_handles.push(handle);
            }
            Some(RopId::SynchronizationGetTransferState) => {
                let Some((
                    folder_id,
                    mailbox_id,
                    checkpoint_kind,
                    checkpoint_change_sequence,
                    checkpoint_modseq,
                    checkpoint_store_allowed,
                    checkpoint_skip_reason,
                    sync_type,
                    state,
                )) = synchronization_context_state(input_object(session, &handle_slots, &request))
                else {
                    responses.extend_from_slice(&rop_error_response(
                        0x82,
                        request.response_handle_index(),
                        0x8004_0102,
                    ));
                    continue;
                };
                let transfer_buffer = if state.is_empty() && matches!(sync_type, 0x01 | 0x02) {
                    let sync_mailboxes = sync_mailboxes_for(folder_id, sync_type, mailboxes);
                    let sync_emails = sync_emails_for(folder_id, sync_type, mailboxes, emails);
                    let sync_attachment_facts =
                        sync_attachment_facts_for(folder_id, &sync_emails, snapshot);
                    mapi_mailstore::sync_state_token_with_attachments(
                        sync_type,
                        folder_id,
                        &sync_mailboxes,
                        &sync_emails,
                        &sync_attachment_facts,
                    )
                } else {
                    state
                };
                let handle = session.allocate_output_handle(
                    request.output_handle_index,
                    MapiObject::SynchronizationSource {
                        folder_id,
                        mailbox_id,
                        checkpoint_kind,
                        checkpoint_change_sequence,
                        checkpoint_modseq,
                        checkpoint_store_allowed,
                        checkpoint_skip_reason,
                        sync_type,
                        state: transfer_buffer.clone(),
                        state_upload_property_tag: None,
                        state_upload_buffer: Vec::new(),
                        client_state_uploaded_bytes: 0,
                        client_state_uploaded_marker_mask: 0,
                        incremental_transfer_buffer: None,
                        transfer_buffer,
                        transfer_position: 0,
                    },
                );
                set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                responses
                    .extend_from_slice(&rop_synchronization_get_transfer_state_response(&request));
                output_handles.push(handle);
            }
            Some(RopId::SynchronizationImportMessageChange) => {
                let Some(folder_id) =
                    input_object(session, &handle_slots, &request).and_then(MapiObject::folder_id)
                else {
                    responses.extend_from_slice(&rop_error_response(
                        0x72,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                let property_values = match request.import_property_values() {
                    Ok(values) => values,
                    Err(_) => {
                        responses.extend_from_slice(&rop_error_response(
                            0x72,
                            request.response_handle_index(),
                            0x8004_0102,
                        ));
                        continue;
                    }
                };
                let import_flag = request.import_flag().unwrap_or_default();
                let import_property_tags = property_values
                    .iter()
                    .map(|(tag, _)| format!("0x{tag:08x}"))
                    .collect::<Vec<_>>()
                    .join(",");
                let import_source_key = property_values
                    .iter()
                    .find_map(|(tag, value)| match (*tag, value) {
                        (PID_TAG_SOURCE_KEY, MapiValue::Binary(bytes)) => Some(bytes_to_hex(bytes)),
                        _ => None,
                    })
                    .unwrap_or_default();
                let import_source_key_global_counter =
                    imported_property_source_key_global_counter(&property_values);
                let import_source_key_identity_scope = import_source_key_global_counter
                    .map(import_source_key_identity_scope)
                    .unwrap_or("");
                let message_id = request.import_message_id().unwrap_or(0);
                tracing::info!(
                    rca_debug = true,
                    adapter = "mapi",
                    endpoint = "emsmdb",
                    mailbox = %principal.email,
                    request_type = "Execute",
                    request_rop_id = "0x72",
                    folder_id = format_args!("0x{:016x}", folder_id),
                    folder_role = debug_role_for_folder_id(folder_id),
                    folder_container_class = debug_container_class_for_folder_id(folder_id),
                    import_flag = format_args!("0x{import_flag:02x}"),
                    import_associated = import_flag & 0x10 != 0,
                    import_fail_on_conflict = import_flag & 0x40 != 0,
                    import_property_tag_count = property_values.len(),
                    import_property_tags = %import_property_tags,
                    import_source_key = %import_source_key,
                    import_source_key_global_counter = import_source_key_global_counter
                        .map(|counter| counter.to_string())
                        .unwrap_or_default(),
                    import_source_key_identity_scope,
                    parsed_message_id = format_args!("0x{message_id:016x}"),
                    "rca debug mapi sync import message change"
                );
                if import_flag & 0x10 != 0 && folder_id == COMMON_VIEWS_FOLDER_ID {
                    let properties = property_values.into_iter().collect::<HashMap<_, _>>();
                    let shortcut = navigation_shortcut_from_mapi_properties(
                        principal.account_id,
                        None,
                        &properties,
                    );
                    match store
                        .upsert_mapi_navigation_shortcut(UpsertMapiNavigationShortcutInput {
                            id: None,
                            account_id: principal.account_id,
                            subject: shortcut.subject,
                            target_folder_id: shortcut.target_folder_id,
                            shortcut_type: shortcut.shortcut_type,
                            flags: shortcut.flags,
                            section: shortcut.section,
                            ordinal: shortcut.ordinal,
                        })
                        .await
                    {
                        Ok(saved) => {
                            let shortcut_id = match remember_created_mapi_identity(
                                store,
                                principal,
                                MapiIdentityObjectKind::NavigationShortcut,
                                saved.id,
                                None,
                            )
                            .await
                            {
                                Ok(shortcut_id) => shortcut_id,
                                Err(_) => {
                                    responses.extend_from_slice(&rop_error_response(
                                        0x72,
                                        request.response_handle_index(),
                                        0x8004_010F,
                                    ));
                                    continue;
                                }
                            };
                            let handle = session.allocate_output_handle(
                                request.output_handle_index,
                                MapiObject::NavigationShortcut {
                                    folder_id,
                                    shortcut_id,
                                },
                            );
                            set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                            responses.extend_from_slice(
                                &rop_synchronization_import_message_change_response(&request),
                            );
                            output_handles.push(handle);
                        }
                        Err(_) => responses.extend_from_slice(&rop_error_response(
                            0x72,
                            request.response_handle_index(),
                            0x8004_010F,
                        )),
                    }
                    continue;
                }
                if message_id != 0
                    && message_for_id(folder_id, message_id, mailboxes, emails).is_some()
                {
                    if apply_canonical_message_property_values(
                        store,
                        principal,
                        folder_id,
                        message_id,
                        property_values,
                        mailboxes,
                        emails,
                    )
                    .await
                    .is_err()
                    {
                        responses.extend_from_slice(&rop_error_response(
                            0x72,
                            request.response_handle_index(),
                            0x8004_0102,
                        ));
                        continue;
                    }
                    let handle = session.allocate_output_handle(
                        request.output_handle_index,
                        MapiObject::Message {
                            folder_id,
                            message_id,
                        },
                    );
                    set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                    responses.extend_from_slice(
                        &rop_synchronization_import_message_change_response(&request),
                    );
                    output_handles.push(handle);
                } else if message_id != 0 && snapshot.note_for_id(folder_id, message_id).is_some() {
                    if apply_canonical_note_property_values(
                        store,
                        principal,
                        folder_id,
                        message_id,
                        property_values,
                        snapshot,
                    )
                    .await
                    .is_err()
                    {
                        responses.extend_from_slice(&rop_error_response(
                            0x72,
                            request.response_handle_index(),
                            0x8004_0102,
                        ));
                        continue;
                    }
                    let handle = session.allocate_output_handle(
                        request.output_handle_index,
                        MapiObject::Note {
                            folder_id,
                            note_id: message_id,
                        },
                    );
                    set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                    responses.extend_from_slice(
                        &rop_synchronization_import_message_change_response(&request),
                    );
                    output_handles.push(handle);
                } else if message_id != 0
                    && snapshot
                        .journal_entry_for_id(folder_id, message_id)
                        .is_some()
                {
                    if apply_canonical_journal_entry_property_values(
                        store,
                        principal,
                        folder_id,
                        message_id,
                        property_values,
                        snapshot,
                    )
                    .await
                    .is_err()
                    {
                        responses.extend_from_slice(&rop_error_response(
                            0x72,
                            request.response_handle_index(),
                            0x8004_0102,
                        ));
                        continue;
                    }
                    let handle = session.allocate_output_handle(
                        request.output_handle_index,
                        MapiObject::JournalEntry {
                            folder_id,
                            journal_entry_id: message_id,
                        },
                    );
                    set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                    responses.extend_from_slice(
                        &rop_synchronization_import_message_change_response(&request),
                    );
                    output_handles.push(handle);
                } else {
                    let pending_object = match folder_id {
                        NOTES_FOLDER_ID => MapiObject::PendingNote {
                            folder_id,
                            properties: property_values.into_iter().collect(),
                        },
                        JOURNAL_FOLDER_ID => MapiObject::PendingJournalEntry {
                            folder_id,
                            properties: property_values.into_iter().collect(),
                        },
                        _ => MapiObject::PendingMessage {
                            folder_id,
                            properties: property_values.into_iter().collect(),
                            recipients: Vec::new(),
                        },
                    };
                    let handle =
                        session.allocate_output_handle(request.output_handle_index, pending_object);
                    set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                    responses.extend_from_slice(
                        &rop_synchronization_import_message_change_response(&request),
                    );
                    output_handles.push(handle);
                }
            }
            Some(RopId::SynchronizationImportHierarchyChange) => {
                let Some(_folder_id) =
                    input_object(session, &handle_slots, &request).and_then(MapiObject::folder_id)
                else {
                    responses.extend_from_slice(&rop_error_response(
                        0x73,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                let (hierarchy_values, property_values) = match request.import_hierarchy_values() {
                    Ok(values) => values,
                    Err(_) => {
                        responses.extend_from_slice(&rop_error_response(
                            0x73,
                            request.response_handle_index(),
                            0x8004_0102,
                        ));
                        continue;
                    }
                };
                let display_name = hierarchy_display_name(&hierarchy_values, &property_values);
                let Some(display_name) = display_name else {
                    responses.extend_from_slice(&rop_error_response(
                        0x73,
                        request.response_handle_index(),
                        0x8004_0102,
                    ));
                    continue;
                };
                if system_folder_display_name(&display_name) {
                    responses.extend_from_slice(&rop_error_response(
                        0x73,
                        request.response_handle_index(),
                        0x8004_0102,
                    ));
                    continue;
                }
                if let Some(existing) =
                    imported_hierarchy_existing_mailbox(&hierarchy_values, &display_name, mailboxes)
                {
                    if existing.role == "custom"
                        && existing.name.eq_ignore_ascii_case(&display_name)
                    {
                        responses.extend_from_slice(
                            &rop_synchronization_import_hierarchy_change_response(&request),
                        );
                    } else {
                        responses.extend_from_slice(&rop_error_response(
                            0x73,
                            request.response_handle_index(),
                            0x8004_0102,
                        ));
                    }
                    continue;
                }

                match store
                    .create_jmap_mailbox(
                        JmapMailboxCreateInput {
                            account_id: principal.account_id,
                            name: display_name.clone(),
                            parent_id: None,
                            sort_order: None,
                            is_subscribed: true,
                        },
                        AuditEntryInput {
                            actor: principal.email.clone(),
                            action: "mapi-sync-import-hierarchy-change".to_string(),
                            subject: display_name.clone(),
                        },
                    )
                    .await
                {
                    Ok(mailbox) => {
                        match remember_created_mapi_identity(
                            store,
                            principal,
                            MapiIdentityObjectKind::Mailbox,
                            mailbox.id,
                            None,
                        )
                        .await
                        {
                            Ok(_) => {}
                            Err(_) => {
                                responses.extend_from_slice(&rop_error_response(
                                    0x73,
                                    request.response_handle_index(),
                                    0x8004_0102,
                                ));
                                continue;
                            }
                        };
                        responses.extend_from_slice(
                            &rop_synchronization_import_hierarchy_change_response(&request),
                        );
                    }
                    Err(_) => responses.extend_from_slice(&rop_error_response(
                        0x73,
                        request.response_handle_index(),
                        0x8004_0102,
                    )),
                }
            }
            Some(RopId::SynchronizationImportDeletes) => {
                let Some(folder_id) =
                    input_object(session, &handle_slots, &request).and_then(MapiObject::folder_id)
                else {
                    responses.extend_from_slice(&rop_error_response(
                        0x74,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                let mut partial_completion = false;
                let hard_delete = request.import_delete_hard_delete();
                for message_id in request.import_delete_message_ids() {
                    if let Some(note) = snapshot.note_for_id(folder_id, message_id) {
                        if store
                            .delete_mapi_note(principal.account_id, note.canonical_id)
                            .await
                            .is_err()
                        {
                            partial_completion = true;
                        }
                        continue;
                    }
                    if let Some(entry) = snapshot.journal_entry_for_id(folder_id, message_id) {
                        if store
                            .delete_mapi_journal_entry(principal.account_id, entry.canonical_id)
                            .await
                            .is_err()
                        {
                            partial_completion = true;
                        }
                        continue;
                    }
                    let Some(email) = message_for_id(folder_id, message_id, mailboxes, emails)
                    else {
                        partial_completion = true;
                        continue;
                    };
                    let result = if hard_delete || email.mailbox_role == "trash" {
                        store
                            .delete_jmap_email_from_mailbox(
                                principal.account_id,
                                email.mailbox_id,
                                email.id,
                                AuditEntryInput {
                                    actor: principal.email.clone(),
                                    action: "mapi-sync-import-hard-delete".to_string(),
                                    subject: format!("message:{}", email.id),
                                },
                            )
                            .await
                            .map(|_| ())
                    } else if let Some(trash_mailbox) =
                        mailboxes.iter().find(|mailbox| mailbox.role == "trash")
                    {
                        store
                            .move_jmap_email_from_mailbox(
                                principal.account_id,
                                email.mailbox_id,
                                email.id,
                                trash_mailbox.id,
                                AuditEntryInput {
                                    actor: principal.email.clone(),
                                    action: "mapi-sync-import-soft-delete".to_string(),
                                    subject: format!("message:{}->{}", email.id, trash_mailbox.id),
                                },
                            )
                            .await
                            .map(|_| ())
                    } else {
                        store
                            .delete_jmap_email_from_mailbox(
                                principal.account_id,
                                email.mailbox_id,
                                email.id,
                                AuditEntryInput {
                                    actor: principal.email.clone(),
                                    action: "mapi-sync-import-delete-without-trash".to_string(),
                                    subject: format!("message:{}", email.id),
                                },
                            )
                            .await
                            .map(|_| ())
                    };
                    if result.is_err() {
                        partial_completion = true;
                    }
                }
                responses.extend_from_slice(&rop_partial_completion_response(
                    0x74,
                    request.response_handle_index(),
                    partial_completion,
                ));
            }
            Some(RopId::SynchronizationImportMessageMove) => {
                let Some((message_id, target_folder_id)) = request.import_move() else {
                    responses.extend_from_slice(&rop_error_response(
                        0x78,
                        request.response_handle_index(),
                        0x8004_0102,
                    ));
                    continue;
                };
                let source_folder_id = input_object(session, &handle_slots, &request)
                    .and_then(MapiObject::folder_id)
                    .unwrap_or(INBOX_FOLDER_ID);
                if snapshot.note_for_id(source_folder_id, message_id).is_some() {
                    if target_folder_id == NOTES_FOLDER_ID {
                        responses.extend_from_slice(
                            &rop_synchronization_import_message_move_response(&request),
                        );
                    } else {
                        responses.extend_from_slice(&rop_error_response(
                            0x78,
                            request.response_handle_index(),
                            0x8004_010F,
                        ));
                    }
                    continue;
                }
                if snapshot
                    .journal_entry_for_id(source_folder_id, message_id)
                    .is_some()
                {
                    if target_folder_id == JOURNAL_FOLDER_ID {
                        responses.extend_from_slice(
                            &rop_synchronization_import_message_move_response(&request),
                        );
                    } else {
                        responses.extend_from_slice(&rop_error_response(
                            0x78,
                            request.response_handle_index(),
                            0x8004_010F,
                        ));
                    }
                    continue;
                }
                let Some(email) = message_for_id(source_folder_id, message_id, mailboxes, emails)
                else {
                    responses.extend_from_slice(&rop_error_response(
                        0x78,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                let Some(target_mailbox) = folder_row_for_id(target_folder_id, mailboxes) else {
                    responses.extend_from_slice(&rop_error_response(
                        0x78,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                };
                match store
                    .move_jmap_email_from_mailbox(
                        principal.account_id,
                        email.mailbox_id,
                        email.id,
                        target_mailbox.id,
                        AuditEntryInput {
                            actor: principal.email.clone(),
                            action: "mapi-sync-import-move".to_string(),
                            subject: format!("message:{}->{}", email.id, target_mailbox.id),
                        },
                    )
                    .await
                {
                    Ok(moved) => {
                        match remember_created_mapi_identity(
                            store,
                            principal,
                            MapiIdentityObjectKind::Message,
                            moved.id,
                            None,
                        )
                        .await
                        {
                            Ok(_) => {}
                            Err(_) => {
                                responses.extend_from_slice(&rop_error_response(
                                    0x78,
                                    request.response_handle_index(),
                                    0x8004_010F,
                                ));
                                continue;
                            }
                        };
                        responses.extend_from_slice(
                            &rop_synchronization_import_message_move_response(&request),
                        );
                    }
                    Err(_) => responses.extend_from_slice(&rop_error_response(
                        0x78,
                        request.response_handle_index(),
                        0x8004_010F,
                    )),
                }
            }
            Some(RopId::SynchronizationImportReadStateChanges) => {
                let folder_id = input_object(session, &handle_slots, &request)
                    .and_then(MapiObject::folder_id)
                    .unwrap_or(INBOX_FOLDER_ID);
                let mut partial_completion = false;
                for (message_id, unread) in request.import_read_state_changes() {
                    let Some(email) = message_for_id(folder_id, message_id, mailboxes, emails)
                    else {
                        partial_completion = true;
                        continue;
                    };
                    if store
                        .update_jmap_email_flags(
                            principal.account_id,
                            email.id,
                            Some(unread),
                            None,
                            AuditEntryInput {
                                actor: principal.email.clone(),
                                action: "mapi-sync-import-read-state".to_string(),
                                subject: format!("message:{}", email.id),
                            },
                        )
                        .await
                        .is_err()
                    {
                        partial_completion = true;
                    }
                }
                responses.extend_from_slice(&rop_partial_completion_response(
                    0x80,
                    request.response_handle_index(),
                    partial_completion,
                ));
            }
            Some(RopId::SetLocalReplicaMidsetDeleted) => {
                match input_object_mut(session, &handle_slots, &request) {
                    Some(MapiObject::SynchronizationSource { state, .. })
                    | Some(MapiObject::SynchronizationCollector { state, .. }) => {
                        state.extend_from_slice(request.local_replica_midset_deleted());
                        responses.extend_from_slice(&rop_simple_success_response(&request));
                    }
                    _ => responses.extend_from_slice(&rop_error_response(
                        0x93,
                        request.response_handle_index(),
                        0x8004_0102,
                    )),
                }
            }
            Some(RopId::GetLocalReplicaIds) => {
                echo_input_handle_table = true;
                let (first_global_counter, _) = mapi_mailstore::local_replica_id_range(
                    principal.account_id,
                    request.local_replica_id_count(),
                    session.next_local_replica_sequence,
                );
                session.next_local_replica_sequence =
                    session.next_local_replica_sequence.saturating_add(1).max(1);
                responses.extend_from_slice(&rop_get_local_replica_ids_response(
                    &request,
                    first_global_counter,
                ));
            }
            Some(RopId::EmptyFolder | RopId::HardDeleteMessagesAndSubfolders) => {
                let Some(folder_id) =
                    input_object(session, &handle_slots, &request).and_then(MapiObject::folder_id)
                else {
                    responses.extend_from_slice(&rop_error_response(
                        request.rop_id,
                        request.response_handle_index(),
                        0x0000_04B9,
                    ));
                    continue;
                };

                match hard_delete_folder_contents(
                    store, principal, folder_id, mailboxes, emails, snapshot,
                )
                .await
                {
                    Ok((deleted_any, partial_completion)) => {
                        if deleted_any {
                            session.record_notification(MapiNotificationEvent::content(
                                folder_id, None,
                            ));
                        }
                        responses.extend_from_slice(&rop_partial_completion_response(
                            request.rop_id,
                            request.response_handle_index(),
                            partial_completion,
                        ));
                    }
                    Err(error) => responses.extend_from_slice(&rop_error_response(
                        request.rop_id,
                        request.response_handle_index(),
                        error,
                    )),
                }
            }
            Some(RopId::GetTransportFolder) => {
                responses.extend_from_slice(&rop_get_transport_folder_response(&request))
            }
            Some(RopId::OptionsData) => {
                responses.extend_from_slice(&rop_options_data_response(&request))
            }
            Some(RopId::GetReceiveFolderTable) => {
                responses.extend_from_slice(&rop_get_receive_folder_table_response(&request))
            }
            Some(RopId::LongTermIdFromId) => {
                let source_id_bytes = request
                    .long_term_source_id_bytes()
                    .map(bytes_to_hex)
                    .unwrap_or_default();
                let decoded_object_id = request.long_term_source_object_id();
                let decoded_object_scope =
                    debug_object_scope_for_id(decoded_object_id, mailboxes, emails, snapshot);
                let response = rop_long_term_id_from_id_response_for_scope(
                    &request,
                    decoded_object_id,
                    decoded_object_scope,
                );
                let response_status = if response.len() > 6 {
                    "ok"
                } else {
                    "ecNotFound"
                };
                tracing::info!(
                    rca_debug = true,
                    adapter = "mapi",
                    endpoint = "emsmdb",
                    mailbox = %principal.email,
                    request_type = "Execute",
                    request_rop_id = "0x43",
                    source_id_bytes = %source_id_bytes,
                    decoded_object_id = decoded_object_id
                        .map(|object_id| format!("{object_id:#018x}"))
                        .unwrap_or_default(),
                    decoded_advertised_special_folder = decoded_object_id
                        .map(is_advertised_special_folder)
                        .unwrap_or(false),
                    decoded_object_scope,
                    response_status,
                    message = "rca debug mapi long term id from id",
                );
                responses.extend_from_slice(&response)
            }
            Some(RopId::IdFromLongTermId) => {
                let replica_guid_aliases = [
                    *principal.account_id.as_bytes(),
                    principal.account_id.to_bytes_le(),
                ];
                responses.extend_from_slice(&rop_id_from_long_term_id_response(
                    &request,
                    &replica_guid_aliases,
                ))
            }
            Some(RopId::PublicFolderIsGhosted) => {
                responses.extend_from_slice(&rop_public_folder_is_ghosted_response(&request))
            }
            Some(RopId::GetAddressTypes) => {
                echo_input_handle_table = true;
                responses.extend_from_slice(&rop_get_address_types_response(&request));
            }
            Some(RopId::GetNamesFromPropertyIds) => {
                let property_ids = request.property_ids();
                let missing_property_ids = property_ids
                    .iter()
                    .copied()
                    .filter(|property_id| !session.named_property_ids.contains_key(property_id))
                    .collect::<Vec<_>>();
                if !missing_property_ids.is_empty() {
                    if let Ok(mappings) = store
                        .fetch_mapi_named_properties_by_ids(
                            principal.account_id,
                            &missing_property_ids,
                        )
                        .await
                    {
                        for mapping in mappings {
                            session.cache_named_property(mapping.property_id, mapping.property);
                        }
                    }
                }
                responses.extend_from_slice(&rop_get_names_from_property_ids_response(
                    &request, session,
                ));
            }
            Some(RopId::GetPropertyIdsFromNames) => {
                echo_input_handle_table = true;
                let properties = match request.named_property_names() {
                    Ok(properties) => properties,
                    Err(_) => {
                        responses.extend_from_slice(&rop_error_response(
                            0x56,
                            request.response_handle_index(),
                            0x8004_0102,
                        ));
                        continue;
                    }
                };
                if properties.is_empty()
                    && matches!(
                        input_object(session, &handle_slots, &request),
                        Some(MapiObject::Logon)
                    )
                {
                    if let Ok(mappings) = store
                        .fetch_mapi_named_properties(principal.account_id, None)
                        .await
                    {
                        for mapping in mappings {
                            session.cache_named_property(mapping.property_id, mapping.property);
                        }
                    }
                    let property_ids = session
                        .named_properties_for_query(None)
                        .into_iter()
                        .map(|(property_id, _property)| property_id)
                        .collect::<Vec<_>>();
                    responses.extend_from_slice(&rop_get_property_ids_from_names_response(
                        &request,
                        &property_ids,
                    ));
                    continue;
                }
                let mut property_ids = Vec::with_capacity(properties.len());
                let mut missing = Vec::new();
                for (index, property) in properties.into_iter().enumerate() {
                    match session.property_id_for_name(property.clone(), false) {
                        Some(property_id) => property_ids.push(property_id),
                        None => {
                            property_ids.push(0);
                            missing.push((index, property));
                        }
                    }
                }
                if !missing.is_empty() {
                    let missing_properties = missing
                        .iter()
                        .map(|(_index, property)| property.clone())
                        .collect::<Vec<_>>();
                    match store
                        .fetch_or_allocate_mapi_named_property_ids(
                            principal.account_id,
                            &missing_properties,
                            request.named_property_create(),
                        )
                        .await
                    {
                        Ok(mappings) => {
                            for (missing_index, (index, property)) in
                                missing.into_iter().enumerate()
                            {
                                let mapping = mappings.get(missing_index).cloned().flatten();
                                let property_id = mapping
                                    .map(|mapping| {
                                        session.cache_named_property(
                                            mapping.property_id,
                                            mapping.property,
                                        );
                                        mapping.property_id
                                    })
                                    .or_else(|| {
                                        session.property_id_for_name(
                                            property,
                                            request.named_property_create(),
                                        )
                                    });
                                property_ids[index] = property_id.unwrap_or(0);
                            }
                        }
                        Err(_) if request.named_property_create() => {
                            responses.extend_from_slice(&rop_error_response(
                                0x56,
                                request.response_handle_index(),
                                0x8007_000E,
                            ));
                            continue;
                        }
                        Err(_) => {}
                    }
                }
                responses.extend_from_slice(&rop_get_property_ids_from_names_response(
                    &request,
                    &property_ids,
                ));
            }
            Some(RopId::QueryNamedProperties) => {
                if let Ok(mappings) = store
                    .fetch_mapi_named_properties(
                        principal.account_id,
                        request.named_property_query_guid(),
                    )
                    .await
                {
                    for mapping in mappings {
                        session.cache_named_property(mapping.property_id, mapping.property);
                    }
                }
                responses.extend_from_slice(&rop_query_named_properties_response(&request, session))
            }
            Some(RopId::RegisterNotification) => {
                let notification_types = request.notification_types().unwrap_or(0);
                if !supported_notification_types(notification_types) {
                    responses.extend_from_slice(&unsupported_rop_response(
                        0x29,
                        request.response_handle_index(),
                    ));
                    continue;
                }
                let registration = notification_registration_from_request(&request);
                if session.notification_cursor.is_none() {
                    session.notification_cursor = store
                        .fetch_mapi_notification_cursor(principal.account_id)
                        .await
                        .ok()
                        .flatten();
                }
                let handle = session.allocate_output_handle(
                    request.output_handle_index,
                    MapiObject::NotificationSubscription { registration },
                );
                set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                responses.extend_from_slice(&rop_register_notification_response(&request));
                output_handles.push(handle);
            }
            Some(RopId::GetPermissionsTable) => {
                let Some(folder_id) =
                    input_object(session, &handle_slots, &request).and_then(MapiObject::folder_id)
                else {
                    responses.extend_from_slice(&rop_handle_index_error_response(&request));
                    continue;
                };
                if folder_row_for_id(folder_id, mailboxes).is_none()
                    && role_for_folder_id(folder_id).is_none()
                {
                    responses.extend_from_slice(&rop_error_response(
                        0x3E,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                }
                let handle = session.allocate_output_handle(
                    request.output_handle_index,
                    MapiObject::PermissionTable {
                        folder_id,
                        columns: default_permission_columns(),
                        position: 0,
                    },
                );
                set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                responses.extend_from_slice(&rop_get_permissions_table_response(&request));
                output_handles.push(handle);
            }
            Some(RopId::GetRulesTable) => {
                let Some(folder_id) =
                    input_object(session, &handle_slots, &request).and_then(MapiObject::folder_id)
                else {
                    responses.extend_from_slice(&rop_handle_index_error_response(&request));
                    continue;
                };
                if folder_row_for_id(folder_id, mailboxes).is_none()
                    && role_for_folder_id(folder_id).is_none()
                {
                    responses.extend_from_slice(&rop_error_response(
                        0x3F,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    continue;
                }
                let handle = session.allocate_output_handle(
                    request.output_handle_index,
                    MapiObject::RuleTable {
                        folder_id,
                        columns: default_rule_columns(),
                        position: 0,
                    },
                );
                set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                responses.extend_from_slice(&rop_get_rules_table_response(&request));
                output_handles.push(handle);
            }
            Some(RopId::ModifyPermissions) => {
                responses.extend_from_slice(&rop_modify_permissions_response(&request))
            }
            Some(RopId::GetStoreState) => {
                responses.extend_from_slice(&rop_get_store_state_response(&request))
            }
            Some(RopId::ResetTable) => {
                if input_object_mut(session, &handle_slots, &request)
                    .is_some_and(reset_table_position)
                {
                    responses.extend_from_slice(&rop_reset_table_response(&request));
                } else {
                    responses.extend_from_slice(&rop_error_response(
                        0x81,
                        request.response_handle_index(),
                        0x8004_0102,
                    ));
                }
            }
            Some(RopId::FreeBookmark) => responses.extend_from_slice(&rop_free_bookmark_response(
                &request,
                input_object_mut(session, &handle_slots, &request),
            )),
            Some(RopId::Logon) => {
                if request.payload.first().copied().unwrap_or(0) & 0x01 == 0 {
                    responses.extend_from_slice(&unsupported_rop_response(
                        0xFE,
                        request.response_handle_index(),
                    ));
                    break;
                }
                let handle =
                    session.allocate_output_handle(request.output_handle_index, MapiObject::Logon);
                set_handle_slot(&mut handle_slots, request.output_handle_index, handle);
                responses.extend_from_slice(&rop_logon_response_body(principal, &request));
                output_handles.push(handle);
            }
            Some(rop_id) => responses.extend_from_slice(&unsupported_rop_response(
                rop_id.as_u8(),
                request.response_handle_index(),
            )),
            None => {
                responses.extend_from_slice(&unsupported_rop_response(
                    request.rop_id,
                    request.response_handle_index(),
                ));
                break;
            }
        }
        if let Some((sync_root_folder_id, get_buffer_summary)) = completed_hierarchy_sync {
            session.record_completed_hierarchy_sync(sync_root_folder_id, get_buffer_summary);
        }
        if content_sync_configure_observed {
            session.record_content_sync_configure();
        }
        if typed_request.unsupported_is_terminal() {
            break;
        }
    }
    if !post_hierarchy_release_events.is_empty() {
        let post_hierarchy = post_hierarchy_action_summary(session, false);
        tracing::info!(
            rca_debug = true,
            adapter = "mapi",
            endpoint = "emsmdb",
            tenant_id = %principal.tenant_id,
            account_id = %principal.account_id,
            mailbox = %principal.email,
            request_type = "Execute",
            last_completed_hierarchy_sync_root =
                %post_hierarchy.last_completed_hierarchy_sync_root,
            content_sync_started_after_hierarchy =
                post_hierarchy.content_sync_configure_observed,
            released_handle_count = post_hierarchy_release_events.len(),
            released_handle_kinds =
                %format_post_hierarchy_release_kinds(&post_hierarchy_release_events),
            released_logon_before_content_sync = post_hierarchy_release_events
                .iter()
                .any(|event| event.logon_before_content_sync),
            remaining_live_handle_count = session.handles.len(),
            release_context =
                %format_post_hierarchy_release_context(&post_hierarchy_release_events),
            "rca debug mapi post hierarchy close reason context"
        );
    }
    let response_handles =
        response_handle_table(&handle_slots, &output_handles, echo_input_handle_table);
    let response = if extended {
        rop_buffer_with_response_spec(responses, &response_handles)
    } else {
        rop_buffer_with_response(responses, &response_handles)
    };
    if extended {
        rpc_header_ext_rop_buffer(response)
    } else {
        response
    }
}

fn property_tags_are_supported(property_tags: &[u32]) -> bool {
    property_tags.iter().all(|tag| {
        let property_type = (*tag & 0xFFFF) as u16;
        property_type == 0 || MapiPropertyType::from_code(property_type).is_some()
    })
}

fn first_fast_transfer_marker(request: &RopRequest) -> Option<u32> {
    let size = u16::from_le_bytes(request.payload.get(..2)?.try_into().ok()?) as usize;
    let bytes = request.payload.get(2..2 + size)?;
    let marker = u32::from_le_bytes(bytes.get(..4)?.try_into().ok()?);
    (marker & 0x4000_0000 != 0).then_some(marker)
}

async fn mapi_submit_attachments_from_email<S>(
    store: &S,
    account_id: Uuid,
    email: &JmapEmail,
) -> Result<Vec<AttachmentUploadInput>>
where
    S: ExchangeStore,
{
    if !email.has_attachments {
        return Ok(Vec::new());
    }

    let attachments = store
        .fetch_message_attachments(account_id, email.id)
        .await?;
    let mut uploads = Vec::with_capacity(attachments.len());
    for attachment in attachments {
        let Some(content) = store
            .fetch_attachment_content(account_id, &attachment.file_reference)
            .await?
        else {
            return Err(anyhow::anyhow!(
                "missing attachment content for {}",
                attachment.file_reference
            ));
        };
        uploads.push(AttachmentUploadInput {
            file_name: content.file_name,
            media_type: content.media_type,
            disposition: None,
            content_id: None,
            blob_bytes: content.blob_bytes,
        });
    }
    Ok(uploads)
}

async fn mapi_message_ids_for_deleted_changes<S>(
    store: &S,
    principal: &AccountPrincipal,
    message_ids: &[Uuid],
) -> Result<Vec<u64>>
where
    S: ExchangeStore,
{
    mapi_object_ids_for_deleted_changes(
        store,
        principal,
        MapiIdentityObjectKind::Message,
        message_ids,
    )
    .await
}

async fn mapi_object_ids_for_deleted_changes<S>(
    store: &S,
    principal: &AccountPrincipal,
    object_kind: MapiIdentityObjectKind,
    object_ids: &[Uuid],
) -> Result<Vec<u64>>
where
    S: ExchangeStore,
{
    let requests = object_ids
        .iter()
        .map(|object_id| MapiIdentityRequest {
            object_kind,
            canonical_id: *object_id,
            reserved_global_counter: None,
        })
        .collect::<Vec<_>>();
    let identities = store
        .fetch_or_allocate_mapi_identities(principal.account_id, &requests)
        .await?;
    for identity in &identities {
        crate::mapi::identity::remember_mapi_identity(identity.canonical_id, identity.object_id);
    }
    Ok(identities
        .into_iter()
        .map(|identity| identity.object_id)
        .collect())
}

fn changed_special_ids_for_folder<'a>(
    folder_id: u64,
    snapshot: &MapiMailStoreSnapshot,
    changes: &'a MapiSyncChangeSet,
) -> &'a [Uuid] {
    if snapshot
        .collaboration_folder_for_id(folder_id)
        .is_some_and(|folder| folder.kind == MapiCollaborationFolderKind::Contacts)
        || matches!(
            folder_id,
            CONTACTS_SEARCH_FOLDER_ID
                | SUGGESTED_CONTACTS_FOLDER_ID
                | QUICK_CONTACTS_FOLDER_ID
                | IM_CONTACT_LIST_FOLDER_ID
        )
    {
        return &changes.changed_contact_ids;
    }
    if snapshot
        .collaboration_folder_for_id(folder_id)
        .is_some_and(|folder| folder.kind == MapiCollaborationFolderKind::Calendar)
    {
        return &changes.changed_calendar_event_ids;
    }
    if snapshot
        .collaboration_folder_for_id(folder_id)
        .is_some_and(|folder| folder.kind == MapiCollaborationFolderKind::Task)
        || matches!(folder_id, TODO_SEARCH_FOLDER_ID | REMINDERS_FOLDER_ID)
    {
        return &changes.changed_task_ids;
    }
    match folder_id {
        NOTES_FOLDER_ID => &changes.changed_note_ids,
        JOURNAL_FOLDER_ID => &changes.changed_journal_entry_ids,
        CONVERSATION_ACTION_SETTINGS_FOLDER_ID => &changes.changed_conversation_action_ids,
        _ => &[],
    }
}

async fn deleted_special_object_ids_for_folder<S>(
    store: &S,
    principal: &AccountPrincipal,
    folder_id: u64,
    snapshot: &MapiMailStoreSnapshot,
    changes: &MapiSyncChangeSet,
) -> Vec<u64>
where
    S: ExchangeStore,
{
    let kind_and_ids = if snapshot
        .collaboration_folder_for_id(folder_id)
        .is_some_and(|folder| folder.kind == MapiCollaborationFolderKind::Contacts)
        || matches!(
            folder_id,
            CONTACTS_SEARCH_FOLDER_ID
                | SUGGESTED_CONTACTS_FOLDER_ID
                | QUICK_CONTACTS_FOLDER_ID
                | IM_CONTACT_LIST_FOLDER_ID
        ) {
        Some((
            MapiIdentityObjectKind::Contact,
            changes.deleted_contact_ids.as_slice(),
        ))
    } else if snapshot
        .collaboration_folder_for_id(folder_id)
        .is_some_and(|folder| folder.kind == MapiCollaborationFolderKind::Calendar)
    {
        Some((
            MapiIdentityObjectKind::CalendarEvent,
            changes.deleted_calendar_event_ids.as_slice(),
        ))
    } else if snapshot
        .collaboration_folder_for_id(folder_id)
        .is_some_and(|folder| folder.kind == MapiCollaborationFolderKind::Task)
        || matches!(folder_id, TODO_SEARCH_FOLDER_ID | REMINDERS_FOLDER_ID)
    {
        Some((
            MapiIdentityObjectKind::Task,
            changes.deleted_task_ids.as_slice(),
        ))
    } else {
        None
    };
    let Some((object_kind, object_ids)) = kind_and_ids else {
        return Vec::new();
    };
    mapi_object_ids_for_deleted_changes(store, principal, object_kind, object_ids)
        .await
        .unwrap_or_default()
}

async fn remember_created_mapi_identity<S>(
    store: &S,
    principal: &AccountPrincipal,
    object_kind: MapiIdentityObjectKind,
    canonical_id: Uuid,
    reserved_global_counter: Option<u64>,
) -> Result<u64>
where
    S: ExchangeStore,
{
    let requests = [MapiIdentityRequest {
        object_kind,
        canonical_id,
        reserved_global_counter,
    }];
    let records = store
        .fetch_or_allocate_mapi_identities(principal.account_id, &requests)
        .await?;
    let object_id = records
        .first()
        .map(|record| record.object_id)
        .ok_or_else(|| anyhow::anyhow!("MAPI identity allocator returned no record"))?;
    crate::mapi::identity::remember_mapi_identity(canonical_id, object_id);
    Ok(object_id)
}

async fn remember_created_message_mapi_identity<S>(
    store: &S,
    principal: &AccountPrincipal,
    canonical_id: Uuid,
    reserved_global_counter: Option<u64>,
) -> Result<(u64, bool, String)>
where
    S: ExchangeStore,
{
    if reserved_global_counter.is_none() {
        let object_id = remember_created_mapi_identity(
            store,
            principal,
            MapiIdentityObjectKind::Message,
            canonical_id,
            None,
        )
        .await?;
        return Ok((object_id, false, String::new()));
    }

    match remember_created_mapi_identity(
        store,
        principal,
        MapiIdentityObjectKind::Message,
        canonical_id,
        reserved_global_counter,
    )
    .await
    {
        Ok(object_id) => Ok((object_id, true, String::new())),
        Err(error) => {
            let object_id = remember_created_mapi_identity(
                store,
                principal,
                MapiIdentityObjectKind::Message,
                canonical_id,
                None,
            )
            .await?;
            Ok((object_id, false, error.to_string()))
        }
    }
}

fn imported_source_key_reserved_global_counter(
    properties: &HashMap<u32, MapiValue>,
) -> Option<u64> {
    let source_key = match properties.get(&PID_TAG_SOURCE_KEY)? {
        MapiValue::Binary(bytes) => bytes,
        _ => return None,
    };
    persistable_import_source_key_global_counter(source_key)
}

fn pending_message_is_sync_metadata_only(
    properties: &HashMap<u32, MapiValue>,
    recipients: &[PendingRecipient],
) -> bool {
    !properties.is_empty()
        && recipients.is_empty()
        && properties.keys().all(|tag| {
            matches!(
                *tag,
                PID_TAG_SOURCE_KEY
                    | PID_TAG_LAST_MODIFICATION_TIME
                    | PID_TAG_CHANGE_KEY
                    | PID_TAG_PREDECESSOR_CHANGE_LIST
            )
        })
}

fn pending_message_is_unbacked_trash_sync_upload(
    folder_id: u64,
    properties: &HashMap<u32, MapiValue>,
) -> bool {
    if folder_id != TRASH_FOLDER_ID {
        return false;
    }
    let Some(counter) = pending_source_key_global_counter(properties) else {
        return false;
    };
    import_source_key_identity_scope(counter) == "out_of_lpe_persisted_range"
        && properties.contains_key(&PID_TAG_MESSAGE_CLASS_W)
        && properties.contains_key(&PID_TAG_NORMALIZED_SUBJECT_W)
}

fn pending_source_key_global_counter(properties: &HashMap<u32, MapiValue>) -> Option<u64> {
    match properties.get(&PID_TAG_SOURCE_KEY)? {
        MapiValue::Binary(bytes) => source_key_global_counter(bytes.as_slice()),
        _ => None,
    }
}

fn pending_source_key_message_id(properties: &HashMap<u32, MapiValue>) -> u64 {
    pending_source_key_global_counter(properties)
        .map(crate::mapi::identity::mapi_store_id)
        .unwrap_or(0)
}

fn imported_property_source_key_global_counter(properties: &[(u32, MapiValue)]) -> Option<u64> {
    properties
        .iter()
        .find_map(|(tag, value)| match (*tag, value) {
            (PID_TAG_SOURCE_KEY, MapiValue::Binary(bytes)) => {
                source_key_global_counter(bytes.as_slice())
            }
            _ => None,
        })
}

fn persistable_import_source_key_global_counter(source_key: &[u8]) -> Option<u64> {
    let counter = source_key_global_counter(source_key)?;
    (import_source_key_identity_scope(counter) == "persistable_dynamic").then_some(counter)
}

fn source_key_global_counter(source_key: &[u8]) -> Option<u64> {
    if source_key.len() != 22 || source_key[..16] != crate::mapi::identity::STORE_REPLICA_GUID {
        return None;
    }
    crate::mapi::identity::global_counter_from_globcnt(source_key.get(16..22)?)
}

fn import_source_key_identity_scope(counter: u64) -> &'static str {
    if counter < crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER {
        "system_reserved"
    } else if counter > crate::mapi::identity::MAX_PERSISTED_GLOBAL_COUNTER {
        "out_of_lpe_persisted_range"
    } else {
        "persistable_dynamic"
    }
}

fn hierarchy_checkpoint_status(
    checkpoint_kind: MapiCheckpointKind,
    folder_id: u64,
    checkpoint: &MapiSyncCheckpoint,
) -> &'static str {
    if checkpoint_kind != MapiCheckpointKind::Hierarchy {
        return "usable";
    }
    if checkpoint
        .cursor_json
        .get("source")
        .and_then(serde_json::Value::as_str)
        != Some("emsmdb-ics-download")
    {
        return "stale-source";
    }
    if checkpoint
        .cursor_json
        .get("hierarchySyncVersion")
        .and_then(serde_json::Value::as_u64)
        != Some(HIERARCHY_SYNC_CURSOR_VERSION)
    {
        return "stale-version";
    }
    if checkpoint
        .cursor_json
        .get("syncRootFolderId")
        .and_then(serde_json::Value::as_u64)
        != Some(folder_id)
    {
        return "stale-root";
    }
    "usable"
}

fn sync_property_filter_mode(sync_flags: u16, requested_property_tags: &[u32]) -> &'static str {
    if requested_property_tags.is_empty() {
        "none"
    } else if sync_flags & 0x0080 == 0 {
        "exclude"
    } else {
        "only-specified"
    }
}

#[derive(Debug, PartialEq, Eq)]
struct FastTransferGetBufferResponseDebug {
    header_bytes: usize,
    rop_id: String,
    rop_id_matches: bool,
    handle_index: u8,
    return_value: String,
    transfer_status: String,
    transfer_status_matches_completed: bool,
    in_progress_count: u16,
    total_step_count: u16,
    reserved_byte: u8,
    reserved_zero: bool,
    transfer_buffer_size: u16,
    transfer_payload_bytes: usize,
    transfer_buffer_size_matches_payload: bool,
    transfer_payload_preview_hex: String,
    transfer_payload_tail_hex: String,
    parse_error: String,
}

fn summarize_fast_transfer_get_buffer_response(
    response: &[u8],
    completed: bool,
) -> FastTransferGetBufferResponseDebug {
    const HEADER_BYTES: usize = 15;
    if response.len() < HEADER_BYTES {
        return FastTransferGetBufferResponseDebug {
            header_bytes: HEADER_BYTES,
            rop_id: response
                .first()
                .map(|value| format!("0x{value:02x}"))
                .unwrap_or_default(),
            rop_id_matches: response.first() == Some(&0x4e),
            handle_index: response.get(1).copied().unwrap_or_default(),
            return_value: String::new(),
            transfer_status: String::new(),
            transfer_status_matches_completed: false,
            in_progress_count: 0,
            total_step_count: 0,
            reserved_byte: 0,
            reserved_zero: false,
            transfer_buffer_size: 0,
            transfer_payload_bytes: 0,
            transfer_buffer_size_matches_payload: false,
            transfer_payload_preview_hex: String::new(),
            transfer_payload_tail_hex: String::new(),
            parse_error: "truncated_get_buffer_response_header".to_string(),
        };
    }

    let return_value = u32::from_le_bytes(response[2..6].try_into().unwrap());
    let transfer_status = u16::from_le_bytes(response[6..8].try_into().unwrap());
    let in_progress_count = u16::from_le_bytes(response[8..10].try_into().unwrap());
    let total_step_count = u16::from_le_bytes(response[10..12].try_into().unwrap());
    let reserved_byte = response[12];
    let transfer_buffer_size = u16::from_le_bytes(response[13..15].try_into().unwrap());
    let transfer_payload = &response[HEADER_BYTES..];
    let tail_start = transfer_payload.len().saturating_sub(16);

    FastTransferGetBufferResponseDebug {
        header_bytes: HEADER_BYTES,
        rop_id: format!("0x{:02x}", response[0]),
        rop_id_matches: response[0] == 0x4e,
        handle_index: response[1],
        return_value: format!("0x{return_value:08x}"),
        transfer_status: format!("0x{transfer_status:04x}"),
        transfer_status_matches_completed: matches!(
            (completed, transfer_status),
            (true, 0x0003) | (false, 0x0001)
        ),
        in_progress_count,
        total_step_count,
        reserved_byte,
        reserved_zero: reserved_byte == 0,
        transfer_buffer_size,
        transfer_payload_bytes: transfer_payload.len(),
        transfer_buffer_size_matches_payload: transfer_buffer_size as usize
            == transfer_payload.len(),
        transfer_payload_preview_hex: hex_preview(transfer_payload, 32),
        transfer_payload_tail_hex: hex_preview(&transfer_payload[tail_start..], 16),
        parse_error: String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uploaded_state_delta_anchor_requires_idset_and_cnset_seen() {
        let idset_only = upload_state_marker_bit(0x4017_0003);
        assert!(!uploaded_state_has_delta_anchor(idset_only));

        let cnset_only = upload_state_marker_bit(0x6796_0102);
        assert!(!uploaded_state_has_delta_anchor(cnset_only));

        assert!(uploaded_state_has_delta_anchor(idset_only | cnset_only));
    }

    #[test]
    fn uploaded_state_empty_stream_does_not_create_delta_anchor() {
        let mut marker_mask = 0;
        let uploaded_bytes = 0usize;

        if uploaded_bytes > 0 {
            mark_uploaded_state_stream(&mut marker_mask, 0x4017_0003);
            mark_uploaded_state_stream(&mut marker_mask, 0x6796_0102);
        }

        assert!(!uploaded_state_has_delta_anchor(marker_mask));
    }

    #[test]
    fn execute_rop_debug_summary_decodes_ids_and_return_codes() {
        let mut request_bytes = vec![0x02, 0, 0, 1];
        request_bytes.extend_from_slice(
            &crate::mapi::identity::wire_id_bytes_from_object_id(ROOT_FOLDER_ID).unwrap(),
        );
        request_bytes.push(0);
        let request_buffer = rop_buffer_with_response(request_bytes, &[0]);
        let request_summary = summarize_request_rop_buffer(&request_buffer);

        assert_eq!(request_summary.ids, vec![0x02]);
        assert_eq!(request_summary.ids_csv, "0x02");
        assert_eq!(request_summary.handle_count, 1);
        assert!(request_summary.parse_error.is_empty());

        let request = RopRequest {
            rop_id: 0x02,
            input_handle_index: Some(0),
            output_handle_index: Some(1),
            payload: Vec::new(),
        };
        let response_buffer = rop_buffer_with_response(rop_open_folder_response(&request), &[42]);
        let response_summary =
            summarize_response_rop_buffer(&response_buffer, &request_summary.ids);

        assert_eq!(response_summary.ids_csv, "0x02");
        assert_eq!(response_summary.results_csv, "0x02:0x00000000");
        assert_eq!(response_summary.count, 1);
        assert_eq!(response_summary.handle_count, 1);
        assert!(response_summary.parse_error.is_empty());
    }

    #[test]
    fn get_buffer_response_debug_exposes_wire_framing() {
        let mut response = vec![0x4e, 0x03];
        response.extend_from_slice(&0u32.to_le_bytes());
        response.extend_from_slice(&0x0003u16.to_le_bytes());
        response.extend_from_slice(&2u16.to_le_bytes());
        response.extend_from_slice(&2u16.to_le_bytes());
        response.push(0);
        response.extend_from_slice(&4u16.to_le_bytes());
        response.extend_from_slice(&[0x40, 0x12, 0x00, 0x03]);

        let debug = summarize_fast_transfer_get_buffer_response(&response, true);

        assert_eq!(debug.rop_id, "0x4e");
        assert!(debug.rop_id_matches);
        assert_eq!(debug.handle_index, 3);
        assert_eq!(debug.return_value, "0x00000000");
        assert_eq!(debug.transfer_status, "0x0003");
        assert!(debug.transfer_status_matches_completed);
        assert_eq!(debug.in_progress_count, 2);
        assert_eq!(debug.total_step_count, 2);
        assert!(debug.reserved_zero);
        assert_eq!(debug.transfer_buffer_size, 4);
        assert_eq!(debug.transfer_payload_bytes, 4);
        assert!(debug.transfer_buffer_size_matches_payload);
        assert_eq!(debug.transfer_payload_preview_hex, "40120003");
        assert!(debug.parse_error.is_empty());
    }

    #[test]
    fn execute_rop_debug_summary_skips_release_rops_without_responses() {
        let request = RopRequest {
            rop_id: 0x7F,
            input_handle_index: Some(1),
            output_handle_index: None,
            payload: 2u32.to_le_bytes().to_vec(),
        };
        let response_buffer =
            rop_buffer_with_response(rop_get_local_replica_ids_response(&request, 42), &[42]);
        let response_summary = summarize_response_rop_buffer(&response_buffer, &[0x01, 0x7F]);

        assert_eq!(response_summary.ids_csv, "0x7f");
        assert_eq!(response_summary.results_csv, "0x7f:0x00000000");
        assert_eq!(response_summary.count, 1);
        assert_eq!(response_summary.handle_count, 1);
        assert!(response_summary.parse_error.is_empty());
    }

    #[test]
    fn execute_rop_response_framing_summary_marks_multi_rop_boundaries() {
        let mut responses = Vec::new();
        for rop_id in [0x02, 0x70, 0x75, 0x77, 0x75, 0x77] {
            responses.push(rop_id);
            responses.push(1);
            responses.extend_from_slice(&0u32.to_le_bytes());
        }
        responses.push(0x4E);
        responses.push(2);
        responses.extend_from_slice(&0u32.to_le_bytes());
        responses.extend_from_slice(&0x0003u16.to_le_bytes());
        responses.extend_from_slice(&1u16.to_le_bytes());
        responses.extend_from_slice(&1u16.to_le_bytes());
        responses.push(0);
        responses.extend_from_slice(&4u16.to_le_bytes());
        responses.extend_from_slice(&[0x03, 0x00, 0x14, 0x40]);

        let response_buffer =
            rpc_header_ext_rop_buffer(rop_buffer_with_response_spec(responses, &[1, 4, 3]));
        let response_summary = summarize_response_rop_buffer(
            &response_buffer,
            &[0x02, 0x70, 0x75, 0x77, 0x75, 0x77, 0x4E],
        );

        assert_eq!(response_summary.buffer_layout, "rpc_header_ext_spec");
        assert_eq!(response_summary.response_payload_bytes, 55);
        assert_eq!(response_summary.handle_table_bytes, 12);
        assert_eq!(response_summary.count, 7);
        assert_eq!(
            response_summary.results_csv,
            "0x02:0x00000000,0x70:0x00000000,0x75:0x00000000,0x77:0x00000000,0x75:0x00000000,0x77:0x00000000,0x4e:0x00000000"
        );
        assert!(response_summary
            .frames
            .contains("0x02@0..6:len=6:out=1:rv=0x00000000"));
        assert!(response_summary
            .frames
            .contains("0x4e@36..55:len=19:out=2:rv=0x00000000"));
        assert!(response_summary.parse_error.is_empty());
    }

    #[test]
    fn execute_response_framing_context_includes_bootstrap_getprops_batches() {
        assert_eq!(
            execute_response_framing_context(&[0x07]),
            Some("getprops_or_release_getprops")
        );
        assert_eq!(
            execute_response_framing_context(&[0x01, 0x07]),
            Some("getprops_or_release_getprops")
        );
        assert_eq!(
            execute_response_framing_context(&[0x01, 0x01]),
            Some("release_only")
        );
        assert_eq!(
            execute_response_framing_context(&[0x02, 0x70, 0x4E]),
            Some("hierarchy_sync")
        );
        assert_eq!(execute_response_framing_context(&[0x0A]), Some("setprops"));
        assert_eq!(execute_response_framing_context(&[0x79]), Some("setprops"));
        assert_eq!(execute_response_framing_context(&[0x02, 0x07]), None);
    }

    #[test]
    fn long_term_id_from_id_rejects_unparsed_or_not_loaded_scope() {
        let object_id = crate::mapi::identity::mapi_store_id(
            crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 1,
        );
        let request = RopRequest {
            rop_id: RopId::LongTermIdFromId as u8,
            input_handle_index: Some(0),
            output_handle_index: None,
            payload: crate::mapi::identity::wire_id_bytes_from_object_id(object_id)
                .unwrap()
                .to_vec(),
        };

        assert_eq!(
            rop_long_term_id_from_id_response_for_scope(&request, None, "not_loaded"),
            vec![RopId::LongTermIdFromId as u8, 0x00, 0x0F, 0x01, 0x04, 0x80]
        );
        assert_eq!(
            &rop_long_term_id_from_id_response_for_scope(&request, None, "message")[..6],
            &[RopId::LongTermIdFromId as u8, 0x00, 0, 0, 0, 0]
        );
        assert_eq!(
            rop_long_term_id_from_id_response_for_scope(&request, None, "unparsed"),
            vec![RopId::LongTermIdFromId as u8, 0x00, 0x0F, 0x01, 0x04, 0x80]
        );
    }

    #[test]
    fn folder_set_property_problems_accepts_ipm_subtree_ostid_write() {
        let ipm_subtree = MapiObject::Folder {
            folder_id: IPM_SUBTREE_FOLDER_ID,
            properties: std::collections::HashMap::new(),
        };
        let inbox = MapiObject::Folder {
            folder_id: INBOX_FOLDER_ID,
            properties: std::collections::HashMap::new(),
        };

        assert!(folder_set_property_problems(
            Some(&ipm_subtree),
            &[(PID_TAG_OST_OSTID, MapiValue::Binary(vec![1; 40]))],
        )
        .is_empty());
        assert_eq!(
            folder_set_property_problems(
                Some(&ipm_subtree),
                &[(PID_TAG_OST_OSTID, MapiValue::Binary(Vec::new()))],
            ),
            vec![(0, PID_TAG_OST_OSTID, 0x8004_0102)]
        );
        assert_eq!(
            folder_set_property_problems(
                Some(&ipm_subtree),
                &[(PID_TAG_DISPLAY_NAME_W, MapiValue::String("IPM".to_string()))],
            ),
            vec![(0, PID_TAG_DISPLAY_NAME_W, 0x8004_0102)]
        );
        assert_eq!(
            folder_set_property_problems(
                Some(&inbox),
                &[(PID_TAG_OST_OSTID, MapiValue::Binary(vec![1; 40]))],
            ),
            vec![(0, PID_TAG_OST_OSTID, 0x8004_0102)]
        );
    }

    #[test]
    fn default_folder_entry_id_values_debug_decodes_indexed_special_folder_ids() {
        let mailbox_guid = Uuid::parse_str("ea339446-27b9-4a9c-b0de-873f03a35376").unwrap();
        let values = vec![
            crate::mapi::identity::folder_entry_id_from_object_id(
                mailbox_guid,
                CONFLICTS_FOLDER_ID,
            )
            .unwrap(),
            crate::mapi::identity::folder_entry_id_from_object_id(
                mailbox_guid,
                SYNC_ISSUES_FOLDER_ID,
            )
            .unwrap(),
            crate::mapi::identity::folder_entry_id_from_object_id(
                mailbox_guid,
                LOCAL_FAILURES_FOLDER_ID,
            )
            .unwrap(),
            crate::mapi::identity::folder_entry_id_from_object_id(
                mailbox_guid,
                SERVER_FAILURES_FOLDER_ID,
            )
            .unwrap(),
        ];

        let debug = default_folder_entry_id_values_for_debug(&[(
            PID_TAG_ADDITIONAL_REN_ENTRY_IDS,
            MapiValue::MultiBinary(values),
        )]);

        assert!(debug.contains("PidTagAdditionalRenEntryIds:count=4"));
        assert!(debug.contains("index=0"));
        assert!(debug.contains("decoded_name=conflicts"));
        assert!(debug.contains("omitted_preserved_indexes=4"));
    }

    #[test]
    fn default_folder_entry_id_values_debug_decodes_freebusy_data_index() {
        let mailbox_guid = Uuid::parse_str("ea339446-27b9-4a9c-b0de-873f03a35376").unwrap();
        let freebusy_entry_id = crate::mapi::identity::folder_entry_id_from_object_id(
            mailbox_guid,
            FREEBUSY_DATA_FOLDER_ID,
        )
        .unwrap();

        let debug = default_folder_entry_id_values_for_debug(&[(
            PID_TAG_FREE_BUSY_ENTRY_IDS,
            MapiValue::MultiBinary(vec![Vec::new(), Vec::new(), Vec::new(), freebusy_entry_id]),
        )]);

        assert!(debug.contains("PidTagFreeBusyEntryIds:count=4"));
        assert!(debug.contains("index=3"));
        assert!(debug.contains("decoded_name=freebusy_data"));
        assert!(debug.contains("matches_expected=true"));
    }

    #[test]
    fn default_folder_identification_values_do_not_shadow_canonical_inbox_projection() {
        let inbox = MapiObject::Folder {
            folder_id: INBOX_FOLDER_ID,
            properties: std::collections::HashMap::new(),
        };
        let retained = default_folder_identification_safe_property_values(
            &test_principal(),
            Some(&inbox),
            vec![
                (
                    PID_TAG_ADDITIONAL_REN_ENTRY_IDS,
                    MapiValue::MultiBinary(vec![
                        vec![0xAA],
                        vec![0xBB],
                        vec![0xCC],
                        vec![0xDD],
                        vec![0xEE],
                        vec![0xFA, 0xCE],
                    ]),
                ),
                (
                    PID_TAG_DISPLAY_NAME_W,
                    MapiValue::String("Inbox".to_string()),
                ),
            ],
        );

        assert_eq!(retained.len(), 2);
        let Some(MapiValue::MultiBinary(values)) = retained
            .iter()
            .find(|(tag, _)| *tag == PID_TAG_ADDITIONAL_REN_ENTRY_IDS)
            .map(|(_, value)| value)
        else {
            panic!("expected AdditionalRenEntryIds");
        };
        assert_eq!(values.len(), 6);
        assert_ne!(values[0], vec![0xAA]);
        assert_eq!(values[5], vec![0xFA, 0xCE]);
        assert_eq!(
            retained
                .iter()
                .find(|(tag, _)| *tag == PID_TAG_DISPLAY_NAME_W),
            Some(&(
                PID_TAG_DISPLAY_NAME_W,
                MapiValue::String("Inbox".to_string())
            ))
        );
    }

    #[test]
    fn root_scalar_default_folder_entry_ids_are_retained_for_session_writeback() {
        let root = MapiObject::Folder {
            folder_id: ROOT_FOLDER_ID,
            properties: std::collections::HashMap::new(),
        };
        let calendar_entry_id = crate::mapi::identity::folder_entry_id_from_object_id(
            test_principal().account_id,
            CALENDAR_FOLDER_ID,
        )
        .unwrap();

        let retained = default_folder_identification_safe_property_values(
            &test_principal(),
            Some(&root),
            vec![
                (
                    PID_TAG_IPM_APPOINTMENT_ENTRY_ID,
                    MapiValue::Binary(calendar_entry_id.clone()),
                ),
                (
                    PID_TAG_ADDITIONAL_REN_ENTRY_IDS,
                    MapiValue::MultiBinary(vec![Vec::new()]),
                ),
            ],
        );

        assert_eq!(
            retained,
            vec![(
                PID_TAG_IPM_APPOINTMENT_ENTRY_ID,
                MapiValue::Binary(calendar_entry_id)
            )]
        );
    }

    #[test]
    fn root_scalar_default_folder_entry_id_write_is_retained_as_session_state() {
        let mut root = MapiObject::Folder {
            folder_id: ROOT_FOLDER_ID,
            properties: std::collections::HashMap::new(),
        };
        let calendar_entry_id = crate::mapi::identity::folder_entry_id_from_object_id(
            test_principal().account_id,
            CALENDAR_FOLDER_ID,
        )
        .unwrap();

        apply_mapi_property_values(
            Some(&mut root),
            vec![(
                PID_TAG_IPM_APPOINTMENT_ENTRY_ID,
                MapiValue::Binary(calendar_entry_id.clone()),
            )],
        )
        .unwrap();

        let MapiObject::Folder { properties, .. } = root else {
            panic!("expected folder object");
        };
        assert_eq!(
            properties.get(&PID_TAG_IPM_APPOINTMENT_ENTRY_ID),
            Some(&MapiValue::Binary(calendar_entry_id))
        );
    }

    #[test]
    fn ipm_subtree_ostid_write_is_retained_as_session_mutable_state() {
        let mut ipm_subtree = MapiObject::Folder {
            folder_id: IPM_SUBTREE_FOLDER_ID,
            properties: std::collections::HashMap::new(),
        };
        let client_ostid = vec![1; 40];

        apply_mapi_property_values(
            Some(&mut ipm_subtree),
            vec![(PID_TAG_OST_OSTID, MapiValue::Binary(client_ostid.clone()))],
        )
        .unwrap();

        let MapiObject::Folder { properties, .. } = ipm_subtree else {
            panic!("expected folder object");
        };
        assert_eq!(
            properties.get(&PID_TAG_OST_OSTID),
            Some(&MapiValue::Binary(client_ostid))
        );
    }

    #[test]
    fn logon_response_debug_summary_decodes_private_mailbox_fields() {
        let principal = AccountPrincipal {
            tenant_id: Uuid::from_u128(0xaaaaaaaa_aaaa_aaaa_aaaa_aaaaaaaaaaaa),
            account_id: Uuid::from_u128(0xbbbbbbbb_bbbb_bbbb_bbbb_bbbbbbbbbbbb),
            email: "alice@example.test".to_string(),
            display_name: "Alice".to_string(),
        };
        let request = RopRequest {
            rop_id: 0xFE,
            input_handle_index: Some(0),
            output_handle_index: Some(1),
            payload: vec![0x01],
        };
        let response_buffer =
            rop_buffer_with_response(rop_logon_response_body(&principal, &request), &[42]);

        let summary = summarize_logon_response_rop(&response_buffer, &[0xFE]);

        assert!(summary.present);
        assert_eq!(summary.output_handle_index, "1");
        assert_eq!(summary.error_code, "0x00000000");
        assert_eq!(summary.logon_flags, "0x01");
        assert!(summary
            .special_folder_ids
            .starts_with(&format!("{ROOT_FOLDER_ID:#018x}")));
        assert_eq!(summary.response_flags, "0x07");
        assert_eq!(summary.mailbox_guid, principal.account_id.to_string());
        assert_eq!(summary.replid, "1");
        assert_eq!(summary.replica_guid.len(), 32);
        assert!(summary.parse_error.is_empty());
    }

    #[test]
    fn first_post_hierarchy_probe_summary_identifies_open_folder_and_getprops_shapes() {
        let mut request_bytes = vec![0x02, 0x00, 0x00, 0x01];
        request_bytes.extend_from_slice(
            &crate::mapi::identity::wire_id_bytes_from_object_id(CALENDAR_FOLDER_ID).unwrap(),
        );
        request_bytes.push(0);
        request_bytes.extend_from_slice(&[0x07, 0x00, 0x01]);
        request_bytes.extend_from_slice(&4096u16.to_le_bytes());
        request_bytes.extend_from_slice(&2u16.to_le_bytes());
        request_bytes.extend_from_slice(&PID_TAG_DISPLAY_NAME_W.to_le_bytes());
        request_bytes.extend_from_slice(&PID_TAG_CONTENT_COUNT.to_le_bytes());
        let request_buffer = rop_buffer_with_response(request_bytes, &[1, u32::MAX]);

        let open_folder_request = RopRequest {
            rop_id: 0x02,
            input_handle_index: Some(0),
            output_handle_index: Some(1),
            payload: Vec::new(),
        };
        let mut responses = rop_open_folder_response(&open_folder_request);
        responses.extend_from_slice(&[0x07, 0x01]);
        responses.extend_from_slice(&0u32.to_le_bytes());
        responses.push(0);
        responses.extend_from_slice(&utf16z_bytes("Calendar"));
        responses.extend_from_slice(&0u32.to_le_bytes());
        let response_buffer = rop_buffer_with_response(responses, &[1]);

        let summary = summarize_first_post_hierarchy_probe(&request_buffer, &response_buffer);

        assert_eq!(summary.open_folder_request_count, 1);
        assert!(summary
            .open_folder_requests
            .contains(&format!("folder=0x{CALENDAR_FOLDER_ID:016x};name=calendar")));
        assert!(summary
            .open_folder_response_shapes
            .contains("result=0x00000000;has_rules=0;is_ghosted=0"));
        assert_eq!(summary.get_properties_specific_request_count, 1);
        assert!(summary
            .get_properties_specific_requests
            .contains("tags=0x3001001f,0x36020003"));
        assert!(summary
            .get_properties_specific_response_shapes
            .contains("result=0x00000000;row=standard"));
        assert!(summary.parse_error.is_empty());
    }

    #[test]
    fn post_hierarchy_probe_summary_marks_default_folder_entry_id_getprops() {
        let mut request_bytes = vec![0x07, 0x00, 0x01];
        request_bytes.extend_from_slice(&4096u16.to_le_bytes());
        request_bytes.extend_from_slice(&1u16.to_le_bytes());
        request_bytes.extend_from_slice(&PID_TAG_IPM_APPOINTMENT_ENTRY_ID.to_le_bytes());
        let request_buffer = rop_buffer_with_response(request_bytes, &[1]);

        let mut responses = vec![0x07, 0x01];
        responses.extend_from_slice(&0u32.to_le_bytes());
        responses.push(0);
        responses.extend_from_slice(&46u16.to_le_bytes());
        responses.extend_from_slice(&[0xAA; 46]);
        let response_buffer = rop_buffer_with_response(responses, &[1]);

        let summary = summarize_first_post_hierarchy_probe(&request_buffer, &response_buffer);

        assert!(summary
            .get_properties_specific_response_shapes
            .contains("values=0x36d00102:binary:bytes=46"));
        assert!(summary.parse_error.is_empty());
    }

    #[test]
    fn root_default_folder_getprops_uses_canonical_projection_not_setprops_state() {
        let reopened_root = MapiObject::Folder {
            folder_id: ROOT_FOLDER_ID,
            properties: HashMap::new(),
        };
        let request = get_properties_specific_request(&[PID_TAG_IPM_APPOINTMENT_ENTRY_ID]);
        let response = rop_get_properties_specific_response(
            &request,
            Some(&reopened_root),
            &test_principal(),
            &[],
            &[],
            &empty_snapshot(),
        );

        let mut cursor = Cursor::new(&response[7..]);
        assert_eq!(
            parse_property_value_for_tag(&mut cursor, PID_TAG_IPM_APPOINTMENT_ENTRY_ID).unwrap(),
            MapiValue::Binary(
                crate::mapi::identity::folder_entry_id_from_object_id(
                    test_principal().account_id,
                    CALENDAR_FOLDER_ID,
                )
                .unwrap()
            )
        );
        let MapiObject::Folder { properties, .. } = &reopened_root else {
            panic!("expected reopened root folder object");
        };
        assert!(properties.is_empty());
    }

    #[test]
    fn first_post_hierarchy_probe_summary_identifies_set_properties_shapes() {
        let mut property_value = Vec::new();
        property_value.extend_from_slice(&PID_TAG_IPM_APPOINTMENT_ENTRY_ID.to_le_bytes());
        property_value.extend_from_slice(&3u16.to_le_bytes());
        property_value.extend_from_slice(&[0xAA, 0xBB, 0xCC]);
        let property_value_size = property_value.len() + 2;
        let mut request_bytes = vec![0x0A, 0x00, 0x01];
        request_bytes.extend_from_slice(&(property_value_size as u16).to_le_bytes());
        request_bytes.extend_from_slice(&1u16.to_le_bytes());
        request_bytes.extend_from_slice(&property_value);
        let request_buffer = rop_buffer_with_response(request_bytes, &[1]);

        let request = RopRequest {
            rop_id: 0x0A,
            input_handle_index: Some(1),
            output_handle_index: None,
            payload: Vec::new(),
        };
        let response_buffer = rop_buffer_with_response(rop_set_properties_response(&request), &[1]);

        let summary = summarize_first_post_hierarchy_probe(&request_buffer, &response_buffer);

        assert_eq!(summary.set_properties_request_count, 1);
        assert!(summary
            .set_properties_requests
            .contains("tags=0x36d00102;values=0x36d00102:binary:bytes=3"));
        assert!(summary
            .set_properties_response_shapes
            .contains("result=0x00000000;property_problem_count=0"));
        assert!(summary.parse_error.is_empty());
    }

    fn utf16z_bytes(value: &str) -> Vec<u8> {
        value
            .encode_utf16()
            .chain(std::iter::once(0))
            .flat_map(u16::to_le_bytes)
            .collect()
    }

    fn get_properties_specific_request(property_tags: &[u32]) -> RopRequest {
        let mut payload = Vec::new();
        payload.extend_from_slice(&4096u16.to_le_bytes());
        payload.extend_from_slice(&(property_tags.len() as u16).to_le_bytes());
        for tag in property_tags {
            payload.extend_from_slice(&tag.to_le_bytes());
        }
        RopRequest {
            rop_id: 0x07,
            input_handle_index: Some(0),
            output_handle_index: None,
            payload,
        }
    }

    fn test_principal() -> AccountPrincipal {
        AccountPrincipal {
            tenant_id: Uuid::from_u128(0xaaaaaaaa_aaaa_aaaa_aaaa_aaaaaaaaaaaa),
            account_id: Uuid::from_u128(0xbbbbbbbb_bbbb_bbbb_bbbb_bbbbbbbbbbbb),
            email: "alice@example.test".to_string(),
            display_name: "Alice".to_string(),
        }
    }

    fn empty_snapshot() -> MapiMailStoreSnapshot {
        MapiMailStoreSnapshot::new(
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )
    }
}
