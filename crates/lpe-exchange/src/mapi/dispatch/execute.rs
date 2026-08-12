use super::*;

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
        return execute_transport_failure_response(
            request_id,
            13,
            "missing MAPI session cookie",
            Vec::new(),
        );
    };
    let Some(_active_request) = acquire_execute_active_session_request(&session_id).await else {
        return execute_transport_failure_response(
            request_id,
            15,
            "MAPI session already has an active request",
            session_context_cookies(endpoint, &session_id, false),
        );
    };
    let Some(mut session) = get_session(&session_id) else {
        return execute_transport_failure_response(
            request_id,
            10,
            "MAPI session context not found",
            Vec::new(),
        );
    };
    if session.endpoint != endpoint
        || session.tenant_id != principal.tenant_id
        || session.account_id != principal.account_id
        || session.email != principal.email
    {
        return execute_transport_failure_response(
            request_id,
            10,
            "MAPI authentication context changed",
            Vec::new(),
        );
    }
    session.record_transport_request("Execute", request_id);

    let execute = match parse_execute_request(body) {
        Ok(execute) => execute,
        Err(error) => {
            log_execute_parse_failure_debug(endpoint, principal, headers, request_id, body, &error);
            return execute_transport_failure_response(
                request_id,
                12,
                &format!("invalid Execute request body: {error}"),
                session_context_cookies(endpoint, &session_id, false),
            );
        }
    };
    if !session_matches(&session, endpoint, principal) {
        return execute_transport_failure_response(
            request_id,
            10,
            "MAPI authentication context changed",
            session_context_cookies(endpoint, &session_id, false),
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
                    session.record_execute_after_hierarchy_completion(
                        &request_debug.ids,
                        &request_debug.names_csv,
                    )
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
            let response_debug = summarize_response_rop_buffer(
                execute_success_rop_buffer(&cached.response_body).unwrap_or_default(),
                &request_debug.ids,
            );
            session.record_last_successful_execute_context(
                format!(
                    "request_id={request_id};request_rops={};response_rops={};response_results={};response_rop_bytes={};cached=true",
                    cached.request_rop_ids,
                    cached.response_rop_ids,
                    cached.response_rop_results,
                    cached.response_rop_buffer_bytes
                ),
                request_debug.ids.iter().any(|rop_id| *rop_id != RopId::Release.as_u8()),
            );
            log_post_common_views_handoff_execute_response(
                endpoint,
                principal,
                headers,
                &session_id,
                request_id,
                &session,
                &request_debug,
                &response_debug,
                cached.response_body.len(),
                true,
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
        return execute_transport_failure_response(
            request_id,
            12,
            "reused MAPI Execute request id with a different ROP payload",
            session_context_cookies(endpoint, &session_id, false),
        );
    }

    if execute_can_skip_identity_scope(&execute.rop_buffer, &session) {
        let mut snapshot = MapiMailStoreSnapshot::empty();
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
            request_id,
            &mut session,
            &mailboxes,
            &emails,
            &mut snapshot,
            None,
            validator,
            &execute.rop_buffer,
            execute.max_rop_out,
            execute.flags,
            request_debug.all_release,
            request_debug.handle_count,
            &request_debug.handle_table_summary,
            &request_debug.ids_csv,
            &request_debug.names_csv,
            &request_debug.non_release_rops,
        )
        .await;
        let post_hierarchy_observation =
            if endpoint == MapiEndpoint::Emsmdb && hierarchy_completed_before_execute {
                session.record_execute_after_hierarchy_completion(
                    &request_debug.ids,
                    &request_debug.names_csv,
                )
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
        let rop_buffer = apply_execute_max_rop_out(
            request_id,
            &execute.rop_buffer,
            rop_buffer,
            execute.max_rop_out,
        );
        let response_body = execute_success_body(rop_buffer, Vec::new());
        let response_debug = summarize_response_rop_buffer(
            execute_success_rop_buffer(&response_body).unwrap_or_default(),
            &request_debug.ids,
        );
        session.record_last_successful_execute_context(
            format!(
                "request_id={request_id};request_rops={};response_rops={};response_results={};response_rop_bytes={};cached=false",
                request_debug.names_csv,
                response_debug.names_csv,
                response_debug.results_csv,
                response_debug.response_payload_bytes
            ),
            request_debug.ids.iter().any(|rop_id| *rop_id != RopId::Release.as_u8()),
        );
        log_post_common_views_handoff_execute_response(
            endpoint,
            principal,
            headers,
            &session_id,
            request_id,
            &session,
            &request_debug,
            &response_debug,
            response_body.len(),
            false,
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

    let notification_cursor_before_snapshot = if session.notification_cursor.is_none()
        && (session.has_notification_targets()
            || request_debug.ids.iter().any(|rop_id| {
                matches!(
                    RopId::from_u8(*rop_id),
                    Some(
                        RopId::CollapseRow
                            | RopId::ExpandRow
                            | RopId::FindRow
                            | RopId::QueryColumnsAll
                            | RopId::QueryPosition
                            | RopId::QueryRows
                            | RopId::SeekRow
                            | RopId::SeekRowBookmark
                            | RopId::SeekRowFractional
                    )
                )
            })) {
        store
            .fetch_mapi_notification_cursor(principal.account_id)
            .await
            .ok()
            .map(|cursor| cursor.unwrap_or(0))
    } else {
        None
    };
    let identity_scope = match load_mapi_identity_scope(store, principal.account_id).await {
        Ok(identity_scope) => identity_scope,
        Err(error) => {
            store_session(session_id.clone(), session);
            return execute_transport_failure_response(
                request_id,
                1,
                &format!("failed to load durable MAPI identity scope: {error:#}"),
                session_context_cookies(endpoint, &session_id, false),
            );
        }
    };
    let request_identity_scope = identity_scope.request_identity_scope();
    session.store_replica_guid = Some(Uuid::from_bytes(identity_scope.codec.replica_guid()));
    if let Err(error) =
        refresh_persisted_special_folder_aliases(store, principal, &mut session).await
    {
        store_session(session_id.clone(), session);
        return execute_transport_failure_response(
            request_id,
            1,
            &format!("failed to load persisted MAPI special-folder aliases: {error:#}"),
            session_context_cookies(endpoint, &session_id, false),
        );
    }
    let mut access_plan = crate::mapi::identity::with_current_mapi_identity_codec(
        identity_scope.codec.clone(),
        async { plan_mapi_store_access(&session, &execute.rop_buffer) },
    )
    .await;
    // [MS-OXCMAPIHTTP] section 2.2.4.4.2: NotificationWait can already have
    // queued a Contact or Calendar change when Outlook sends its release-only
    // Execute. The active root
    // hierarchy row needs the current collaboration item count, even though
    // the ROP itself does not open that folder.
    if session.pending_collaboration_hierarchy_notification_requires_contents() {
        access_plan.requires_associated_contents = true;
    }
    log_execute_store_access_debug(endpoint, principal, headers, request_id, &access_plan);
    let mut snapshot = match crate::mapi::identity::with_current_mapi_request_identity_scope(
        request_identity_scope.clone(),
        Box::pin(load_mapi_store_for_access_plan(
            store,
            principal.account_id,
            &identity_scope,
            &request_identity_scope,
            &access_plan,
            500,
        )),
    )
    .await
    {
        Ok(snapshot) => snapshot,
        Err(error) => {
            if let Some(fallback_plan) =
                hierarchy_sync_selective_fallback_plan(&session, &execute.rop_buffer)
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
                match crate::mapi::identity::with_current_mapi_request_identity_scope(
                    request_identity_scope.clone(),
                    Box::pin(load_mapi_store_for_access_plan(
                        store,
                        principal.account_id,
                        &identity_scope,
                        &request_identity_scope,
                        &fallback_plan,
                        500,
                    )),
                )
                .await
                {
                    Ok(snapshot) => snapshot,
                    Err(fallback_error) => {
                        store_session(session_id.clone(), session);
                        return execute_transport_failure_response(
                            request_id,
                            1,
                            &format!(
                                "failed to load MAPI mail store view: {error:#}; fallback failed: {fallback_error:#}"
                            ),
                            session_context_cookies(endpoint, &session_id, false),
                        );
                    }
                }
            } else {
                store_session(session_id.clone(), session);
                return execute_transport_failure_response(
                    request_id,
                    1,
                    &format!("failed to load MAPI mail store view: {error:#}"),
                    session_context_cookies(endpoint, &session_id, false),
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
    let rop_buffer = crate::mapi::identity::with_current_mapi_request_identity_scope(
        request_identity_scope,
        Box::pin(crate::mapi::identity::with_current_mapi_identity_codec(
            snapshot.identity_codec().clone(),
            execute_rops(
                store,
                principal,
                request_id,
                &mut session,
                &mailboxes,
                &emails,
                &mut snapshot,
                notification_cursor_before_snapshot,
                validator,
                &execute.rop_buffer,
                execute.max_rop_out,
                execute.flags,
                request_debug.all_release,
                request_debug.handle_count,
                &request_debug.handle_table_summary,
                &request_debug.ids_csv,
                &request_debug.names_csv,
                &request_debug.non_release_rops,
            ),
        )),
    )
    .await;
    let post_hierarchy_observation = if endpoint == MapiEndpoint::Emsmdb
        && hierarchy_completed_before_execute
    {
        session
            .record_execute_after_hierarchy_completion(&request_debug.ids, &request_debug.names_csv)
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
    let rop_buffer = apply_execute_max_rop_out(
        request_id,
        &execute.rop_buffer,
        rop_buffer,
        execute.max_rop_out,
    );
    let response_body = execute_success_body(rop_buffer, Vec::new());
    let response_debug = summarize_response_rop_buffer(
        execute_success_rop_buffer(&response_body).unwrap_or_default(),
        &request_debug.ids,
    );
    session.record_last_successful_execute_context(
        format!(
            "request_id={request_id};request_rops={};response_rops={};response_results={};response_rop_bytes={};cached=false",
            request_debug.names_csv,
            response_debug.names_csv,
            response_debug.results_csv,
            response_debug.response_payload_bytes
        ),
        request_debug.ids.iter().any(|rop_id| *rop_id != RopId::Release.as_u8()),
    );
    log_post_common_views_handoff_execute_response(
        endpoint,
        principal,
        headers,
        &session_id,
        request_id,
        &session,
        &request_debug,
        &response_debug,
        response_body.len(),
        false,
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

fn log_post_common_views_handoff_execute_response(
    endpoint: MapiEndpoint,
    principal: &AccountPrincipal,
    headers: &HeaderMap,
    session_id: &str,
    request_id: &str,
    session: &MapiSession,
    request: &RopRequestDebugSummary,
    response: &RopResponseDebugSummary,
    response_body_bytes: usize,
    cached_execute_response: bool,
) {
    if endpoint != MapiEndpoint::Emsmdb {
        return;
    }
    let state = &session.post_hierarchy_actions;
    if state.last_common_views_inbox_shortcut_context.is_empty()
        || state.inbox_associated_contents_table_observed
        || state.inbox_normal_contents_table_observed
    {
        return;
    }

    let notification_registered = !state
        .last_inbox_notification_registration_context
        .is_empty();
    let handoff_phase = if notification_registered {
        "post_common_views_notification_handoff"
    } else {
        "post_common_views_inbox_handoff"
    };
    let next_expected_client_step = if notification_registered {
        "notification_wait_or_open_inbox_associated_or_normal_contents_table"
    } else {
        "open_inbox_or_register_notification"
    };
    let cookie_debug = request_cookie_transport_debug(endpoint, headers);
    let session_cookie_debug = cookie_value_debug(Some(session_id));
    let request_sequence_cookie_matches =
        request_sequence_cookie_matches(endpoint, headers, session_id);
    let notification_subscription_count = session
        .handles
        .values()
        .filter(|object| matches!(object, MapiObject::NotificationSubscription { .. }))
        .count();
    let startup_gates = outlook_startup_gate_summary(session);
    let normal_inbox_missing_reason = normal_inbox_visible_row_missing_reason(session);
    let normal_inbox_release_request_shape =
        normal_inbox_visible_row_release_request_shape(session);
    let advertised_default_view_pending_open = session.advertised_default_view_pending_open();
    let default_view_advertisement_state = session.default_view_advertisement_state();
    let default_view_advertisement_summary = session.default_view_advertisement_summary();
    let post_handoff_context = format_inbox_post_fai_handoff_context(state);
    let live_handle_summaries = format_live_handle_debug_summary(session);

    tracing::info!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = "emsmdb",
        tenant_id = %principal.tenant_id,
        account_id = %principal.account_id,
        mailbox = %principal.email,
        request_type = "Execute",
        mapi_request_id = request_id,
        handoff_phase = handoff_phase,
        request_rop_names = %request.names_csv,
        response_rop_names = %response.names_csv,
        response_rop_results = %response.results_csv,
        response_body_bytes = response_body_bytes,
        cached_execute_response = cached_execute_response,
        selected_context_hash = %cookie_debug.selected_context_hash,
        selected_sequence_hash = %cookie_debug.selected_sequence_hash,
        session_id_hash = %session_cookie_debug.hash,
        request_sequence_cookie_matches = request_sequence_cookie_matches,
        notification_subscription_count = notification_subscription_count,
        outlook_startup_last_successful_gate = startup_gates.last_successful_gate,
        outlook_startup_first_missing_gate = startup_gates.first_missing_gate,
        outlook_startup_passed_gate_count = startup_gates.passed_count,
        normal_inbox_visible_row_missing_reason = normal_inbox_missing_reason,
        normal_inbox_visible_row_release_request_shape =
            %normal_inbox_release_request_shape,
        normal_inbox_table_observed =
            session
                .post_hierarchy_actions
                .inbox_normal_contents_table_observed,
        normal_inbox_setcolumns_observed =
            session
                .post_hierarchy_actions
                .inbox_normal_contents_table_setcolumns_observed,
        normal_inbox_query_rows_observed =
            session
                .post_hierarchy_actions
                .inbox_normal_contents_table_query_rows_observed,
        normal_inbox_find_row_observed =
            session
                .post_hierarchy_actions
                .inbox_normal_contents_table_find_row_observed,
        advertised_default_view_pending_open,
        default_view_advertisement_state = %default_view_advertisement_state,
        default_view_advertisement_summary = %default_view_advertisement_summary,
        post_handoff_context = %post_handoff_context,
        live_handle_summaries = %live_handle_summaries,
        next_expected_client_step = next_expected_client_step,
        "rca debug mapi post common views execute response handoff transport"
    );

    let tenant_id = principal.tenant_id.to_string();
    let account_id = principal.account_id.to_string();
    write_outlook_trace(&OutlookTraceEvent {
        component: "mapi",
        endpoint: "emsmdb",
        session_key: session_id,
        direction: OutlookTraceDirection::Outbound,
        phase: "ExecutePostCommonViewsHandoff",
        remote_peer: None,
        tenant_id: Some(&tenant_id),
        account: Some(&principal.email),
        status: Some(200),
        metadata: vec![
            ("protocol_event", "false".to_string()),
            ("diagnostic_stream", "post_common_views_handoff".to_string()),
            ("account_id", account_id),
            ("mapi_request_id", request_id.to_string()),
            ("handoff_phase", handoff_phase.to_string()),
            ("request_rop_ids", request.ids_csv.clone()),
            ("request_rop_names", request.names_csv.clone()),
            ("response_rop_ids", response.ids_csv.clone()),
            ("response_rop_names", response.names_csv.clone()),
            ("response_rop_results", response.results_csv.clone()),
            ("response_body_bytes", response_body_bytes.to_string()),
            (
                "cached_execute_response",
                cached_execute_response.to_string(),
            ),
            (
                "cookie_header_count",
                cookie_debug.cookie_header_count.to_string(),
            ),
            (
                "mapi_context_candidate_count",
                cookie_debug.context_candidate_count.to_string(),
            ),
            (
                "mapi_sequence_candidate_count",
                cookie_debug.sequence_candidate_count.to_string(),
            ),
            ("selected_context_hash", cookie_debug.selected_context_hash),
            (
                "selected_sequence_hash",
                cookie_debug.selected_sequence_hash,
            ),
            ("session_id_hash", session_cookie_debug.hash),
            (
                "request_sequence_cookie_matches",
                request_sequence_cookie_matches.to_string(),
            ),
            (
                "notification_subscription_count",
                notification_subscription_count.to_string(),
            ),
            (
                "outlook_startup_last_successful_gate",
                startup_gates.last_successful_gate.to_string(),
            ),
            (
                "outlook_startup_first_missing_gate",
                startup_gates.first_missing_gate.to_string(),
            ),
            (
                "outlook_startup_passed_gate_count",
                startup_gates.passed_count.to_string(),
            ),
            (
                "normal_inbox_visible_row_missing_reason",
                normal_inbox_missing_reason.to_string(),
            ),
            (
                "normal_inbox_visible_row_release_request_shape",
                normal_inbox_release_request_shape,
            ),
            (
                "normal_inbox_table_observed",
                session
                    .post_hierarchy_actions
                    .inbox_normal_contents_table_observed
                    .to_string(),
            ),
            (
                "normal_inbox_setcolumns_observed",
                session
                    .post_hierarchy_actions
                    .inbox_normal_contents_table_setcolumns_observed
                    .to_string(),
            ),
            (
                "normal_inbox_query_rows_observed",
                session
                    .post_hierarchy_actions
                    .inbox_normal_contents_table_query_rows_observed
                    .to_string(),
            ),
            (
                "normal_inbox_find_row_observed",
                session
                    .post_hierarchy_actions
                    .inbox_normal_contents_table_find_row_observed
                    .to_string(),
            ),
            (
                "advertised_default_view_pending_open",
                advertised_default_view_pending_open.to_string(),
            ),
            (
                "default_view_advertisement_state",
                default_view_advertisement_state,
            ),
            (
                "default_view_advertisement_summary",
                default_view_advertisement_summary,
            ),
            ("post_handoff_context", post_handoff_context),
            ("live_handle_summaries", live_handle_summaries),
            (
                "next_expected_client_step",
                next_expected_client_step.to_string(),
            ),
        ],
        payload: None,
    });
}

const EXECUTE_ACTIVE_SESSION_RETRY_ATTEMPTS: usize = 50;
const EXECUTE_ACTIVE_SESSION_RETRY_DELAY_MS: u64 = 10;
pub(in crate::mapi) const EXECUTE_FLAG_CHAIN: u32 = 0x0000_0004;

pub(in crate::mapi) struct ExecuteRequest {
    pub(in crate::mapi) flags: u32,
    pub(in crate::mapi) rop_buffer: Vec<u8>,
    pub(in crate::mapi) max_rop_out: u32,
}

pub(in crate::mapi) fn parse_execute_request(body: &[u8]) -> Result<ExecuteRequest> {
    let mut cursor = Cursor::new(body);
    let flags = cursor.read_u32()?;
    let rop_buffer_size = cursor.read_u32()? as usize;
    let rop_buffer = cursor.read_bytes(rop_buffer_size)?.to_vec();
    let max_rop_out = cursor.read_u32()?;
    let auxiliary_buffer_size = cursor.read_u32()? as usize;
    let _auxiliary_buffer = cursor.read_bytes(auxiliary_buffer_size)?;
    Ok(ExecuteRequest {
        flags,
        rop_buffer,
        max_rop_out,
    })
}

pub(super) async fn acquire_execute_active_session_request(
    session_id: &str,
) -> Option<ActiveSessionRequest> {
    for attempt in 0..EXECUTE_ACTIVE_SESSION_RETRY_ATTEMPTS {
        if let Some(active_request) = begin_active_session_request(session_id) {
            return Some(active_request);
        }
        if attempt + 1 < EXECUTE_ACTIVE_SESSION_RETRY_ATTEMPTS {
            tokio::time::sleep(std::time::Duration::from_millis(
                EXECUTE_ACTIVE_SESSION_RETRY_DELAY_MS,
            ))
            .await;
        }
    }
    None
}

pub(super) fn rop_buffer_is_store_independent_release_only(rop_buffer: &[u8]) -> bool {
    let Some((requests, _handle_table)) = split_rop_buffer(rop_buffer) else {
        return false;
    };
    let mut cursor = Cursor::new(requests);
    let mut saw_request = false;
    while cursor.remaining() > 0 {
        let Ok(request) = read_rop_request(&mut cursor) else {
            return false;
        };
        if !matches!(RopId::from_u8(request.rop_id), Some(RopId::Release)) {
            return false;
        }
        saw_request = true;
    }
    saw_request
}

pub(in crate::mapi) fn execute_can_skip_identity_scope(
    rop_buffer: &[u8],
    session: &MapiSession,
) -> bool {
    // A release-only Execute can carry RopNotify. Its FolderId must be encoded
    // with the durable special-folder identity for this mailbox, so it cannot
    // bypass the request-scoped identity codec while a notification target is
    // active.
    (rop_buffer_has_no_requests(rop_buffer)
        || rop_buffer_is_store_independent_release_only(rop_buffer))
        && !session.has_notification_targets()
}

#[cfg(test)]
pub(super) fn rop_buffer_is_store_independent_special_folder_getprops_probe(
    rop_buffer: &[u8],
    session: &MapiSession,
) -> bool {
    let Some((requests, handle_table)) = split_rop_buffer(rop_buffer) else {
        return false;
    };
    let Ok(handle_slots) = read_handle_table(handle_table) else {
        return false;
    };
    let mut opened_probe_folder_by_index = HashMap::new();
    let mut saw_open_folder = false;
    let mut saw_get_properties = false;
    let mut cursor = Cursor::new(requests);
    while cursor.remaining() > 0 {
        let Ok(request) = read_rop_request(&mut cursor) else {
            return false;
        };
        match RopId::from_u8(request.rop_id) {
            Some(RopId::Release) => {}
            Some(RopId::OpenFolder) => {
                let folder_id = session
                    .resolve_special_folder_alias(request.folder_id().unwrap_or(ROOT_FOLDER_ID));
                if !is_store_independent_special_folder(folder_id) {
                    return false;
                }
                opened_probe_folder_by_index
                    .insert(request.output_handle_index.unwrap_or(0), folder_id);
                saw_open_folder = true;
            }
            Some(RopId::GetPropertiesSpecific) => {
                let property_tags = request.property_tags();
                if property_tags.iter().copied().any(is_custom_property_tag) {
                    return false;
                }
                let input_handle_index = request.input_handle_index().unwrap_or(0);
                let opened_folder_id = opened_probe_folder_by_index
                    .get(&input_handle_index)
                    .copied();
                let existing_folder_id = input_handle(&handle_slots, &request)
                    .and_then(|handle| session.handles.get(&handle))
                    .and_then(MapiObject::folder_id);
                let Some(folder_id) = opened_folder_id.or(existing_folder_id) else {
                    return false;
                };
                if !is_store_independent_folder_getprops_probe(folder_id, &property_tags) {
                    return false;
                }
                saw_get_properties = true;
            }
            _ => return false,
        }
    }
    saw_open_folder && saw_get_properties
}

#[cfg(test)]
fn is_store_independent_folder_getprops_probe(folder_id: u64, property_tags: &[u32]) -> bool {
    is_store_independent_special_folder(folder_id)
        && !property_tags
            .iter()
            .any(|tag| strips_default_folder_identification_value_for_folder_id(folder_id, *tag))
}

#[cfg(test)]
fn is_store_independent_special_folder(folder_id: u64) -> bool {
    matches!(
        folder_id,
        ROOT_FOLDER_ID
            | COMMON_VIEWS_FOLDER_ID
            | SCHEDULE_FOLDER_ID
            | SEARCH_FOLDER_ID
            | VIEWS_FOLDER_ID
            | SHORTCUTS_FOLDER_ID
            | FREEBUSY_DATA_FOLDER_ID
    )
}

pub(super) fn rop_buffer_has_no_requests(rop_buffer: &[u8]) -> bool {
    split_rop_buffer(rop_buffer)
        .map(|(requests, _handle_table)| requests.is_empty())
        .unwrap_or(false)
}

pub(super) fn execute_success_rop_buffer(body: &[u8]) -> Option<&[u8]> {
    let mut cursor = Cursor::new(body);
    cursor.read_u32().ok()?;
    cursor.read_u32().ok()?;
    cursor.read_u32().ok()?;
    let rop_buffer_size = cursor.read_u32().ok()? as usize;
    cursor.read_bytes(rop_buffer_size).ok()
}

pub(super) fn apply_execute_max_rop_out(
    request_id: &str,
    request_rop_buffer: &[u8],
    response_rop_buffer: Vec<u8>,
    max_rop_out: u32,
) -> Vec<u8> {
    if !execute_response_exceeds_max_rop_out(&response_rop_buffer, max_rop_out) {
        return response_rop_buffer;
    }
    let Some((requests, handle_table)) = split_rop_buffer(request_rop_buffer) else {
        return response_rop_buffer;
    };
    let replacement =
        rop_buffer_too_small_response(response_rop_buffer.len(), requests, handle_table);
    // [MS-OXCRPC] 3.1.4.2 and 3.1.4.2.1.1.2 require an extended rgbOut
    // response to retain RPC_HEADER_EXT around its ROP response payload.
    let replacement = if is_rpc_header_ext_rop_buffer(request_rop_buffer) {
        rpc_header_ext_rop_buffer(replacement)
    } else {
        replacement
    };
    tracing::info!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = "emsmdb",
        request_type = "Execute",
        mapi_request_id = request_id,
        max_rop_out,
        response_rop_buffer_size = response_rop_buffer.len(),
        replacement_rop_buffer_size = replacement.len(),
        "rca debug mapi execute max rop out exceeded"
    );
    replacement
}

pub(super) fn execute_response_exceeds_max_rop_out(
    response_rop_buffer: &[u8],
    max_rop_out: u32,
) -> bool {
    max_rop_out != 0 && response_rop_buffer.len() > max_rop_out as usize
}

pub(super) fn restore_pending_notifications_after_execute_overflow(
    session: &mut MapiSession,
    mut delivered_notification_events: VecDeque<MapiNotificationEvent>,
    response_rop_buffer: &[u8],
    max_rop_out: u32,
) {
    if !execute_response_exceeds_max_rop_out(response_rop_buffer, max_rop_out) {
        return;
    }

    // [MS-OXCNOTIF] section 3.1.5.7 keeps notifications available when their
    // RopNotify responses do not fit. Execute will return RopBufferTooSmall,
    // so retain this batch for a later successful Execute rather than lose it.
    delivered_notification_events.append(&mut session.pending_notifications);
    session.pending_notifications = delivered_notification_events;
}

pub(super) fn available_execute_rop_response_size(
    max_rop_out: u32,
    extended: bool,
    preceding_response_size: usize,
    response_handle_count: usize,
) -> usize {
    if max_rop_out == 0 {
        return usize::MAX;
    }
    let framing_size = if extended { 8usize } else { 0 };
    (max_rop_out as usize)
        .saturating_sub(framing_size)
        .saturating_sub(2)
        .saturating_sub(preceding_response_size)
        .saturating_sub(response_handle_count.saturating_mul(4))
}

pub(super) fn execute_response_handle_table(
    responses: &[u8],
    handle_slots: &[u32],
    output_handles: &[u32],
    response_handle_indexes: &[u8],
    echo_input_handle_table: bool,
    released_handle_indexes: &[u8],
) -> Vec<u32> {
    if responses.is_empty() && !echo_input_handle_table {
        return Vec::new();
    }
    let mut handles = response_handle_table_with_released_handle_sentinel(
        handle_slots,
        output_handles,
        echo_input_handle_table,
        if echo_input_handle_table {
            released_handle_indexes
        } else {
            &[]
        },
    );
    if !responses.is_empty() {
        if let Some(max_response_handle_index) = response_handle_indexes.iter().copied().max() {
            let required_len = usize::from(max_response_handle_index) + 1;
            if handles.len() > required_len {
                handles.truncate(required_len);
            }
            while handles.len() < required_len {
                handles.push(u32::MAX);
            }
        }
    }
    handles
}

pub(super) fn parse_execute_rop_dispatch_input(
    rop_buffer: &[u8],
) -> Result<(&[u8], Vec<u32>, bool), Vec<u8>> {
    let Some((requests, handle_table)) = split_rop_buffer(rop_buffer) else {
        return Err(rop_buffer_with_response(rop_parse_error_response(), &[]));
    };
    let extended = is_rpc_header_ext_rop_buffer(rop_buffer);
    match read_handle_table(handle_table) {
        Ok(handle_slots) => Ok((requests, handle_slots, extended)),
        Err(_) => {
            let response = if extended {
                rop_buffer_with_response_spec(rop_parse_error_response(), &[])
            } else {
                rop_buffer_with_response(rop_parse_error_response(), &[])
            };
            Err(if extended {
                rpc_header_ext_rop_buffer(response)
            } else {
                response
            })
        }
    }
}

pub(super) fn record_execute_stream_batch_observation(
    principal: &AccountPrincipal,
    request_id: &str,
    request_rop_names: &str,
    request_handle_table_summary: &str,
    session: &mut MapiSession,
) {
    if request_rop_names != "SetProperties,OpenStream,SetStreamSize,WriteStream,CommitStream" {
        return;
    }
    let summary = format!(
        "request_id={request_id};request_rops={request_rop_names};handles={request_handle_table_summary}"
    );
    session.record_outlook_stream_batch_observed(summary.clone());
    session.record_outlook_view_failure_trace_event(format!("stream_batch_observed:{summary}"));
    tracing::info!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = "emsmdb",
        mailbox = %principal.email,
        request_type = "Execute",
        mapi_request_id = request_id,
        request_rop_names = %request_rop_names,
        input_handle_table_summary = %request_handle_table_summary,
        stream_batch_observed = true,
        "rca debug outlook stream batch observed"
    );
}

pub(super) fn read_next_execute_rop_request(
    cursor: &mut Cursor<'_>,
    responses: &mut Vec<u8>,
) -> Option<(RopRequest, u8)> {
    if cursor.remaining_is_zero_padding() {
        return None;
    }
    match read_rop_request_with_logon_id(cursor) {
        Ok(request) => Some(request),
        Err(_) => {
            responses.extend_from_slice(&rop_parse_error_response());
            None
        }
    }
}

pub(super) fn finalize_execute_rop_buffer(
    responses: Vec<u8>,
    handle_slots: &[u32],
    output_handles: &[u32],
    response_handle_indexes: &[u8],
    echo_input_handle_table: bool,
    released_handle_indexes: &[u8],
    extended: bool,
) -> Vec<u8> {
    let response_handles = execute_response_handle_table(
        &responses,
        handle_slots,
        output_handles,
        response_handle_indexes,
        echo_input_handle_table,
        released_handle_indexes,
    );
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

pub(super) fn record_execute_sync_observations(
    session: &mut MapiSession,
    completed_hierarchy_sync: Option<(u64, String, String)>,
    content_sync_configure_observed: bool,
) {
    if let Some((
        sync_root_folder_id,
        get_buffer_summary,
        default_folder_hierarchy_membership_summary,
    )) = completed_hierarchy_sync
    {
        session.record_completed_hierarchy_sync(
            sync_root_folder_id,
            get_buffer_summary,
            default_folder_hierarchy_membership_summary,
        );
    }
    if content_sync_configure_observed {
        session.record_content_sync_configure();
    }
}

pub(super) fn abort_response(request: &RopRequest, input_object: Option<&MapiObject>) -> Vec<u8> {
    let result = match input_object {
        Some(MapiObject::HierarchyTable { .. } | MapiObject::ContentsTable { .. }) => 0x8004_0114,
        _ => 0x8004_0102,
    };
    rop_error_response(0x38, request.response_handle_index(), result)
}

pub(super) fn append_abort_response(
    request: &RopRequest,
    input_object: Option<&MapiObject>,
    responses: &mut Vec<u8>,
) {
    responses.extend_from_slice(&abort_response(request, input_object));
}

pub(super) fn progress_response(
    request: &RopRequest,
    input_object: Option<&MapiObject>,
) -> Vec<u8> {
    let result = if !matches!(request.payload.first().copied(), Some(0x00 | 0x01)) {
        0x8007_0057
    } else {
        match input_object {
            Some(MapiObject::HierarchyTable { .. } | MapiObject::ContentsTable { .. }) => {
                0x8004_0400
            }
            _ => 0x8004_0102,
        }
    };
    rop_error_response(0x50, request.response_handle_index(), result)
}

pub(super) fn append_progress_response(
    request: &RopRequest,
    input_object: Option<&MapiObject>,
    responses: &mut Vec<u8>,
) {
    responses.extend_from_slice(&progress_response(request, input_object));
}

pub(super) fn reset_table_response(request: &RopRequest, reset_succeeded: bool) -> Vec<u8> {
    if reset_succeeded {
        rop_reset_table_response(request)
    } else {
        rop_error_response(0x81, request.response_handle_index(), 0x8004_0102)
    }
}

pub(super) fn append_reset_table_response(
    request: &RopRequest,
    reset_succeeded: bool,
    responses: &mut Vec<u8>,
) {
    responses.extend_from_slice(&reset_table_response(request, reset_succeeded));
}

pub(super) fn append_execute_status_response(
    session: &mut MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    responses: &mut Vec<u8>,
) {
    match RopId::from_u8(request.rop_id) {
        Some(RopId::Abort) => append_abort_response(
            request,
            input_object(session, handle_slots, request),
            responses,
        ),
        Some(RopId::Progress) => append_progress_response(
            request,
            input_object(session, handle_slots, request),
            responses,
        ),
        Some(RopId::ResetTable) => {
            let handle = input_handle(handle_slots, request);
            let reset_succeeded =
                input_object_mut(session, handle_slots, request).is_some_and(reset_table_state);
            if reset_succeeded {
                session.deactivate_table_notifications(handle);
            }
            append_reset_table_response(request, reset_succeeded, responses);
        }
        _ => {}
    }
}

pub(super) fn unknown_property_wire_type_response(
    principal: &AccountPrincipal,
    request: &RopRequest,
) -> Option<Vec<u8>> {
    if !matches!(request.rop_id, 0x07 | 0x0B | 0x7A)
        || property_tags_have_known_wire_types(&request.property_tags())
    {
        return None;
    }
    tracing::info!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = "emsmdb",
        mailbox = %principal.email,
        request_type = "Execute",
        request_rop_id = %format!("{:#04x}", request.rop_id),
        input_handle_index = request.input_handle_index().unwrap_or(0),
        property_tags = %format_debug_property_tags(&request.property_tags()),
        failure_reason = "unknown_property_wire_type",
        "rca debug mapi property rop rejected"
    );
    Some(rop_error_response(
        request.rop_id,
        request.response_handle_index(),
        0x8004_0102,
    ))
}
