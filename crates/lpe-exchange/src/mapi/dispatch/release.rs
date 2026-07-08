use super::*;

pub(super) fn is_release_dispatch_rop(rop_id: RopId) -> bool {
    matches!(rop_id, RopId::Release)
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn append_release_dispatch_response<S: ExchangeStore>(
    store: &S,
    principal: &AccountPrincipal,
    request_id: &str,
    request_rop_names: &str,
    session: &mut MapiSession,
    handle_slots: &mut Vec<u32>,
    request: &RopRequest,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    same_execute_released_handles: &mut HashSet<u32>,
    post_hierarchy_release_events: &mut Vec<PostHierarchyReleaseDebugEvent>,
) -> bool {
    if matches!(RopId::from_u8(request.rop_id), Some(RopId::Release)) {
        append_release_response(
            store,
            principal,
            request_id,
            request_rop_names,
            session,
            handle_slots,
            request,
            mailboxes,
            emails,
            snapshot,
            same_execute_released_handles,
            post_hierarchy_release_events,
        )
        .await;
        true
    } else {
        false
    }
}

pub(super) async fn append_release_response<S: ExchangeStore>(
    store: &S,
    principal: &AccountPrincipal,
    request_id: &str,
    request_rop_names: &str,
    session: &mut MapiSession,
    handle_slots: &mut Vec<u32>,
    request: &RopRequest,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    same_execute_released_handles: &mut HashSet<u32>,
    post_hierarchy_release_events: &mut Vec<PostHierarchyReleaseDebugEvent>,
) {
    let released_handle = input_handle(&handle_slots, &request);
    let released_object = input_object(session, &handle_slots, &request);
    let released_object_for_stream_persist = released_object.cloned();
    let released_object_kind = mapi_object_debug_kind(released_object);
    let released_folder_id = mapi_object_debug_folder_id(released_object);
    let released_folder_role = released_object
        .and_then(MapiObject::folder_id)
        .map(debug_role_for_folder_id)
        .unwrap_or_default();
    let released_associated_contents_table = matches!(
        released_object,
        Some(MapiObject::ContentsTable {
            associated: true,
            ..
        })
    );
    let inbox_related_release_context = format_inbox_related_release_context(
        released_object,
        released_handle,
        &session.post_hierarchy_actions,
        snapshot,
    );
    let inbox_related_release_context_for_log =
        inbox_related_release_context.clone().unwrap_or_default();
    let visible_inbox_release_without_query_rows = match released_object {
        Some(MapiObject::ContentsTable {
            folder_id,
            associated,
            columns,
            position,
            restriction,
            sort_orders,
            ..
        }) if *folder_id == INBOX_FOLDER_ID
            && !*associated
            && !columns.is_empty()
            && session
                .post_hierarchy_actions
                .last_inbox_normal_contents_table_setcolumns_handle
                == released_handle
            && session
                .post_hierarchy_actions
                .last_inbox_normal_contents_table_query_rows_handle
                != released_handle =>
        {
            let release_request_metrics = format_visible_inbox_release_request_metrics(
                request,
                request_rop_names,
                handle_slots,
                released_handle,
                same_execute_released_handles,
                session,
            );
            Some(format!(
                "{release_request_metrics};request_id={request_id};request_rops={request_rop_names};handle={};folder=0x{folder_id:016x};position={};row_count={};columns={};column_support={};normal_message_defaulted_column_detail={};sort={};restriction={};last_setcolumns={};last_query_rows={};view_handoff={};table_compatibility={};descriptor_behavior={};descriptor_query_window={};default_view_advertisement_state={};live_handles_before_release={}",
                format_optional_debug_handle(released_handle),
                position,
                folder_message_count(*folder_id, mailboxes, emails, snapshot),
                format_debug_property_tags(columns),
                normal_message_table_column_support_summary(columns),
                normal_message_defaulted_column_detail(columns),
                format_debug_sort_orders(sort_orders),
                format_debug_restriction_option(restriction.as_ref()),
                debug_context_or_none(
                    &session
                        .post_hierarchy_actions
                        .last_inbox_normal_contents_table_setcolumns_context
                ),
                debug_context_or_none(
                    &session
                        .post_hierarchy_actions
                        .last_inbox_normal_contents_table_query_rows_context
                ),
                format_outlook_view_handoff_table_contract(
                    *folder_id,
                    *associated,
                    columns,
                    snapshot,
                ),
                format_default_view_table_compatibility_contract(
                    *folder_id,
                    *associated,
                    columns,
                    sort_orders,
                    restriction.as_ref(),
                    snapshot,
                ),
                format_inbox_view_descriptor_set_columns_behavior_contract(
                    *folder_id,
                    *associated,
                    columns,
                    snapshot,
                ),
                format_inbox_view_descriptor_behavior_contract(
                    *folder_id,
                    *associated,
                    *position,
                    true,
                    40,
                    sort_orders,
                    restriction.as_ref(),
                    columns,
                    mailboxes,
                    emails,
                    snapshot,
                ),
                session.default_view_advertisement_state_for_folder(*folder_id),
                format_live_handle_debug_summary(session)
            ))
        }
        _ => None,
    };
    let calendar_normal_release_context = match released_object {
        Some(MapiObject::ContentsTable {
            folder_id,
            associated,
            columns,
            position,
            restriction,
            sort_orders,
            ..
        }) if *folder_id == CALENDAR_FOLDER_ID && !*associated => Some(format!(
            "request_id={request_id};request_rops={request_rop_names};handle={};position={};row_count={};columns={};sort={};restriction={};view_handoff={}",
            format_optional_debug_handle(released_handle),
            position,
            folder_message_count(*folder_id, mailboxes, emails, snapshot),
            format_debug_property_tags(columns),
            format_debug_sort_orders(sort_orders),
            format_debug_restriction_option(restriction.as_ref()),
            format_outlook_view_handoff_table_contract(
                *folder_id,
                *associated,
                columns,
                snapshot,
            )
        )),
        _ => None,
    };
    let post_inbox_fai_handoff_context = match released_object {
        Some(MapiObject::ContentsTable {
            folder_id,
            associated,
            columns,
            position,
            restriction,
            sort_orders,
            ..
        }) if *folder_id == INBOX_FOLDER_ID
            && *associated
            && session
                .post_hierarchy_actions
                .inbox_associated_contents_table_observed
            && !session
                .post_hierarchy_actions
                .inbox_associated_findrow_returned_content
            && !session
                .post_hierarchy_actions
                .inbox_associated_query_rows_returned_non_empty
            && !session
                .post_hierarchy_actions
                .inbox_normal_contents_table_observed
            && !session.post_hierarchy_actions.post_inbox_fai_handoff_logged =>
        {
            let filtered_row_count = restricted_associated_folder_message_count(
                *folder_id,
                snapshot,
                restriction.as_ref(),
                principal.account_id,
            );
            let unfiltered_row_count = associated_folder_message_count(*folder_id, snapshot);
            Some((
                format!(
                    "handle={};folder=0x{folder_id:016x};position={position};columns={};sort={};restriction={};filtered_row_count={};unfiltered_row_count={};handoff_visibility={}",
                    format_optional_debug_handle(released_handle),
                    format_debug_property_tags(columns),
                    format_debug_sort_orders(sort_orders),
                    restriction
                        .as_ref()
                        .map(format_debug_parsed_restriction)
                        .unwrap_or_default(),
                    filtered_row_count,
                    unfiltered_row_count,
                    format_inbox_fai_handoff_visibility_context(
                        snapshot,
                        restriction.as_ref(),
                        principal.account_id,
                    )
                ),
                format_inbox_post_fai_handoff_context(
                    &session.post_hierarchy_actions,
                ),
                format_live_handle_debug_summary(session),
            ))
        }
        _ => None,
    };
    let post_fai_hierarchy_release_without_inbox_contents =
        format_post_fai_hierarchy_release_without_inbox_contents_context(
            released_object,
            released_handle,
            &session.post_hierarchy_actions,
            mailboxes,
            snapshot,
        );
    if session.hierarchy_sync_completed() {
        let remaining_before = session.handles.len();
        post_hierarchy_release_events.push(PostHierarchyReleaseDebugEvent {
            input_handle_index: request.input_handle_index().unwrap_or(0),
            handle: format_optional_debug_handle(released_handle),
            object_kind: released_object_kind.to_string(),
            folder_id: released_folder_id.clone(),
            remaining_before,
            remaining_after: remaining_before,
            logon_before_content_sync: matches!(
                released_object,
                Some(MapiObject::Logon | MapiObject::PublicFolderLogon)
            ) && !session
                .post_hierarchy_actions
                .content_sync_configure_observed,
        });
    }
    if matches!(
        released_object,
        Some(MapiObject::Logon | MapiObject::PublicFolderLogon)
    ) {
        session.record_logoff_after_hierarchy_completion();
    }
    if let Err(error) = persist_released_associated_config_stream(
        store,
        principal,
        session,
        released_object_for_stream_persist.as_ref(),
    )
    .await
    {
        tracing::warn!(
            rca_debug = true,
            adapter = "mapi",
            endpoint = "emsmdb",
            mailbox = %principal.email,
            request_type = "Execute",
            request_rop_id = "0x01",
            input_handle_index = request.input_handle_index().unwrap_or(0),
            input_handle_value = %format_optional_debug_handle(released_handle),
            error = %error,
            "mapi associated config stream release persist failed"
        );
    }
    release_handle_slot(session, handle_slots, &request);
    if let Some(handle) = released_handle {
        same_execute_released_handles.insert(handle);
    }
    if let Some(context) = inbox_related_release_context {
        session.record_last_inbox_related_release_context(context);
    }
    session.record_last_table_release_context(format!(
        "phase=release;request_id={request_id};request_rops={request_rop_names};input_index={};handle={};kind={};folder={};role={};associated={}",
        request.input_handle_index().unwrap_or(0),
        format_optional_debug_handle(released_handle),
        released_object_kind,
        released_folder_id,
        released_folder_role,
        released_associated_contents_table
    ));
    if let Some(context) = visible_inbox_release_without_query_rows {
        let has_defaulted_columns =
            context.contains(";defaulted=0x") || context.contains("backed=false");
        session.record_outlook_view_failure_trace_event(format!(
            "visible_inbox_release_without_query_rows:{context}"
        ));
        if !session
            .post_hierarchy_actions
            .last_outlook_umolk_named_property_probe_context
            .is_empty()
            && !session
                .post_hierarchy_actions
                .outlook_umolk_visible_inbox_release_logged
        {
            session
                .post_hierarchy_actions
                .outlook_umolk_visible_inbox_release_logged = true;
            tracing::info!(
                rca_debug = true,
                adapter = "mapi",
                endpoint = "emsmdb",
                mailbox = %principal.email,
                request_type = "Execute",
                mapi_request_id = %request_id,
                request_rop_id = "0x01",
                input_handle_index = request.input_handle_index().unwrap_or(0),
                input_handle_value = %format_optional_debug_handle(released_handle),
                umolk_named_property_probe_context = %session
                    .post_hierarchy_actions
                    .last_outlook_umolk_named_property_probe_context,
                umolk_getprops_materialization_context = %debug_context_or_none(
                    &session
                        .post_hierarchy_actions
                        .last_outlook_umolk_getprops_materialization_context
                ),
                release_without_query_rows_context = %context,
                content_sync_started_after_hierarchy = session
                    .post_hierarchy_actions
                    .content_sync_configure_observed,
                "rca debug mapi umolk to visible inbox release before query rows"
            );
        }
        if has_defaulted_columns {
            tracing::warn!(
                rca_debug = true,
                adapter = "mapi",
                endpoint = "emsmdb",
                mailbox = %principal.email,
                request_type = "Execute",
                mapi_request_id = %request_id,
                request_rop_id = "0x01",
                input_handle_index = request.input_handle_index().unwrap_or(0),
                input_handle_value = %format_optional_debug_handle(released_handle),
                release_without_query_rows_context = %context,
                live_handles_before_release = %format_live_handle_debug_summary(session),
                last_query_position_before_release = %debug_context_or_none(
                    &session
                        .post_hierarchy_actions
                        .last_inbox_normal_contents_table_query_position_context
                ),
                recent_actions_before_release =
                    %session.post_hierarchy_actions.recent_probe_actions.join(">"),
                "rca debug mapi visible inbox released before query rows"
            );
        } else {
            tracing::info!(
                rca_debug = true,
                adapter = "mapi",
                endpoint = "emsmdb",
                mailbox = %principal.email,
                request_type = "Execute",
                mapi_request_id = %request_id,
                request_rop_id = "0x01",
                input_handle_index = request.input_handle_index().unwrap_or(0),
                input_handle_value = %format_optional_debug_handle(released_handle),
                release_without_query_rows_context = %context,
                live_handles_before_release = %format_live_handle_debug_summary(session),
                last_query_position_before_release = %debug_context_or_none(
                    &session
                        .post_hierarchy_actions
                        .last_inbox_normal_contents_table_query_position_context
                ),
                recent_actions_before_release =
                    %session.post_hierarchy_actions.recent_probe_actions.join(">"),
                "rca debug mapi visible inbox released before query rows"
            );
        }
    }
    if let Some(context) = calendar_normal_release_context {
        session
            .record_outlook_view_failure_trace_event(format!("calendar_normal_release:{context}"));
        tracing::info!(
            rca_debug = true,
            adapter = "mapi",
            endpoint = "emsmdb",
            mailbox = %principal.email,
            request_type = "Execute",
            request_rop_id = "0x01",
            input_handle_index = request.input_handle_index().unwrap_or(0),
            input_handle_value = %format_optional_debug_handle(released_handle),
            release_context = %context,
            "rca debug mapi calendar normal released"
        );
    }
    if let Some((released_table_context, handoff_context, live_handle_summary)) =
        post_inbox_fai_handoff_context
    {
        record_mapi_outlook_view_inbox_fai_handoff_without_contents();
        record_mapi_outlook_view_bootstrap_stall(1);
        tracing::info!(
            rca_debug = true,
            adapter = "mapi",
            endpoint = "emsmdb",
            mailbox = %principal.email,
            request_type = "Execute",
            request_rop_id = "0x01",
            input_handle_index = request.input_handle_index().unwrap_or(0),
            input_handle_value = %format_optional_debug_handle(released_handle),
            released_table_context = %released_table_context,
            handoff_context = %handoff_context,
            live_handle_summaries_before_release = %live_handle_summary,
            remaining_handle_count_after_release = session.handles.len(),
            "rca debug mapi inbox associated handoff without contents"
        );
        session.mark_post_inbox_fai_handoff_logged();
    }
    if let Some(context) = post_fai_hierarchy_release_without_inbox_contents {
        record_mapi_outlook_view_post_fai_hierarchy_without_contents();
        record_mapi_outlook_view_bootstrap_stall(2);
        session.record_outlook_view_failure_trace_event(format!(
            "post_fai_hierarchy_release_without_inbox_contents:{context}"
        ));
        tracing::info!(
            rca_debug = true,
            adapter = "mapi",
            endpoint = "emsmdb",
            mailbox = %principal.email,
            request_type = "Execute",
            mapi_request_id = %request_id,
            request_rop_id = "0x01",
            input_handle_index = request.input_handle_index().unwrap_or(0),
            input_handle_value = %format_optional_debug_handle(released_handle),
            release_context = %context,
            live_handle_summaries_after_release = %format_live_handle_debug_summary(session),
            "rca debug mapi post fai hierarchy released without inbox contents"
        );
    }
    if let Some(event) = post_hierarchy_release_events.last_mut() {
        event.remaining_after = session.handles.len();
    }
    session.record_recent_probe_action(format!(
        "Release(in={},handle={},kind={},folder={})",
        request.input_handle_index().unwrap_or(0),
        format_optional_debug_handle(released_handle),
        released_object_kind,
        released_folder_id
    ));
    tracing::debug!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = "emsmdb",
        mailbox = %principal.email,
        request_type = "Execute",
        request_rop_id = "0x01",
        input_handle_index = request.input_handle_index().unwrap_or(0),
        input_handle_value = %format_optional_debug_handle(released_handle),
        object_kind = released_object_kind,
        folder_id = %released_folder_id,
        inbox_related_release_context = %inbox_related_release_context_for_log,
        remaining_handle_count = session.handles.len(),
        "rca debug mapi release before inbox probe"
    );
}

fn format_visible_inbox_release_request_metrics(
    request: &RopRequest,
    request_rop_names: &str,
    handle_slots: &[u32],
    released_handle: Option<u32>,
    same_execute_released_handles: &HashSet<u32>,
    session: &MapiSession,
) -> String {
    let rop_names: Vec<&str> = request_rop_names
        .split(',')
        .filter(|name| !name.is_empty())
        .collect();
    let release_rop_count = rop_names
        .iter()
        .filter(|name| name.eq_ignore_ascii_case("Release"))
        .count();
    let release_request_shape =
        classify_release_request_shape(request_rop_names, rop_names.len(), release_rop_count);
    let same_execute_already_released = released_handle
        .map(|handle| same_execute_released_handles.contains(&handle))
        .unwrap_or(false);
    format!(
        "release_request_shape={release_request_shape};release_input_index={};release_response_index={};release_rop_count={};release_batch_rop_count={};release_same_execute_already_released={};release_handle_slots_before={};release_live_handle_count_before={};release_query_position_seen_before_release={};release_findrow_seen_before_release={};release_query_rows_seen_before_release={};release_content_sync_seen_before_release={}",
        request.input_handle_index().unwrap_or(0),
        request.response_handle_index(),
        release_rop_count,
        rop_names.len(),
        same_execute_already_released,
        format_release_handle_slots(handle_slots),
        session.handles.len(),
        !session
            .post_hierarchy_actions
            .last_inbox_normal_contents_table_query_position_context
            .is_empty(),
        session
            .post_hierarchy_actions
            .last_inbox_normal_contents_table_find_row_handle
            .is_some(),
        session
            .post_hierarchy_actions
            .last_inbox_normal_contents_table_query_rows_handle
            .is_some(),
        session
            .post_hierarchy_actions
            .content_sync_configure_observed
    )
}

fn classify_release_request_shape(
    request_rop_names: &str,
    rop_count: usize,
    release_rop_count: usize,
) -> &'static str {
    if request_rop_names == "Release" {
        "standalone_release"
    } else if rop_count > 0 && release_rop_count == rop_count {
        "release_only_batch"
    } else if request_rop_names.contains("SetColumns") && release_rop_count > 0 {
        "mixed_setcolumns_release_batch"
    } else if release_rop_count > 0 {
        "mixed_release_batch"
    } else {
        "unknown_release_request"
    }
}

fn format_release_handle_slots(handle_slots: &[u32]) -> String {
    if handle_slots.is_empty() {
        return "empty".to_string();
    }
    handle_slots
        .iter()
        .enumerate()
        .map(|(index, handle)| format!("{index}:0x{handle:08x}"))
        .collect::<Vec<_>>()
        .join("|")
}

pub(super) fn log_post_hierarchy_release_events(
    principal: &AccountPrincipal,
    request_id: &str,
    request_rop_ids: &str,
    request_rop_names: &str,
    request_non_release_rops: &str,
    request_all_rops_are_release: bool,
    request_handle_count: usize,
    request_handle_table_summary: &str,
    session: &MapiSession,
    post_hierarchy_release_events: &[PostHierarchyReleaseDebugEvent],
    responses: &[u8],
) {
    if post_hierarchy_release_events.is_empty() {
        return;
    }
    let post_hierarchy = post_hierarchy_action_summary(session, false);
    if post_hierarchy.content_sync_configure_observed {
        tracing::info!(
            rca_debug = true,
            adapter = "mapi",
            endpoint = "emsmdb",
            tenant_id = %principal.tenant_id,
            account_id = %principal.account_id,
            mailbox = %principal.email,
            request_type = "Execute",
            mapi_request_id = request_id,
            request_rop_ids = %request_rop_ids,
            request_rop_names = %request_rop_names,
            request_non_release_rops = %request_non_release_rops,
            request_all_rops_are_release = request_all_rops_are_release,
            request_handle_count = request_handle_count,
            input_handle_table_summary = %request_handle_table_summary,
            release_rops_have_no_response_rows = true,
            response_rop_payload_bytes_before_handle_table = responses.len(),
            response_rop_payload_empty_is_expected = responses.is_empty(),
            last_completed_hierarchy_sync_root =
                %post_hierarchy.last_completed_hierarchy_sync_root,
            content_sync_started_after_hierarchy =
                post_hierarchy.content_sync_configure_observed,
            post_hierarchy_execute_count_before_record =
                post_hierarchy.execute_count,
            released_handle_count = post_hierarchy_release_events.len(),
            released_handle_kinds =
                %format_post_hierarchy_release_kinds(post_hierarchy_release_events),
            released_handle_role_counts =
                %post_sync_release_flags(post_hierarchy_release_events),
            released_logon_after_content_sync = post_hierarchy_release_events
                .iter()
                .any(|event| matches!(
                    event.object_kind.as_str(),
                    "logon" | "public_folder_logon"
                )),
            release_closes_all_live_handles = session.handles.is_empty(),
            remaining_live_handle_count = session.handles.len(),
            remaining_live_handles = %format_live_handle_debug_summary(session),
            release_context =
                %format_post_hierarchy_release_context(post_hierarchy_release_events),
            next_expected_client_step = "continue_mixed_sync_or_disconnect_after_release",
            "rca debug mapi post sync release-containing execute"
        );
    }
    if request_all_rops_are_release && post_hierarchy.content_sync_configure_observed {
        tracing::info!(
            rca_debug = true,
            adapter = "mapi",
            endpoint = "emsmdb",
            tenant_id = %principal.tenant_id,
            account_id = %principal.account_id,
            mailbox = %principal.email,
            request_type = "Execute",
            mapi_request_id = request_id,
            request_all_rops_are_release = request_all_rops_are_release,
            request_handle_count = request_handle_count,
            input_handle_table_summary = %request_handle_table_summary,
            release_rops_have_no_response_rows = true,
            response_rop_payload_bytes_before_handle_table = responses.len(),
            response_rop_payload_empty_is_expected = responses.is_empty(),
            last_completed_hierarchy_sync_root =
                %post_hierarchy.last_completed_hierarchy_sync_root,
            content_sync_started_after_hierarchy =
                post_hierarchy.content_sync_configure_observed,
            post_hierarchy_execute_count_before_record =
                post_hierarchy.execute_count,
            released_handle_count = post_hierarchy_release_events.len(),
            released_handle_kinds =
                %format_post_hierarchy_release_kinds(post_hierarchy_release_events),
            released_handle_role_counts =
                %post_sync_release_flags(post_hierarchy_release_events),
            released_logon_after_content_sync = post_hierarchy_release_events
                .iter()
                .any(|event| matches!(
                    event.object_kind.as_str(),
                    "logon" | "public_folder_logon"
                )),
            release_closes_all_live_handles = session.handles.is_empty(),
            remaining_live_handle_count = session.handles.len(),
            remaining_live_handles = %format_live_handle_debug_summary(session),
            release_context =
                %format_post_hierarchy_release_context(post_hierarchy_release_events),
            next_expected_client_step = "disconnect_or_reconnect_after_release_only_execute",
            "rca debug mapi post sync release-only execute"
        );
    }
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
            %format_post_hierarchy_release_kinds(post_hierarchy_release_events),
        released_logon_before_content_sync = post_hierarchy_release_events
            .iter()
            .any(|event| event.logon_before_content_sync),
        remaining_live_handle_count = session.handles.len(),
        release_context =
            %format_post_hierarchy_release_context(post_hierarchy_release_events),
        "rca debug mapi post hierarchy close reason context"
    );
}
