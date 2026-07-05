use super::*;

pub(super) fn is_folder_open_rop(rop_id: RopId) -> bool {
    matches!(rop_id, RopId::OpenFolder)
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn append_folder_open_dispatch_response<S: ExchangeStore>(
    store: &S,
    principal: &AccountPrincipal,
    request_id: &str,
    session: &mut MapiSession,
    handle_slots: &mut Vec<u32>,
    request: &RopRequest,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    same_execute_released_handles: &HashSet<u32>,
    responses: &mut Vec<u8>,
    output_handles: &mut Vec<u32>,
) {
    if matches!(RopId::from_u8(request.rop_id), Some(RopId::OpenFolder)) {
        append_open_folder_response(
            store,
            principal,
            request_id,
            session,
            handle_slots,
            request,
            mailboxes,
            emails,
            snapshot,
            same_execute_released_handles,
            responses,
            output_handles,
        )
        .await;
    }
}

pub(super) async fn append_open_folder_response<S: ExchangeStore>(
    store: &S,
    principal: &AccountPrincipal,
    request_id: &str,
    session: &mut MapiSession,
    handle_slots: &mut Vec<u32>,
    request: &RopRequest,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    same_execute_released_handles: &HashSet<u32>,
    responses: &mut Vec<u8>,
    output_handles: &mut Vec<u32>,
) {
    let input_handle_value = input_handle(handle_slots, request);
    let input_object_kind = mapi_object_debug_kind(input_object(session, handle_slots, request));
    let input_folder_id = mapi_object_debug_folder_id(input_object(session, handle_slots, request));
    let input_context = format_handle_lineage_context(input_object(session, handle_slots, request));
    let requested_folder_id = request.folder_id().unwrap_or(ROOT_FOLDER_ID);
    let folder_id = session.resolve_special_folder_alias(requested_folder_id);
    let mailbox_folder = folder_row_for_id(folder_id, mailboxes);
    let mailbox_folder_found = mailbox_folder.is_some();
    let collaboration_folder_found = snapshot.collaboration_folder_for_id(folder_id).is_some();
    let public_folder_found = snapshot.public_folder_for_id(folder_id).is_some();
    let search_folder_definition_found = !session.search_folder_definition_was_deleted(folder_id)
        && (snapshot
            .search_folder_definition_for_folder_id(folder_id)
            .is_some()
            || session.search_folder_definition(folder_id).is_some());
    let advertised_special_folder = is_advertised_special_folder(folder_id);
    let (folder_name, folder_role, folder_container_class) =
        debug_open_folder_metadata(folder_id, mailboxes);
    let open_folder_result = if mailbox_folder_found
        || collaboration_folder_found
        || public_folder_found
        || search_folder_definition_found
        || advertised_special_folder
    {
        "success"
    } else {
        "not_found"
    };
    tracing::info!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = "emsmdb",
        tenant_id = %principal.tenant_id,
        account_id = %principal.account_id,
        mailbox = %principal.email,
        request_type = "Execute",
        mapi_request_id = request_id,
        request_rop_id = "0x02",
        input_handle_index = request.input_handle_index().unwrap_or(0),
        response_handle_index = request.output_handle_index.unwrap_or(0),
        open_mode_flags = format!("0x{:02x}", request.payload.get(8).copied().unwrap_or(0)),
        requested_folder_id = format!("0x{requested_folder_id:016x}"),
        folder_id = format!("0x{folder_id:016x}"),
        folder_alias_resolved = requested_folder_id != folder_id,
        folder_name,
        role = folder_role,
        container_class = folder_container_class,
        mailbox_folder_found = mailbox_folder_found,
        collaboration_folder_found = collaboration_folder_found,
        public_folder_found = public_folder_found,
        search_folder_definition_found = search_folder_definition_found,
        advertised_special_folder = advertised_special_folder,
        result = open_folder_result,
        message = "rca debug mapi open folder"
    );
    tracing::debug!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = "emsmdb",
        tenant_id = %principal.tenant_id,
        account_id = %principal.account_id,
        mailbox = %principal.email,
        request_type = "Execute",
        mapi_request_id = request_id,
        request_rop_id = "0x02",
        input_handle_index = request.input_handle_index().unwrap_or(0),
        input_handle_value = %format_optional_debug_handle(input_handle_value),
        input_object_kind,
        input_folder_id,
        input_handle_context = %input_context,
        requested_folder_id = format!("0x{requested_folder_id:016x}"),
        resolved_folder_id = format!("0x{folder_id:016x}"),
        output_handle_index = request.output_handle_index.unwrap_or(0),
        "rca debug mapi open folder handle lineage"
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
        request_id,
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
        return;
    }
    let is_public_folder_ghosted = public_folder_found
        && snapshot
            .public_folder_replica_server_names(folder_id)
            .is_empty();
    session.record_opened_folder(folder_id);
    let properties =
        folder_properties_for_open(store, principal, session, folder_id, mailboxes, snapshot).await;
    tracing::info!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = "emsmdb",
        tenant_id = %principal.tenant_id,
        account_id = %principal.account_id,
        mailbox = %principal.email,
        request_type = "Execute",
        mapi_request_id = request_id,
        request_rop_id = "0x02",
        input_handle_index = request.input_handle_index().unwrap_or(0),
        response_handle_index = request.output_handle_index.unwrap_or(0),
        open_mode_flags = format!("0x{:02x}", request.payload.get(8).copied().unwrap_or(0)),
        folder_id = format!("0x{folder_id:016x}"),
        folder_name = post_hierarchy_probe_folder_name(folder_id),
        role = debug_role_for_folder_id(folder_id),
        property_count = properties.len(),
        property_shapes = %debug_open_folder_property_shapes(&properties),
        message = "rca debug mapi open folder properties"
    );
    let inbox_contract_display_name = mapi_value_debug_string(&properties, PID_TAG_DISPLAY_NAME_W);
    let inbox_contract_folder_type = mapi_value_debug_u32(&properties, PID_TAG_FOLDER_TYPE);
    let inbox_contract_container_class =
        mapi_value_debug_string(&properties, PID_TAG_CONTAINER_CLASS_W);
    let inbox_contract_record_key = mapi_value_debug_binary_decode(&properties, PID_TAG_RECORD_KEY);
    let inbox_contract_source_key = mapi_value_debug_binary_decode(&properties, PID_TAG_SOURCE_KEY);
    let inbox_contract_parent_source_key =
        mapi_value_debug_binary_decode(&properties, PID_TAG_PARENT_SOURCE_KEY);
    let inbox_contract_content_count = mapi_value_debug_u32(&properties, PID_TAG_CONTENT_COUNT);
    let inbox_contract_unread_count =
        mapi_value_debug_u32(&properties, PID_TAG_CONTENT_UNREAD_COUNT);
    let inbox_contract_subfolders = mapi_value_debug_bool(&properties, PID_TAG_SUBFOLDERS);
    let root_ipm_contract_display_name =
        mapi_value_debug_string(&properties, PID_TAG_DISPLAY_NAME_W);
    let root_ipm_contract_folder_type = mapi_value_debug_u32(&properties, PID_TAG_FOLDER_TYPE);
    let root_ipm_contract_container_class =
        mapi_value_debug_string(&properties, PID_TAG_CONTAINER_CLASS_W);
    let root_ipm_contract_record_key =
        mapi_value_debug_binary_decode(&properties, PID_TAG_RECORD_KEY);
    let root_ipm_contract_source_key =
        mapi_value_debug_binary_decode(&properties, PID_TAG_SOURCE_KEY);
    let root_ipm_contract_parent_source_key =
        mapi_value_debug_binary_decode(&properties, PID_TAG_PARENT_SOURCE_KEY);
    let handle = session.allocate_output_handle_avoiding(
        request.output_handle_index,
        MapiObject::Folder {
            folder_id,
            properties,
        },
        same_execute_released_handles,
    );
    set_handle_slot(handle_slots, request.output_handle_index, handle);
    let open_folder_response = rop_open_folder_response(request, is_public_folder_ghosted);
    if default_view_supported_folder(folder_id, &folder_container_class) {
        let context = format!(
            "request_id={request_id};handle={handle};folder=0x{folder_id:016x};role={folder_role};container_class={folder_container_class};content_count={};open_folder_response_bytes={}",
            inbox_contract_content_count,
            open_folder_response.len()
        );
        session
            .record_outlook_view_failure_trace_event(format!("default_view_folder_open:{context}"));
        tracing::info!(
            rca_debug = true,
            adapter = "mapi",
            endpoint = "emsmdb",
            mailbox = %principal.email,
            request_type = "Execute",
            mapi_request_id = request_id,
            request_rop_id = "0x02",
            folder_id = format!("0x{folder_id:016x}"),
            folder_role,
            container_class = folder_container_class,
            output_handle = handle,
            default_view_folder_open = %context,
            next_expected_client_step = "get_contents_table_or_sync_configure_for_opened_default_view_folder",
            "rca debug mapi default view folder opened"
        );
    }
    if folder_id == INBOX_FOLDER_ID {
        let first_loop_transition = format!(
            "trigger=open_folder;open_probe_before={};folder_type_probe_before={};input_index={};input_handle={};input_kind={};input_folder={};input_context={};output_index={};output_handle={};open_mode=0x{:02x};requested_folder=0x{requested_folder_id:016x};resolved_folder=0x{folder_id:016x};alias_resolved={};recent_before={}",
            session.post_hierarchy_actions.inbox_open_folder_probe_count,
            session
                .post_hierarchy_actions
                .inbox_folder_type_getprops_probe_count,
            request.input_handle_index().unwrap_or(0),
            format_optional_debug_handle(input_handle_value),
            input_object_kind,
            input_folder_id,
            input_context,
            request.output_handle_index.unwrap_or(0),
            handle,
            request.payload.get(8).copied().unwrap_or(0),
            requested_folder_id != folder_id,
            session.post_hierarchy_actions.recent_probe_actions.join(">")
        );
        if session.post_hierarchy_actions.inbox_open_folder_probe_count >= 1
            && !session
                .post_hierarchy_actions
                .inbox_normal_contents_table_observed
        {
            session.record_first_inbox_loop_transition_context(first_loop_transition.clone());
        }
        session.record_inbox_open_folder_probe();
        session.record_last_inbox_open_folder_context(format!(
            "input_index={};input_handle={};input_kind={};input_folder={};output_index={};output_handle={};open_mode=0x{:02x};display_name={};folder_type={};container_class={};content_count={};unread_count={};subfolders={};record_key={};source_key={};parent_source_key={};open_folder_response_bytes={};open_folder_response_preview={}",
            request.input_handle_index().unwrap_or(0),
            format_optional_debug_handle(input_handle_value),
            input_object_kind,
            input_folder_id,
            request.output_handle_index.unwrap_or(0),
            handle,
            request.payload.get(8).copied().unwrap_or(0),
            inbox_contract_display_name,
            inbox_contract_folder_type,
            inbox_contract_container_class,
            inbox_contract_content_count,
            inbox_contract_unread_count,
            inbox_contract_subfolders,
            inbox_contract_record_key,
            inbox_contract_source_key,
            inbox_contract_parent_source_key,
            open_folder_response.len(),
            hex_preview(&open_folder_response, 32)
        ));
        session.record_recent_probe_action(format!(
            "OpenFolder(in={},handle={},out={},folder=0x{folder_id:016x})",
            request.input_handle_index().unwrap_or(0),
            format_optional_debug_handle(input_handle_value),
            handle
        ));
    }
    let post_fai_reopen_stall = folder_id == INBOX_FOLDER_ID
        && inbox_post_fai_reopen_stall_observed(&session.post_hierarchy_actions)
        && !session.post_hierarchy_actions.post_inbox_fai_reopen_logged;
    responses.extend_from_slice(&open_folder_response);
    session.record_post_hierarchy_request_contract(post_hierarchy_open_folder_contract(
        folder_id, "ok",
    ));
    if folder_id == INBOX_FOLDER_ID {
        if post_fai_reopen_stall {
            tracing::warn!(
                rca_debug = true,
                adapter = "mapi",
                endpoint = "emsmdb",
                mailbox = %principal.email,
                request_type = "Execute",
                request_rop_id = "0x02",
                mapi_request_id = request_id,
                folder_id = format!("0x{folder_id:016x}"),
                output_handle_id = handle,
                open_mode_flags = format!("0x{:02x}", request.payload.get(8).copied().unwrap_or(0)),
                open_folder_response_bytes = open_folder_response.len(),
                open_folder_response_preview = %hex_preview(&open_folder_response, 32),
                last_open = %debug_context_or_none(
                    &session.post_hierarchy_actions.last_inbox_open_folder_context
                ),
                last_associated_query = %debug_context_or_none(
                    &session.post_hierarchy_actions.last_inbox_associated_query_context
                ),
                last_associated_find = %debug_context_or_none(
                    &session.post_hierarchy_actions.last_inbox_associated_find_context
                ),
                last_inbox_related_release = %debug_context_or_none(
                    &session.post_hierarchy_actions.last_inbox_related_release_context
                ),
                last_folder_type_getprops = %debug_context_or_none(
                    &session
                        .post_hierarchy_actions
                        .last_inbox_folder_type_getprops_context
                ),
                recent_actions = %session.post_hierarchy_actions.recent_probe_actions.join(">"),
                expected_next_client_step =
                    "Open Inbox normal contents table or SynchronizationConfigure",
                "rca warn mapi inbox reopened after associated FAI without normal contents"
            );
            session.mark_post_inbox_fai_reopen_logged();
        }
        if session
            .post_hierarchy_actions
            .inbox_rule_organizer_stream_read_observed
            && !session
                .post_hierarchy_actions
                .post_rule_organizer_stream_reopen_logged
        {
            tracing::info!(
                rca_debug = true,
                adapter = "mapi",
                endpoint = "emsmdb",
                mailbox = %principal.email,
                request_type = "Execute",
                request_rop_id = "0x02",
                mapi_request_id = request_id,
                folder_id = format!("0x{folder_id:016x}"),
                output_handle_id = handle,
                open_mode_flags = format!("0x{:02x}", request.payload.get(8).copied().unwrap_or(0)),
                open_folder_response_bytes = open_folder_response.len(),
                open_folder_response_preview = %hex_preview(&open_folder_response, 32),
                last_rule_organizer_stream = %debug_context_or_none(
                    &session
                        .post_hierarchy_actions
                        .last_inbox_rule_organizer_stream_context
                ),
                last_open = %debug_context_or_none(
                    &session.post_hierarchy_actions.last_inbox_open_folder_context
                ),
                last_contents_table = %debug_context_or_none(
                    &session.post_hierarchy_actions.last_inbox_contents_table_context
                ),
                last_associated_query = %debug_context_or_none(
                    &session.post_hierarchy_actions.last_inbox_associated_query_context
                ),
                last_associated_find = %debug_context_or_none(
                    &session.post_hierarchy_actions.last_inbox_associated_find_context
                ),
                last_inbox_related_release = %debug_context_or_none(
                    &session.post_hierarchy_actions.last_inbox_related_release_context
                ),
                last_folder_type_getprops = %debug_context_or_none(
                    &session
                        .post_hierarchy_actions
                        .last_inbox_folder_type_getprops_context
                ),
                recent_actions = %session.post_hierarchy_actions.recent_probe_actions.join(">"),
                "rca debug mapi inbox reopened after RuleOrganizer stream read"
            );
            session.mark_post_rule_organizer_stream_reopen_logged();
        }
        tracing::info!(
            rca_debug = true,
            adapter = "mapi",
            endpoint = "emsmdb",
            mailbox = %principal.email,
            request_type = "Execute",
            request_rop_id = "0x02",
            folder_id = format!("0x{folder_id:016x}"),
            output_handle_id = handle,
            display_name = %inbox_contract_display_name,
            folder_type = %inbox_contract_folder_type,
            container_class = %inbox_contract_container_class,
            record_key = %inbox_contract_record_key,
            source_key = %inbox_contract_source_key,
            parent_source_key = %inbox_contract_parent_source_key,
            content_count = %inbox_contract_content_count,
            unread_count = %inbox_contract_unread_count,
            subfolders = %inbox_contract_subfolders,
            "rca debug mapi opened inbox folder handle contract"
        );
        if let Some(summary) = format_inbox_open_loop_summary(&session.post_hierarchy_actions) {
            if !session
                .post_hierarchy_actions
                .post_common_views_inbox_open_loop_metric_logged
                && !session
                    .post_hierarchy_actions
                    .last_common_views_inbox_shortcut_context
                    .is_empty()
            {
                record_mapi_outlook_view_repeated_inbox_open_after_common_views();
                session.record_outlook_view_failure_trace_event(format!(
                    "repeated_inbox_open_after_common_views:{summary}"
                ));
                tracing::info!(
                    rca_debug = true,
                    adapter = "mapi",
                    endpoint = "emsmdb",
                    mailbox = %principal.email,
                    request_type = "Execute",
                    request_rop_id = "0x02",
                    folder_id = format!("0x{INBOX_FOLDER_ID:016x}"),
                    loop_summary = %summary,
                    last_inbox_notification_registration =
                        %debug_context_or_none(
                            &session
                                .post_hierarchy_actions
                                .last_inbox_notification_registration_context
                        ),
                    "rca debug mapi repeated inbox open after common views"
                );
                session.mark_post_common_views_inbox_open_loop_metric_logged();
            }
            if !session.post_hierarchy_actions.inbox_loop_transition_logged {
                tracing::info!(
                    rca_debug = true,
                    adapter = "mapi",
                    endpoint = "emsmdb",
                    mailbox = %principal.email,
                    request_type = "Execute",
                    request_rop_id = "0x02",
                    folder_id = format!("0x{INBOX_FOLDER_ID:016x}"),
                    transition_context =
                        %debug_context_or_none(
                            &session
                                .post_hierarchy_actions
                                .first_inbox_loop_transition_context
                        ),
                    loop_summary = %summary,
                    "rca debug mapi inbox open loop transition"
                );
                session.mark_inbox_loop_transition_logged();
            }
            tracing::info!(
                rca_debug = true,
                adapter = "mapi",
                endpoint = "emsmdb",
                mailbox = %principal.email,
                request_type = "Execute",
                folder_id = format!("0x{INBOX_FOLDER_ID:016x}"),
                loop_summary = %summary,
                "rca debug mapi repeated inbox open folder loop summary"
            );
        }
    }
    if matches!(folder_id, ROOT_FOLDER_ID | IPM_SUBTREE_FOLDER_ID) {
        tracing::info!(
            rca_debug = true,
            adapter = "mapi",
            endpoint = "emsmdb",
            mailbox = %principal.email,
            request_type = "Execute",
            request_rop_id = "0x02",
            folder_id = format!("0x{folder_id:016x}"),
            folder_name = post_hierarchy_probe_folder_name(folder_id),
            output_handle_id = handle,
            display_name = %root_ipm_contract_display_name,
            folder_type = %root_ipm_contract_folder_type,
            container_class = %root_ipm_contract_container_class,
            record_key = %root_ipm_contract_record_key,
            source_key = %root_ipm_contract_source_key,
            parent_source_key = %root_ipm_contract_parent_source_key,
            default_folder_identification_contract =
                %default_folder_identification_contract_for_debug(principal),
            "rca debug mapi root ipm subtree folder handle contract"
        );
        log_outlook_bootstrap_phase(
            principal,
            "root_ipm_subtree_opened",
            "0x02",
            Some(folder_id),
            false,
            None,
            None,
            Some(handle),
            "",
        );
    } else if folder_id == INBOX_FOLDER_ID {
        log_outlook_bootstrap_phase(
            principal,
            "inbox_opened",
            "0x02",
            Some(folder_id),
            false,
            None,
            None,
            Some(handle),
            "",
        );
    }
    output_handles.push(handle);
}
