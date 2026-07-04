use super::*;
use anyhow::Result;

#[derive(Debug, Default)]
pub(in crate::mapi) struct ConnectBodyDebugSummary {
    pub(in crate::mapi) status_code: u32,
    pub(in crate::mapi) error_code: u32,
    pub(in crate::mapi) polls_max: u32,
    pub(in crate::mapi) retry_count: u32,
    pub(in crate::mapi) retry_delay_ms: u32,
    pub(in crate::mapi) dn_prefix: String,
    pub(in crate::mapi) display_name: String,
    pub(in crate::mapi) auxiliary_buffer_bytes: u32,
    pub(in crate::mapi) parse_error: String,
}

pub(in crate::mapi) fn log_connect_body_debug(
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

    tracing::debug!(
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

pub(in crate::mapi) fn summarize_connect_body(body: &[u8]) -> ConnectBodyDebugSummary {
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

pub(in crate::mapi) fn recent_execute_debug_summaries(
    session: &MapiSession,
    limit: usize,
) -> String {
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

pub(in crate::mapi) fn special_folder_contract_summary(session: &MapiSession) -> String {
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
        (
            "conversation_history",
            CONVERSATION_HISTORY_FOLDER_ID,
            "additional_ren",
        ),
        ("freebusy_data", FREEBUSY_DATA_FOLDER_ID, "freebusy"),
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

pub(in crate::mapi) fn required_default_folder_disconnect_coverage_summary(
    session: &MapiSession,
) -> String {
    const REQUIRED_DEFAULT_FOLDERS: &[(&str, u64, &str, u64)] = &[
        ("inbox", INBOX_FOLDER_ID, "logon", IPM_SUBTREE_FOLDER_ID),
        ("outbox", OUTBOX_FOLDER_ID, "logon", IPM_SUBTREE_FOLDER_ID),
        ("sent", SENT_FOLDER_ID, "logon", IPM_SUBTREE_FOLDER_ID),
        ("trash", TRASH_FOLDER_ID, "logon", IPM_SUBTREE_FOLDER_ID),
        (
            "drafts",
            DRAFTS_FOLDER_ID,
            "default_ipm",
            IPM_SUBTREE_FOLDER_ID,
        ),
        (
            "contacts",
            CONTACTS_FOLDER_ID,
            "default_ipm",
            IPM_SUBTREE_FOLDER_ID,
        ),
        (
            "calendar",
            CALENDAR_FOLDER_ID,
            "default_ipm",
            IPM_SUBTREE_FOLDER_ID,
        ),
        (
            "journal",
            JOURNAL_FOLDER_ID,
            "default_ipm",
            IPM_SUBTREE_FOLDER_ID,
        ),
        (
            "notes",
            NOTES_FOLDER_ID,
            "default_ipm",
            IPM_SUBTREE_FOLDER_ID,
        ),
        (
            "tasks",
            TASKS_FOLDER_ID,
            "default_ipm",
            IPM_SUBTREE_FOLDER_ID,
        ),
    ];
    let request_contracts = session
        .post_hierarchy_actions
        .request_contract_sequence
        .join("|");
    let last_hierarchy_root = session
        .post_hierarchy_actions
        .last_completed_hierarchy_sync_root;

    REQUIRED_DEFAULT_FOLDERS
        .iter()
        .map(|(role, folder_id, advertised_source, parent_folder_id)| {
            let opened = session
                .post_hierarchy_actions
                .opened_folder_ids
                .contains(folder_id);
            let content_checkpointed = session
                .post_hierarchy_actions
                .completed_sync_checkpoint_folder_ids
                .contains(folder_id);
            let hierarchy_row_expected_present = last_hierarchy_root.is_some_and(|root_id| {
                root_id == ROOT_FOLDER_ID
                    || root_id == *parent_folder_id
                    || root_id == *folder_id
            });
            let folder_hex = format!("0x{folder_id:016x}");
            let pre_content_contract_seen =
                request_contracts.contains(role) || request_contracts.contains(&folder_hex);
            let live_handle_count = session
                .handles
                .values()
                .filter(|object| object.folder_id() == Some(*folder_id))
                .count();
            format!(
                "{role}:folder={folder_hex};advertised_source={advertised_source};parent=0x{parent_folder_id:016x};hierarchy_row_expected_present={hierarchy_row_expected_present};opened={opened};pre_content_contract_seen={pre_content_contract_seen};content_checkpointed={content_checkpointed};live_handle_count={live_handle_count}"
            )
        })
        .collect::<Vec<_>>()
        .join("|")
}

pub(in crate::mapi) fn mapi_object_debug_kind(object: &MapiObject) -> &'static str {
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
        MapiObject::CommonViewNamedView { .. } => "common_view_named_view",
        MapiObject::SearchFolderDefinitionMessage { .. } => "search_folder_definition_message",
        MapiObject::AssociatedConfig { .. } => "associated_config",
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
    pub(in crate::mapi) outlook_bootstrap_phase: u64,
    pub(in crate::mapi) outlook_bootstrap_phase_name: &'static str,
    pub(in crate::mapi) outlook_bootstrap_stall_code: u64,
    pub(in crate::mapi) outlook_bootstrap_stall_name: &'static str,
    pub(in crate::mapi) outlook_bootstrap_next_expected_phase: &'static str,
    pub(in crate::mapi) execute_count: usize,
    pub(in crate::mapi) rop_ids_seen: String,
    pub(in crate::mapi) content_sync_configure_observed: bool,
    pub(in crate::mapi) release_client_initiated: bool,
    pub(in crate::mapi) logoff_client_initiated: bool,
    pub(in crate::mapi) disconnect_client_initiated: bool,
    pub(in crate::mapi) close_kind: &'static str,
    pub(in crate::mapi) last_completed_hierarchy_sync_root: String,
    pub(in crate::mapi) last_successful_hierarchy_get_buffer_summary: String,
    pub(in crate::mapi) last_default_folder_hierarchy_membership_summary: String,
    pub(in crate::mapi) last_getprops_request_contract: String,
    pub(in crate::mapi) last_setprops_request_contract: String,
    pub(in crate::mapi) request_contract_sequence: String,
    pub(in crate::mapi) outlook_view_trace_events: String,
}

pub(in crate::mapi) fn post_hierarchy_action_summary(
    session: &MapiSession,
    disconnect_client_initiated: bool,
) -> PostHierarchyActionDebugSummary {
    let actions = &session.post_hierarchy_actions;
    let bootstrap_phase = outlook_bootstrap_phase(actions);
    let bootstrap_stall_code = outlook_bootstrap_stall_code(actions);
    record_mapi_outlook_view_bootstrap_progress(
        bootstrap_phase,
        bootstrap_stall_code,
        actions.inbox_open_folder_probe_count,
        actions.inbox_folder_type_getprops_probe_count,
    );
    PostHierarchyActionDebugSummary {
        outlook_bootstrap_phase: bootstrap_phase,
        outlook_bootstrap_phase_name: outlook_bootstrap_phase_name(bootstrap_phase),
        outlook_bootstrap_stall_code: bootstrap_stall_code,
        outlook_bootstrap_stall_name: outlook_bootstrap_stall_name(bootstrap_stall_code),
        outlook_bootstrap_next_expected_phase: outlook_bootstrap_next_expected_phase(actions),
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
        last_default_folder_hierarchy_membership_summary: actions
            .last_default_folder_hierarchy_membership_summary
            .clone(),
        last_getprops_request_contract: actions.last_getprops_request_contract.clone(),
        last_setprops_request_contract: actions.last_setprops_request_contract.clone(),
        request_contract_sequence: actions
            .request_contract_sequence
            .iter()
            .enumerate()
            .map(|(index, contract)| format!("{}:{contract}", index + 1))
            .collect::<Vec<_>>()
            .join("|"),
        outlook_view_trace_events: actions.outlook_view_failure_trace_events.join(">"),
    }
}

pub(in crate::mapi) fn outlook_bootstrap_phase(actions: &PostHierarchyActionState) -> u64 {
    if actions.content_sync_configure_observed {
        11
    } else if actions.inbox_normal_contents_table_query_rows_observed {
        10
    } else if actions.inbox_normal_contents_table_setcolumns_observed {
        9
    } else if actions.inbox_normal_contents_table_observed {
        8
    } else if actions.post_inbox_fai_folder_type_probe_loop_logged {
        7
    } else if actions.inbox_open_folder_probe_count >= 2
        && !actions.last_inbox_hierarchy_query_context.is_empty()
    {
        6
    } else if !actions.last_inbox_hierarchy_query_context.is_empty()
        || actions
            .last_completed_hierarchy_sync_root
            .is_some_and(|folder_id| folder_id == IPM_SUBTREE_FOLDER_ID)
    {
        5
    } else if actions.inbox_associated_contents_table_observed
        && !actions.last_inbox_related_release_context.is_empty()
    {
        4
    } else if actions.inbox_associated_contents_table_observed {
        3
    } else if actions.bootstrap_probe_observed {
        2
    } else if actions.execute_count > 0 {
        1
    } else {
        0
    }
}

pub(in crate::mapi) fn outlook_bootstrap_phase_name(phase: u64) -> &'static str {
    match phase {
        11 => "content_sync_configure_observed",
        10 => "inbox_normal_contents_query_rows_observed",
        9 => "inbox_normal_contents_setcolumns_observed",
        8 => "inbox_normal_contents_table_opened",
        7 => "repeated_inbox_folder_type_probe_loop",
        6 => "inbox_reopened_after_ipm_hierarchy",
        5 => "ipm_hierarchy_rows_completed",
        4 => "inbox_fai_released_without_normal_contents",
        3 => "inbox_fai_query_rows_completed",
        2 => "bootstrap_probe_observed",
        1 => "execute_started",
        _ => "none",
    }
}

pub(in crate::mapi) fn outlook_bootstrap_stall_code(actions: &PostHierarchyActionState) -> u64 {
    if !actions
        .last_inbox_notification_registration_context
        .is_empty()
        && !actions.last_common_views_inbox_shortcut_context.is_empty()
        && !actions.inbox_associated_contents_table_observed
        && !actions.inbox_normal_contents_table_observed
    {
        4
    } else if actions.inbox_associated_contents_table_observed
        && actions.inbox_associated_exact_ipm_configuration_findrow_matched
        && !actions.inbox_associated_findrow_returned_content
        && !actions.inbox_associated_query_rows_returned_non_empty
        && !actions.inbox_associated_config_open_observed
        && !actions.inbox_normal_contents_table_observed
    {
        5
    } else if actions.post_inbox_fai_folder_type_probe_loop_logged {
        3
    } else if !actions.last_inbox_hierarchy_query_context.is_empty()
        && actions.inbox_open_folder_probe_count >= 2
        && !actions.inbox_normal_contents_table_observed
    {
        2
    } else if actions.inbox_associated_contents_table_observed
        && !actions.inbox_normal_contents_table_observed
        && !actions.last_inbox_related_release_context.is_empty()
    {
        1
    } else {
        0
    }
}

pub(in crate::mapi) fn outlook_bootstrap_stall_name(stall_code: u64) -> &'static str {
    match stall_code {
        5 => "after_inbox_fai_exact_config_findrow_without_open",
        4 => "after_common_views_inbox_notification_without_contents",
        3 => "repeated_inbox_folder_type_probe_without_contents",
        2 => "after_ipm_hierarchy_without_inbox_contents",
        1 => "after_inbox_fai_without_inbox_contents",
        _ => "none",
    }
}

pub(in crate::mapi) fn outlook_bootstrap_next_expected_phase(
    actions: &PostHierarchyActionState,
) -> &'static str {
    if actions.content_sync_configure_observed {
        "outlook_running_content_sync"
    } else if actions.inbox_normal_contents_table_query_rows_observed {
        "content_sync_configure_or_message_open"
    } else if actions.inbox_normal_contents_table_setcolumns_observed {
        "inbox_normal_contents_query_rows"
    } else if actions.inbox_normal_contents_table_observed {
        "inbox_normal_contents_setcolumns"
    } else {
        "open_inbox_normal_contents_table_or_sync_configure"
    }
}

pub(in crate::mapi) fn post_hierarchy_close_kind(
    actions: &PostHierarchyActionState,
    disconnect_client_initiated: bool,
) -> &'static str {
    if actions.content_sync_configure_observed {
        "post_hierarchy_content_sync_observed"
    } else if !actions
        .last_inbox_notification_registration_context
        .is_empty()
        && !actions.last_common_views_inbox_shortcut_context.is_empty()
        && !actions.inbox_associated_contents_table_observed
        && !actions.inbox_normal_contents_table_observed
    {
        "outlook_post_common_views_notification_before_content_sync"
    } else if actions.release_client_initiated && actions.logoff_client_initiated {
        "outlook_release_logoff_before_content_sync"
    } else if actions.release_client_initiated {
        "outlook_release_before_content_sync"
    } else if actions.post_calendar_query_position_named_property_probe_count > 0
        && !actions.calendar_normal_contents_table_query_rows_observed
    {
        "outlook_calendar_query_position_named_property_burst_before_query_rows"
    } else if !actions
        .last_calendar_normal_contents_table_query_position_context
        .is_empty()
        && !actions.calendar_normal_contents_table_query_rows_observed
    {
        "outlook_calendar_query_position_before_query_rows"
    } else if !actions
        .last_inbox_normal_contents_table_query_position_context
        .is_empty()
        && !actions.inbox_normal_contents_table_query_rows_observed
    {
        "outlook_visible_inbox_query_position_before_query_rows"
    } else if actions.execute_count > 0 {
        "outlook_post_hierarchy_execute_before_content_sync"
    } else if disconnect_client_initiated && actions.last_completed_hierarchy_sync_root.is_some() {
        "outlook_disconnect_immediately_after_hierarchy"
    } else {
        "post_hierarchy_no_close"
    }
}

pub(in crate::mapi) fn partial_scope_checkpoint_not_stored_count(
    actions: &PostHierarchyActionState,
) -> usize {
    actions
        .completed_sync_checkpoint_summaries
        .iter()
        .filter(|summary| summary.contains("status=ok_partial_scope_no_checkpoint"))
        .count()
}

pub(in crate::mapi) fn post_fai_inbox_probe_loop_terminal_summary(
    actions: &PostHierarchyActionState,
) -> Option<String> {
    if !actions.post_inbox_fai_folder_type_probe_loop_logged
        || !actions.inbox_associated_contents_table_observed
        || actions.inbox_normal_contents_table_observed
        || actions.inbox_open_folder_probe_count < 2
        || actions.inbox_folder_type_getprops_probe_count < 2
    {
        return None;
    }

    Some(format!(
        "folder=0x{INBOX_FOLDER_ID:016x};open_folder_count={};folder_type_getprops_count={};associated_contents_table_observed={};normal_contents_table_observed={};normal_setcolumns_observed={};normal_query_rows_observed={};last_open={};last_folder_type_getprops={};last_associated_query={};last_associated_find={};last_inbox_related_release={};recent_actions={};next_expected_client_step=open_inbox_normal_contents_table_or_sync_configure",
        actions.inbox_open_folder_probe_count,
        actions.inbox_folder_type_getprops_probe_count,
        actions.inbox_associated_contents_table_observed,
        actions.inbox_normal_contents_table_observed,
        actions.inbox_normal_contents_table_setcolumns_observed,
        actions.inbox_normal_contents_table_query_rows_observed,
        debug_context_or_none(&actions.last_inbox_open_folder_context),
        debug_context_or_none(&actions.last_inbox_folder_type_getprops_context),
        debug_context_or_none(&actions.last_inbox_associated_query_context),
        debug_context_or_none(&actions.last_inbox_associated_find_context),
        debug_context_or_none(&actions.last_inbox_related_release_context),
        actions.recent_probe_actions.join(">")
    ))
}

pub(in crate::mapi) fn debug_context_or_none(context: &str) -> &str {
    if context.is_empty() {
        "none"
    } else {
        context
    }
}

pub(in crate::mapi) fn format_rop_ids_for_debug(rop_ids: &[u8]) -> String {
    rop_ids
        .iter()
        .map(|rop_id| format!("0x{rop_id:02x}"))
        .collect::<Vec<_>>()
        .join(",")
}

pub(in crate::mapi) fn log_mapi_session_disconnect(
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
                checkpoint_zero_delta,
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
                "handle={handle};folder=0x{folder_id:016x};sync=0x{sync_type:02x};kind={};mailbox={};seq={checkpoint_change_sequence};modseq={checkpoint_modseq};zero_delta={checkpoint_zero_delta};state={};client_state={};marker_mask=0x{:02x};upload_buffer={};transfer={}/{};transfer_completed={};checkpoint_recorded={};incremental={}",
                checkpoint_kind.as_str(),
                mailbox_id.map(|id| id.to_string()).unwrap_or_default(),
                state.len(),
                client_state_uploaded_bytes,
                client_state_uploaded_marker_mask,
                state_upload_buffer.len(),
                transfer_position,
                transfer_buffer.len(),
                *transfer_position >= transfer_buffer.len(),
                session
                    .post_hierarchy_actions
                    .completed_sync_checkpoint_folder_ids
                    .contains(folder_id),
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
    let outlook_view_failure_trace_summary = session
        .post_hierarchy_actions
        .outlook_view_failure_trace_events
        .join("|");
    let outlook_stream_batch_status =
        if session.post_hierarchy_actions.outlook_stream_batch_observed {
            "observed"
        } else {
            "not_observed"
        };
    let outlook_stream_batch_summaries = session
        .post_hierarchy_actions
        .outlook_stream_batch_summaries
        .join("|");
    let special_folder_contract_summary = special_folder_contract_summary(session);
    let required_default_folder_coverage =
        required_default_folder_disconnect_coverage_summary(session);
    let all_sync_sources_completed = sync_source_count == completed_sync_source_count;
    let partial_scope_checkpoint_not_stored_count =
        partial_scope_checkpoint_not_stored_count(&session.post_hierarchy_actions);
    let partial_scope_checkpoint_not_stored_expected =
        partial_scope_checkpoint_not_stored_count > 0 && all_sync_sources_completed;
    let post_fai_inbox_probe_loop_terminal_summary =
        post_fai_inbox_probe_loop_terminal_summary(&session.post_hierarchy_actions);
    let post_fai_inbox_probe_loop_terminal = post_fai_inbox_probe_loop_terminal_summary.is_some();
    let post_fai_inbox_probe_loop_terminal_summary =
        post_fai_inbox_probe_loop_terminal_summary.unwrap_or_default();
    let clean_client_close_after_sync = endpoint == MapiEndpoint::Emsmdb
        && request_type == "Disconnect"
        && post_hierarchy_summary.content_sync_configure_observed
        && all_sync_sources_completed;
    let final_phase_abandoned_after_inbox_fai_query_rows =
        session.abandoned_after_inbox_fai_query_rows();
    let outlook_startup_gates = outlook_startup_gate_summary(session);
    let final_phase_next_debug_focus = if final_phase_abandoned_after_inbox_fai_query_rows {
        "client_abandoned_after_inbox_fai_query_rows"
    } else if session
        .post_hierarchy_actions
        .post_calendar_query_position_named_property_probe_count
        > 0
        && !session
            .post_hierarchy_actions
            .calendar_normal_contents_table_query_rows_observed
    {
        "calendar_query_rows_missing_after_named_property_probe"
    } else if !session
        .post_hierarchy_actions
        .last_calendar_normal_contents_table_query_position_context
        .is_empty()
        && !session
            .post_hierarchy_actions
            .calendar_normal_contents_table_query_rows_observed
    {
        "calendar_query_rows_missing_after_query_position"
    } else if !session
        .post_hierarchy_actions
        .last_inbox_normal_contents_table_query_position_context
        .is_empty()
        && !session
            .post_hierarchy_actions
            .inbox_normal_contents_table_query_rows_observed
    {
        "visible_inbox_query_rows_missing_after_query_position"
    } else if post_hierarchy_summary.outlook_bootstrap_stall_code == 5 {
        "post_inbox_fai_exact_config_findrow_without_open"
    } else if post_hierarchy_summary.outlook_bootstrap_stall_code == 4 {
        "post_common_views_inbox_notification_without_contents"
    } else if post_fai_inbox_probe_loop_terminal {
        "post_fai_inbox_folder_type_probe_loop"
    } else if clean_client_close_after_sync {
        "outlook_reconnect_or_client_side_reason"
    } else {
        "post_hierarchy_sequence"
    };
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

    if endpoint == MapiEndpoint::Emsmdb && request_type == "Disconnect" {
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
            outlook_startup_last_successful_gate =
                outlook_startup_gates.last_successful_gate,
            outlook_startup_first_missing_gate =
                outlook_startup_gates.first_missing_gate,
            outlook_startup_gate_count = outlook_startup_gates.gate_count,
            outlook_startup_passed_gate_count = outlook_startup_gates.passed_count,
            outlook_startup_gates = %outlook_startup_gates.gates,
            outlook_abandoned_immediately_after_fai =
                outlook_startup_gates.abandoned_immediately_after_fai,
            outlook_smart_input_variant = %session.outlook_smart_input_variant,
            outlook_smart_input_variant_scope = "session",
            outlook_smart_input_variant_selected =
                session.outlook_smart_input_variant != "none"
                    && !session.outlook_smart_input_variant.starts_with("unknown:"),
            outlook_smart_input_variant_applied =
                session.outlook_smart_input_variant_applied,
            inbox_associated_broad_ipm_configuration_findrow_matched =
                session
                    .post_hierarchy_actions
                    .inbox_associated_broad_ipm_configuration_findrow_matched,
            inbox_associated_exact_ipm_configuration_findrow_matched =
                session
                    .post_hierarchy_actions
                    .inbox_associated_exact_ipm_configuration_findrow_matched,
            outlook_smart_input_variant_result =
                if session.outlook_smart_input_variant_applied {
                    "applied"
                } else {
                    "not_applied"
                },
            next_debug_focus = final_phase_next_debug_focus,
            "rca debug mapi outlook startup gate summary"
        );
    }

    if endpoint == MapiEndpoint::Emsmdb && request_type == "Disconnect" {
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
            handle_count = session.handles.len(),
            live_handle_summaries = %live_handle_summaries,
            last_successful_execute_context =
                %debug_context_or_none(
                    &session
                        .post_hierarchy_actions
                        .last_successful_execute_context
                ),
            last_successful_non_release_execute_context =
                %debug_context_or_none(
                    &session
                        .post_hierarchy_actions
                        .last_successful_non_release_execute_context
                ),
            last_table_context =
                %debug_context_or_none(&session.post_hierarchy_actions.last_table_context),
            last_table_query_rows_context =
                %debug_context_or_none(
                    &session
                        .post_hierarchy_actions
                        .last_table_query_rows_context
                ),
            last_table_release_context =
                %debug_context_or_none(
                    &session
                        .post_hierarchy_actions
                        .last_table_release_context
                ),
            last_inbox_associated_query_context =
                %debug_context_or_none(
                    &session
                        .post_hierarchy_actions
                        .last_inbox_associated_query_context
                ),
            last_inbox_associated_non_empty_query_context =
                %debug_context_or_none(
                    &session
                        .post_hierarchy_actions
                        .last_inbox_associated_non_empty_query_context
                ),
            last_inbox_associated_end_query_context =
                %debug_context_or_none(
                    &session
                        .post_hierarchy_actions
                        .last_inbox_associated_end_query_context
                ),
            last_inbox_related_release_context =
                %debug_context_or_none(
                    &session
                        .post_hierarchy_actions
                        .last_inbox_related_release_context
                ),
            inbox_associated_contents_table_observed =
                session
                    .post_hierarchy_actions
                    .inbox_associated_contents_table_observed,
            inbox_associated_query_rows_returned_non_empty =
                session
                    .post_hierarchy_actions
                    .inbox_associated_query_rows_returned_non_empty,
            inbox_associated_query_rows_reached_end =
                session
                    .post_hierarchy_actions
                    .inbox_associated_query_rows_reached_end,
            inbox_associated_config_open_observed =
                session
                    .post_hierarchy_actions
                    .inbox_associated_config_open_observed,
            inbox_associated_config_stream_open_observed =
                session
                    .post_hierarchy_actions
                    .inbox_associated_config_stream_open_observed,
            inbox_associated_config_stream_read_observed =
                session
                    .post_hierarchy_actions
                    .inbox_associated_config_stream_read_observed,
            inbox_normal_contents_table_observed =
                session
                    .post_hierarchy_actions
                    .inbox_normal_contents_table_observed,
            inbox_normal_query_rows_observed =
                session
                    .post_hierarchy_actions
                    .inbox_normal_contents_table_query_rows_observed,
            last_inbox_normal_query_position_context =
                %debug_context_or_none(
                    &session
                        .post_hierarchy_actions
                        .last_inbox_normal_contents_table_query_position_context
                ),
            last_inbox_normal_query_position_handle =
                %session
                    .post_hierarchy_actions
                    .last_inbox_normal_contents_table_query_position_handle
                    .map(|handle| handle.to_string())
                    .unwrap_or_else(|| "none".to_string()),
            calendar_normal_query_rows_observed =
                session
                    .post_hierarchy_actions
                    .calendar_normal_contents_table_query_rows_observed,
            last_calendar_normal_query_position_context =
                %debug_context_or_none(
                    &session
                        .post_hierarchy_actions
                        .last_calendar_normal_contents_table_query_position_context
                ),
            last_calendar_normal_query_position_handle =
                %session
                    .post_hierarchy_actions
                    .last_calendar_normal_contents_table_query_position_handle
                    .map(|handle| handle.to_string())
                    .unwrap_or_else(|| "none".to_string()),
            post_calendar_query_position_named_property_probe_count =
                session
                    .post_hierarchy_actions
                    .post_calendar_query_position_named_property_probe_count,
            last_post_calendar_query_position_named_property_context =
                %debug_context_or_none(
                    &session
                        .post_hierarchy_actions
                        .last_post_calendar_query_position_named_property_context
                ),
            abandoned_after_inbox_fai_query_rows =
                final_phase_abandoned_after_inbox_fai_query_rows,
            post_fai_inbox_probe_loop_terminal,
            post_hierarchy_content_sync_configure_observed =
                post_hierarchy_summary.content_sync_configure_observed,
            post_hierarchy_close_kind = %post_hierarchy_summary.close_kind,
            recent_probe_actions =
                %session.post_hierarchy_actions.recent_probe_actions.join(">"),
            recent_execute_summaries = %recent_execute_summaries,
            next_debug_focus = final_phase_next_debug_focus,
            "rca debug mapi final phase disconnect summary"
        );
    }

    if endpoint == MapiEndpoint::Emsmdb
        && request_type == "Disconnect"
        && (!session.handles.is_empty() || post_hierarchy_summary.content_sync_configure_observed)
    {
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
            handle_count = session.handles.len(),
            live_handle_summaries = %live_handle_summaries,
            sync_source_count,
            sync_collector_count,
            notification_subscription_count,
            completed_sync_source_count,
            incomplete_sync_source_count,
            all_live_sync_sources_completed = all_sync_sources_completed,
            sync_source_summaries = %sync_source_summaries,
            completed_sync_checkpoint_summaries = %completed_sync_checkpoint_summaries,
            required_default_folder_coverage = %required_default_folder_coverage,
            partial_scope_checkpoint_not_stored_count,
            partial_scope_checkpoint_not_stored_expected,
            total_transfer_buffer_bytes,
            total_transfer_position_bytes,
            clean_client_close_after_sync,
            post_hierarchy_content_sync_configure_observed =
                post_hierarchy_summary.content_sync_configure_observed,
            post_hierarchy_last_completed_sync_root =
                %post_hierarchy_summary.last_completed_hierarchy_sync_root,
            post_hierarchy_close_kind = %post_hierarchy_summary.close_kind,
            outlook_profile_stage = %outlook_profile_stage,
            next_expected_client_step = %next_expected_client_step,
            recent_execute_summaries = %recent_execute_summaries,
            outlook_view_failure_trace_event_count =
                session.post_hierarchy_actions.outlook_view_failure_trace_events.len(),
            outlook_view_failure_trace_summary = %outlook_view_failure_trace_summary,
            outlook_stream_batch_status = outlook_stream_batch_status,
            outlook_stream_batch_summaries = %outlook_stream_batch_summaries,
            next_debug_focus =
                if clean_client_close_after_sync && !session.handles.is_empty() {
                    "client_closed_cleanly_with_completed_live_handles"
                } else if incomplete_sync_source_count > 0 {
                    "disconnect_with_incomplete_live_sync_source"
                } else if session.handles.is_empty() {
                    "client_closed_after_releasing_all_handles"
                } else {
                    "disconnect_remaining_handle_review"
                },
            "rca debug mapi disconnect remaining handles"
        );
    }

    if endpoint == MapiEndpoint::Emsmdb
        && request_type == "Disconnect"
        && post_hierarchy_summary.content_sync_configure_observed
    {
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
            required_default_folder_coverage = %required_default_folder_coverage,
            completed_sync_checkpoint_summaries = %completed_sync_checkpoint_summaries,
            special_folder_contract_summary = %special_folder_contract_summary,
            recent_execute_summaries = %recent_execute_summaries,
            clean_client_close_after_sync,
            all_sync_sources_completed,
            outlook_profile_stage = %outlook_profile_stage,
            next_expected_client_step = %next_expected_client_step,
            next_debug_focus = "outlook_default_folder_content_coverage",
            "rca debug mapi default folder disconnect coverage"
        );
    }

    tracing::debug!(
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
        required_default_folder_coverage = %required_default_folder_coverage,
        completed_sync_checkpoint_summaries = %completed_sync_checkpoint_summaries,
        partial_scope_checkpoint_not_stored_count,
        partial_scope_checkpoint_not_stored_expected,
        post_fai_inbox_probe_loop_terminal,
        post_fai_inbox_probe_loop_terminal_summary = %post_fai_inbox_probe_loop_terminal_summary,
        nspi_address_book_probe_only,
        outlook_profile_stage = %outlook_profile_stage,
        next_expected_client_step = %next_expected_client_step,
        recent_execute_summaries = %recent_execute_summaries,
        outlook_view_failure_trace_event_count =
            session.post_hierarchy_actions.outlook_view_failure_trace_events.len(),
        outlook_view_failure_trace_summary = %outlook_view_failure_trace_summary,
        outlook_stream_batch_status = outlook_stream_batch_status,
        outlook_stream_batch_summaries = %outlook_stream_batch_summaries,
        "rca debug mapi session disconnect"
    );
    tracing::debug!(
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
        outlook_view_failure_trace_event_count =
            session.post_hierarchy_actions.outlook_view_failure_trace_events.len(),
        outlook_view_failure_trace_summary = %outlook_view_failure_trace_summary,
        outlook_stream_batch_status = outlook_stream_batch_status,
        outlook_stream_batch_summaries = %outlook_stream_batch_summaries,
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
        required_default_folder_coverage = %required_default_folder_coverage,
        completed_sync_checkpoint_summaries = %completed_sync_checkpoint_summaries,
        partial_scope_checkpoint_not_stored_count,
        partial_scope_checkpoint_not_stored_expected,
        nspi_address_book_probe_only,
        outlook_profile_stage = %outlook_profile_stage,
        next_expected_client_step = %next_expected_client_step,
        outlook_view_failure_trace_event_count =
            session.post_hierarchy_actions.outlook_view_failure_trace_events.len(),
        outlook_view_failure_trace_summary = %outlook_view_failure_trace_summary,
        outlook_stream_batch_status = outlook_stream_batch_status,
        outlook_stream_batch_summaries = %outlook_stream_batch_summaries,
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
            if post_fai_inbox_probe_loop_terminal {
                "post_fai_inbox_folder_type_probe_loop"
            } else if clean_client_close_after_sync {
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
        required_default_folder_coverage = %required_default_folder_coverage,
        completed_sync_checkpoint_summaries = %completed_sync_checkpoint_summaries,
        partial_scope_checkpoint_not_stored_count,
        partial_scope_checkpoint_not_stored_expected,
        post_fai_inbox_probe_loop_terminal,
        post_fai_inbox_probe_loop_terminal_summary = %post_fai_inbox_probe_loop_terminal_summary,
        nspi_address_book_probe_only,
        outlook_profile_stage = %outlook_profile_stage,
        next_expected_client_step = %next_expected_client_step,
        "rca debug mapi disconnect verdict"
    );

    if endpoint == MapiEndpoint::Emsmdb
        && request_type == "Disconnect"
        && post_fai_inbox_probe_loop_terminal
    {
        tracing::warn!(
            rca_debug = true,
            rca_warning = "post_fai_inbox_folder_type_probe_loop_terminal",
            adapter = "mapi",
            endpoint = endpoint_label,
            tenant_id = %principal.tenant_id,
            account_id = %principal.account_id,
            mailbox = %principal.email,
            request_type = %request_type,
            mapi_request_id = %request_id,
            post_fai_inbox_probe_loop_terminal_summary =
                %post_fai_inbox_probe_loop_terminal_summary,
            recent_execute_summaries = %recent_execute_summaries,
            outlook_view_failure_trace_summary = %outlook_view_failure_trace_summary,
            "rca debug mapi terminal post fai inbox folder type probe loop"
        );
    }

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
            post_hierarchy_default_folder_membership =
                %post_hierarchy_summary.last_default_folder_hierarchy_membership_summary,
            post_hierarchy_last_getprops_request_contract =
                %post_hierarchy_summary.last_getprops_request_contract,
            post_hierarchy_last_setprops_request_contract =
                %post_hierarchy_summary.last_setprops_request_contract,
            post_hierarchy_last_request_contract =
                %post_hierarchy_summary.request_contract_sequence,
            sync_source_summaries = %sync_source_summaries,
            live_handle_summaries = %live_handle_summaries,
            "rca debug mapi post hierarchy disconnect before content sync"
        );
    }
}
