use super::*;

pub(super) enum SyncConfigureFlow {
    Continue,
    StopBatch,
}

pub(super) async fn append_synchronization_configure_response<S: ExchangeStore>(
    store: &S,
    principal: &AccountPrincipal,
    session: &mut MapiSession,
    handle_slots: &mut Vec<u32>,
    request: &RopRequest,
    request_id: &str,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    responses: &mut Vec<u8>,
    output_handles: &mut Vec<u32>,
    content_sync_configure_observed: &mut bool,
) -> SyncConfigureFlow {
    let Some(folder_id) =
        input_object(session, &handle_slots, &request).and_then(MapiObject::folder_id)
    else {
        responses.extend_from_slice(&rop_error_response(
            0x70,
            request.response_handle_index(),
            0x8004_010F,
        ));
        return SyncConfigureFlow::Continue;
    };
    let sync_type = request.sync_type();
    if MapiSyncType::from_u8(sync_type).is_none() {
        responses.extend_from_slice(&rop_error_response(
            0x70,
            request.response_handle_index(),
            0x8004_0102,
        ));
        return SyncConfigureFlow::StopBatch;
    }
    let sync_send_options = request.sync_send_options();
    let sync_flags = request.sync_flags();
    let sync_extra_flags = request.sync_extra_flags();
    let sync_property_tags = request.sync_property_tags();
    if !property_tags_are_supported(&sync_property_tags) {
        responses.extend_from_slice(&rop_error_response(
            0x70,
            request.response_handle_index(),
            0x8004_0102,
        ));
        return SyncConfigureFlow::StopBatch;
    }
    let sync_property_tags_hex = sync_property_tags
        .iter()
        .map(|tag| format!("0x{tag:08x}"))
        .collect::<Vec<_>>()
        .join(",");
    let partial_item_requested = sync_send_options & SYNC_SEND_OPTION_PARTIAL_ITEM != 0;
    let recover_mode_requested = sync_send_options & SYNC_SEND_OPTION_RECOVER_MODE != 0;
    let partial_item_behavior = if partial_item_requested && sync_type == 0x01 {
        "full-item-fallback"
    } else if partial_item_requested {
        "ignored-non-content-sync"
    } else {
        "not-requested"
    };
    let checkpoint_kind = sync_checkpoint_kind(sync_type);
    let checkpoint_mailbox_id = sync_checkpoint_mailbox_id(folder_id, sync_type, mailboxes);
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
        .fetch_mapi_sync_checkpoint(principal.account_id, checkpoint_mailbox_id, checkpoint_kind)
        .await
    {
        Ok(checkpoint) => checkpoint,
        Err(_) => {
            responses.extend_from_slice(&rop_error_response(
                0x70,
                request.response_handle_index(),
                0x8004_0102,
            ));
            return SyncConfigureFlow::Continue;
        }
    };
    let checkpoint_status = checkpoint
        .as_ref()
        .map(|checkpoint| hierarchy_checkpoint_status(checkpoint_kind, folder_id, checkpoint))
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
    // The operational checkpoint cannot bound client-visible tombstones: a
    // different OST can upload an older IdsetGiven, and hierarchy deletions
    // that were already observed by another session still have to be eligible
    // for that client. [MS-OXCFXICS] section 3.2.5.3 derives the download only
    // from the initial client state, never from server-side per-client state.
    let since = 0;
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
            return SyncConfigureFlow::Continue;
        }
    };
    let all_sync_mailboxes = sync_mailboxes_with_collaboration_counts(
        sync_mailboxes_for_excluding_deleted(
            folder_id,
            sync_type,
            mailboxes,
            &session.deleted_advertised_special_folders,
        ),
        snapshot,
        folder_id,
        sync_type,
    );
    let state_sync_mailboxes = sync_mailboxes_with_collaboration_counts(
        sync_state_mailboxes_for_excluding_deleted(
            folder_id,
            sync_type,
            mailboxes,
            &session.deleted_advertised_special_folders,
        ),
        snapshot,
        folder_id,
        sync_type,
    );
    let all_sync_emails = sync_emails_for(folder_id, sync_type, mailboxes, emails);
    let all_special_sync_objects =
        special_sync_objects_for(folder_id, sync_type, snapshot, principal);
    log_calendar_special_sync_objects(principal, folder_id, sync_type, &all_special_sync_objects);
    log_special_sync_objects(principal, folder_id, sync_type, &all_special_sync_objects);
    let available_sync_mailbox_count = all_sync_mailboxes.len();
    let available_sync_email_count = all_sync_emails.len();
    let available_special_sync_object_count = all_special_sync_objects.len();
    let (delta_sync_mailboxes, delta_sync_emails, delta_special_sync_objects) = if checkpoint
        .is_some()
    {
        let changed_special_ids = changed_special_ids_for_folder(folder_id, snapshot, &changes);
        (
            changed_sync_mailboxes(all_sync_mailboxes.clone(), &changes.changed_mailbox_ids),
            changed_sync_emails(all_sync_emails.clone(), &changes.changed_message_ids),
            changed_special_sync_objects(all_special_sync_objects.clone(), &changed_special_ids),
        )
    } else {
        (
            all_sync_mailboxes.clone(),
            all_sync_emails.clone(),
            all_special_sync_objects.clone(),
        )
    };
    let sync_attachment_facts = sync_attachment_facts_for_with_embedded_content(
        store,
        principal.account_id,
        folder_id,
        &all_sync_emails,
        snapshot,
    )
    .await;
    let aggregate_sync_emails = if sync_type == 0x02 {
        emails.to_vec()
    } else {
        all_sync_emails.clone()
    };
    let state_attachment_facts = sync_attachment_facts_for(folder_id, &all_sync_emails, snapshot);
    let aggregate_attachment_facts =
        sync_attachment_facts_for(folder_id, &aggregate_sync_emails, snapshot);
    let mut deleted_message_ids =
        mapi_message_ids_for_deleted_changes(store, principal, &changes.deleted_message_ids)
            .await
            .unwrap_or_default();
    if checkpoint_kind == MapiCheckpointKind::Hierarchy {
        deleted_message_ids.extend(changes.deleted_mailbox_object_ids.iter().copied());
        deleted_message_ids.extend(changes.deleted_search_folder_object_ids.iter().copied());
    }
    if folder_id == NOTES_FOLDER_ID {
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
    if folder_id == JOURNAL_FOLDER_ID {
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
    deleted_message_ids.extend(
        deleted_special_object_ids_for_folder(store, principal, folder_id, snapshot, &changes)
            .await,
    );
    if folder_id == CONVERSATION_ACTION_SETTINGS_FOLDER_ID {
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
    let folder_versions = snapshot.folder_versions();
    let download_change_facts = mapi_mailstore::download_change_facts(
        sync_type,
        sync_flags,
        folder_id,
        &all_sync_mailboxes,
        &all_sync_emails,
        &sync_attachment_facts,
        &all_special_sync_objects,
        &folder_versions,
    );
    let initial_state = mapi_mailstore::initial_sync_state_stream(sync_type);
    let transfer_buffer = mapi_mailstore::sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions(
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
        &deleted_message_ids,
        mailboxes,
        &state_sync_mailboxes,
        &all_sync_emails,
        &state_attachment_facts,
        &all_special_sync_objects,
        &aggregate_sync_emails,
        &aggregate_attachment_facts,
        &folder_versions,
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
    let tenant_id_debug = principal.tenant_id.to_string();
    let account_id_debug = principal.account_id.to_string();
    mapi_mailstore::log_fai_content_sync_debug(
        sync_type,
        sync_flags,
        folder_id,
        principal.account_id,
        &all_special_sync_objects,
        &transfer_buffer,
        mapi_mailstore::FaiContentSyncDebugContext {
            mailbox: principal.email.as_str(),
            tenant: tenant_id_debug.as_str(),
            account: account_id_debug.as_str(),
            mapi_request_id: request_id,
            request_rop_id: "0x70",
            checkpoint_kind: checkpoint_kind.as_str(),
            active_transfer_selection: "initial_full_candidate",
        },
    );
    // [MS-OXCFXICS] sections 3.2.5.2 and 3.2.5.3: the client-uploaded
    // ICS state, not LPE's operational checkpoint, selects the download.
    // Keep the checkpoint below for completion diagnostics only.
    let incremental_transfer_buffer: Option<Vec<u8>> = None;
    let checkpoint_delta_mailbox_count = delta_sync_mailboxes.len();
    let checkpoint_delta_email_count = delta_sync_emails.len();
    let checkpoint_delta_special_object_count = delta_special_sync_objects.len();
    let checkpoint_deleted_message_count = deleted_message_ids.len();
    let incremental_transfer_buffer_bytes = incremental_transfer_buffer
        .as_ref()
        .map(|buffer| buffer.len())
        .unwrap_or_default();
    let checkpoint_delta_total_count = checkpoint_delta_mailbox_count
        + checkpoint_delta_email_count
        + checkpoint_delta_special_object_count
        + checkpoint_deleted_message_count;
    let checkpoint_zero_delta = checkpoint.is_some() && checkpoint_delta_total_count == 0;
    let checkpoint_incremental_response_candidate = false;
    let initial_checkpoint_delta_selected = false;
    let initial_transfer_selection = "full_pending_client_state_selection";
    let scope_flags_present = sync_type != 0x01 || sync_flags & 0x0030 != 0;
    let default_fai_scope_requested = all_sync_emails.is_empty()
        && all_special_sync_objects
            .iter()
            .all(|object| object.associated)
        && all_special_sync_objects
            .iter()
            .any(|object| object.associated);
    let normal_scope_requested = sync_type != 0x01
        || (scope_flags_present && sync_flags & 0x0020 != 0)
        || (!scope_flags_present && !default_fai_scope_requested);
    let fai_scope_requested = sync_type != 0x01
        || (scope_flags_present && sync_flags & 0x0010 != 0)
        || (!scope_flags_present && default_fai_scope_requested);
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
        .filter(|object| object.associated && !fai_scope_requested)
        .count();
    let checkpoint_store_allowed = suppressed_normal_sync_object_count == 0
        && (suppressed_fai_sync_object_count == 0
            || (normal_scope_requested && !fai_scope_requested));
    let checkpoint_skip_reason = if checkpoint_store_allowed {
        ""
    } else {
        "partial_content_scope_suppressed_objects"
    };
    let empty_content_sync_state_only = sync_type == 0x01
        && wire_sync_email_count == 0
        && wire_sync_special_object_count == 0
        && checkpoint_deleted_message_count == 0
        && transfer_buffer.len() == state.len().saturating_add(4);
    tracing::info!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = "emsmdb",
        mailbox = %principal.email,
        request_type = "Execute",
        mapi_request_id = request_id,
        request_rop_id = "0x70",
        folder_id = format_args!("0x{folder_id:016x}"),
        folder_role,
        folder_container_class,
        sync_type = format_args!("0x{sync_type:02x}"),
        sync_send_options = format_args!("0x{sync_send_options:02x}"),
        sync_send_options_partial_item = partial_item_requested,
        sync_send_options_recover_mode = recover_mode_requested,
        sync_partial_item_behavior = partial_item_behavior,
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
        empty_content_sync_state_only,
        outlook_no_current_item_candidate = empty_content_sync_state_only,
        suppressed_normal_sync_object_count,
        suppressed_fai_sync_object_count,
        checkpoint_store_allowed,
        checkpoint_skip_reason,
        checkpoint_delta_mailbox_count,
        checkpoint_delta_email_count,
        checkpoint_delta_special_object_count,
        checkpoint_delta_total_count,
        checkpoint_zero_delta,
        checkpoint_incremental_response_candidate,
        initial_checkpoint_delta_selected,
        checkpoint_delta_selection_gate = "disabled_client_state_is_authoritative",
        initial_transfer_selection,
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
        initial_sync_state_bytes = initial_state.len(),
        generated_sync_state_bytes = state.len(),
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
            checkpoint_zero_delta,
            sync_type,
            sync_flags,
            state: initial_state.clone(),
            initial_state,
            state_upload_property_tag: None,
            state_upload_buffer: Vec::new(),
            client_state_uploaded_bytes: 0,
            client_state_uploaded_marker_mask: 0,
            client_state_selection_enabled: true,
            client_state_selection_invalidated: false,
            client_state_selection_applied: false,
            download_change_facts,
            incremental_transfer_buffer,
            transfer_buffer,
            transfer_position: 0,
        },
    );
    set_handle_slot(handle_slots, request.output_handle_index, handle);
    responses.extend_from_slice(&rop_synchronization_configure_response(&request));
    output_handles.push(handle);
    *content_sync_configure_observed = sync_type == 0x01;
    SyncConfigureFlow::Continue
}
