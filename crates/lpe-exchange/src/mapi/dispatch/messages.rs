use super::*;

pub(super) fn canonical_message_folder_id(email: &JmapEmail, mailboxes: &[JmapMailbox]) -> u64 {
    email
        .mailbox_states
        .iter()
        .find_map(|state| {
            mailboxes
                .iter()
                .find(|mailbox| mailbox.id == state.mailbox_id)
                .map(mapi_folder_id)
        })
        .or_else(|| {
            mailboxes
                .iter()
                .find(|mailbox| mailbox.id == email.mailbox_id)
                .map(mapi_folder_id)
        })
        .unwrap_or_else(|| {
            mailboxes
                .iter()
                .find(|mailbox| mailbox.role == email.mailbox_role)
                .map(mapi_folder_id)
                .unwrap_or(INBOX_FOLDER_ID)
        })
}

pub(super) fn fallback_open_message_folder_id(
    requested_folder_id: u64,
    email: &JmapEmail,
    mailboxes: &[JmapMailbox],
) -> u64 {
    if email_matches_folder(email, requested_folder_id, mailboxes) {
        requested_folder_id
    } else {
        canonical_message_folder_id(email, mailboxes)
    }
}

pub(super) fn open_message_folder_id(request: &RopRequest, message_id: u64) -> u64 {
    request.folder_id().unwrap_or_else(|| {
        if crate::mapi_store::is_outlook_local_freebusy_message_id(message_id) {
            FREEBUSY_DATA_FOLDER_ID
        } else {
            INBOX_FOLDER_ID
        }
    })
}

pub(super) fn unique_message_for_id(message_id: u64, emails: &[JmapEmail]) -> Option<&JmapEmail> {
    let mut matches = emails
        .iter()
        .filter(|email| mapi_item_id_matches(&email.id, message_id));
    let email = matches.next()?;
    matches.next().is_none().then_some(email)
}

pub(super) fn persisted_message_delete_is_best_effort(object: Option<&MapiObject>) -> bool {
    matches!(object, Some(MapiObject::Message { .. }))
}

pub(super) fn append_create_message_response(
    principal: &AccountPrincipal,
    session: &mut MapiSession,
    handle_slots: &mut Vec<u32>,
    request: &RopRequest,
    mailboxes: &[JmapMailbox],
    snapshot: &MapiMailStoreSnapshot,
    responses: &mut Vec<u8>,
    output_handles: &mut Vec<u32>,
) {
    let folder_id = request.folder_id().unwrap_or_else(|| {
        input_object(session, handle_slots, request)
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
        return;
    }
    if snapshot.collaboration_folder_for_id(folder_id).is_none()
        && folder_row_for_id(folder_id, mailboxes).is_none()
        && snapshot.public_folder_for_id(folder_id).is_none()
        && folder_id != CALENDAR_FOLDER_ID
        && !synthetic_folder_allows_create_message(folder_id)
    {
        responses.extend_from_slice(&rop_error_response(
            0x06,
            request.output_handle_index.unwrap_or(0),
            0x8004_010F,
        ));
        return;
    }

    let created_at = current_windows_filetime();
    let initial_message_properties = || {
        HashMap::from([
            (PID_TAG_CREATION_TIME, MapiValue::U64(created_at)),
            (PID_TAG_LAST_MODIFICATION_TIME, MapiValue::U64(created_at)),
        ])
    };
    let pending_object = if request.create_message_associated()
        && !matches!(
            folder_id,
            COMMON_VIEWS_FOLDER_ID | CONVERSATION_ACTION_SETTINGS_FOLDER_ID
        ) {
        MapiObject::PendingAssociatedMessage {
            folder_id,
            properties: initial_message_properties(),
        }
    } else {
        match snapshot
            .collaboration_folder_for_id(folder_id)
            .map(|folder| folder.kind)
        {
            Some(MapiCollaborationFolderKind::Contacts) => MapiObject::PendingContact {
                folder_id,
                properties: initial_message_properties(),
            },
            Some(MapiCollaborationFolderKind::Calendar) => MapiObject::PendingEvent {
                folder_id,
                properties: initial_message_properties(),
            },
            None if folder_id == CALENDAR_FOLDER_ID => MapiObject::PendingEvent {
                folder_id,
                properties: initial_message_properties(),
            },
            Some(MapiCollaborationFolderKind::Task) => MapiObject::PendingTask {
                folder_id,
                properties: initial_message_properties(),
            },
            _ if folder_id == NOTES_FOLDER_ID => MapiObject::PendingNote {
                folder_id,
                properties: initial_message_properties(),
            },
            _ if folder_id == JOURNAL_FOLDER_ID => MapiObject::PendingJournalEntry {
                folder_id,
                properties: initial_message_properties(),
            },
            _ if folder_id == CONVERSATION_ACTION_SETTINGS_FOLDER_ID => {
                MapiObject::PendingConversationAction {
                    folder_id,
                    properties: initial_message_properties(),
                }
            }
            _ if folder_id == FREEBUSY_DATA_FOLDER_ID => MapiObject::PendingAssociatedMessage {
                folder_id,
                properties: initial_message_properties(),
            },
            _ if folder_id == COMMON_VIEWS_FOLDER_ID => MapiObject::PendingNavigationShortcut {
                folder_id,
                properties: initial_message_properties(),
            },
            _ => MapiObject::PendingMessage {
                folder_id,
                properties: initial_message_properties(),
                recipients: Vec::new(),
            },
        }
    };
    let handle = session.allocate_output_handle(request.output_handle_index, pending_object);
    set_handle_slot(handle_slots, request.output_handle_index, handle);
    responses.extend_from_slice(&rop_create_message_response(request));
    output_handles.push(handle);
}

pub(super) fn append_save_changes_message_response(
    responses: &mut Vec<u8>,
    handle_slots: &mut Vec<u32>,
    request: &RopRequest,
    handle: u32,
    message_id: u64,
) {
    set_handle_slot(handle_slots, Some(request.response_handle_index()), handle);
    responses.extend_from_slice(&rop_save_changes_message_response(request, message_id));
}

pub(super) fn stage_message_property_values(
    session: &mut MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    values: Vec<(u32, MapiValue)>,
) -> Result<()> {
    let handle = input_handle(handle_slots, request).ok_or_else(|| anyhow!("missing handle"))?;
    let object = session
        .handles
        .get_mut(&handle)
        .ok_or_else(|| anyhow!("missing message object"))?;
    let MapiObject::Message {
        pending_properties, ..
    } = object
    else {
        return Err(anyhow!("MAPI object is not a message"));
    };
    let (canonical_values, _custom_values) = split_custom_property_values(values.clone());
    let followup_values = canonical_values
        .into_iter()
        .filter(|(tag, _)| {
            !matches!(
                *tag,
                PID_TAG_SUBJECT_W
                    | PID_TAG_NORMALIZED_SUBJECT_W
                    | PID_TAG_BODY_W
                    | PID_TAG_SOURCE_KEY
                    | PID_TAG_CHANGE_KEY
                    | PID_TAG_PREDECESSOR_CHANGE_LIST
            )
        })
        .collect::<Vec<_>>();
    message_followup_update_from_mapi_values(followup_values)?;
    apply_mapi_property_values_to_map(pending_properties, values);
    Ok(())
}

pub(super) async fn apply_staged_message_property_values<S>(
    store: &S,
    principal: &AccountPrincipal,
    folder_id: u64,
    message_id: u64,
    pending_properties: HashMap<u32, MapiValue>,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
) -> Result<()>
where
    S: ExchangeStore,
{
    let values = pending_properties.into_iter().collect::<Vec<_>>();
    let object = MapiObject::Message {
        folder_id,
        message_id,
        saved_email: None,
        pending_properties: HashMap::new(),
    };
    apply_supported_object_property_values(
        store, principal, &object, values, mailboxes, emails, snapshot,
    )
    .await
}

pub(super) async fn apply_staged_message_recipient_replacement<S>(
    store: &S,
    principal: &AccountPrincipal,
    folder_id: u64,
    message_id: u64,
    recipients: &[PendingRecipient],
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
) -> Result<()>
where
    S: ExchangeStore,
{
    let Some(email) = message_for_id(folder_id, message_id, mailboxes, emails) else {
        return Err(anyhow::anyhow!("canonical message not found"));
    };
    let (to, cc, bcc) = submitted_recipients_from_pending(recipients);
    store
        .replace_message_recipients(
            principal.account_id,
            email.id,
            &to,
            &cc,
            &bcc,
            AuditEntryInput {
                actor: principal.email.clone(),
                action: "mapi-replace-message-recipients".to_string(),
                subject: format!("message:{}", email.id),
            },
        )
        .await?;
    Ok(())
}

pub(super) async fn delete_canonical_message_text_properties<S>(
    store: &S,
    principal: &AccountPrincipal,
    object: Option<&MapiObject>,
    property_tags: &[u32],
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
) -> Result<bool>
where
    S: ExchangeStore,
{
    let Some(MapiObject::Message {
        folder_id,
        message_id,
        ..
    }) = object
    else {
        return Ok(false);
    };
    let mut values = Vec::new();
    for tag in property_tags {
        match *tag {
            PID_TAG_SUBJECT_W | PID_TAG_NORMALIZED_SUBJECT_W => {
                values.push((PID_TAG_SUBJECT_W, MapiValue::String(String::new())));
            }
            PID_TAG_BODY_W => values.push((PID_TAG_BODY_W, MapiValue::String(String::new()))),
            _ => {}
        }
    }
    if values.is_empty() {
        return Ok(false);
    }
    values.sort_by_key(|(tag, _)| *tag);
    values.dedup_by_key(|(tag, _)| *tag);
    let target = MapiObject::Message {
        folder_id: *folder_id,
        message_id: *message_id,
        saved_email: None,
        pending_properties: HashMap::new(),
    };
    apply_supported_object_property_values(
        store, principal, &target, values, mailboxes, emails, snapshot,
    )
    .await?;
    Ok(true)
}

fn copyable_message_followup_property_tag(tag: u32) -> bool {
    matches!(
        canonical_property_storage_tag(tag),
        PID_TAG_MESSAGE_FLAGS
            | PID_TAG_FLAG_STATUS
            | PID_TAG_FOLLOWUP_ICON
            | PID_TAG_TODO_ITEM_FLAGS
            | PID_TAG_FLAG_COMPLETE_TIME
            | PID_LID_TASK_START_DATE_TAG
            | PID_LID_TASK_DUE_DATE_TAG
            | PID_LID_REMINDER_SET_TAG
            | PID_LID_REMINDER_TIME_TAG
            | PID_LID_REMINDER_SIGNAL_TIME_TAG
            | PID_LID_FLAG_REQUEST_W_TAG
            | PID_TAG_SWAPPED_TODO_STORE
            | PID_TAG_SWAPPED_TODO_DATA
            | PID_NAME_KEYWORDS_TAG
    )
}

pub(super) async fn copy_message_followup_property_values_for_request<S>(
    store: &S,
    principal: &AccountPrincipal,
    source: Option<&MapiObject>,
    destination: Option<&MapiObject>,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    property_tags: &[u32],
) -> Result<Option<Vec<(usize, u32, u32)>>>
where
    S: ExchangeStore,
{
    if property_tags.is_empty()
        || !property_tags
            .iter()
            .copied()
            .all(copyable_message_followup_property_tag)
    {
        return Ok(None);
    }
    let Some(MapiObject::Message {
        folder_id: source_folder_id,
        message_id: source_message_id,
        ..
    }) = source
    else {
        return Ok(None);
    };
    let Some(destination_object @ MapiObject::Message { .. }) = destination else {
        return Ok(None);
    };
    let Some(email) = message_for_id(*source_folder_id, *source_message_id, mailboxes, emails)
    else {
        return Ok(None);
    };
    let mut values = Vec::new();
    let mut problems = Vec::new();
    for (index, property_tag) in property_tags.iter().copied().enumerate() {
        match normal_message_debug_property_value(email, property_tag) {
            Some(value) => values.push((canonical_property_storage_tag(property_tag), value)),
            None => problems.push((index, property_tag, 0x8004_010F)),
        }
    }
    if !values.is_empty() {
        apply_supported_object_property_values(
            store,
            principal,
            destination_object,
            values,
            mailboxes,
            emails,
            snapshot,
        )
        .await?;
    }
    Ok(Some(problems))
}

pub(super) async fn copy_all_message_followup_property_values_for_request<S>(
    store: &S,
    principal: &AccountPrincipal,
    source: Option<&MapiObject>,
    destination: Option<&MapiObject>,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    excluded_property_tags: &[u32],
) -> Result<bool>
where
    S: ExchangeStore,
{
    let Some(MapiObject::Message {
        folder_id: source_folder_id,
        message_id: source_message_id,
        ..
    }) = source
    else {
        return Ok(false);
    };
    let Some(destination_object @ MapiObject::Message { .. }) = destination else {
        return Ok(false);
    };
    let Some(email) = message_for_id(*source_folder_id, *source_message_id, mailboxes, emails)
    else {
        return Ok(false);
    };
    let excluded = excluded_property_tags
        .iter()
        .copied()
        .map(canonical_property_storage_tag)
        .collect::<HashSet<_>>();
    let mut values = Vec::new();
    for property_tag in [
        PID_TAG_MESSAGE_FLAGS,
        PID_TAG_FLAG_STATUS,
        PID_TAG_FOLLOWUP_ICON,
        PID_TAG_TODO_ITEM_FLAGS,
        PID_TAG_FLAG_COMPLETE_TIME,
        PID_LID_TASK_START_DATE_TAG,
        PID_LID_TASK_DUE_DATE_TAG,
        PID_LID_REMINDER_SET_TAG,
        PID_LID_REMINDER_TIME_TAG,
        PID_LID_REMINDER_SIGNAL_TIME_TAG,
        PID_LID_FLAG_REQUEST_W_TAG,
        PID_TAG_SWAPPED_TODO_STORE,
        PID_TAG_SWAPPED_TODO_DATA,
        PID_NAME_KEYWORDS_TAG,
    ] {
        if excluded.contains(&property_tag) {
            continue;
        }
        if let Some(value) = normal_message_debug_property_value(email, property_tag) {
            values.push((property_tag, value));
        }
    }
    if values.is_empty() {
        return Ok(false);
    }
    apply_supported_object_property_values(
        store,
        principal,
        destination_object,
        values,
        mailboxes,
        emails,
        snapshot,
    )
    .await?;
    Ok(true)
}

pub(super) fn append_reload_cached_information_response(
    session: &MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    responses: &mut Vec<u8>,
) {
    if request.reload_cached_information_reserved() != Some(0) {
        responses.extend_from_slice(&rop_error_response(
            0x10,
            request.response_handle_index(),
            0x8007_0057,
        ));
        return;
    }
    responses.extend_from_slice(&rop_reload_cached_information_response(
        request,
        input_object(session, handle_slots, request),
        mailboxes,
        emails,
        snapshot,
    ));
}

pub(super) async fn append_set_message_read_flag_response<S>(
    store: &S,
    principal: &AccountPrincipal,
    session: &mut MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    responses: &mut Vec<u8>,
) where
    S: ExchangeStore,
{
    let Some(object) = input_object(session, handle_slots, request).cloned() else {
        responses.extend_from_slice(&rop_error_response(
            0x11,
            request.response_handle_index(),
            0x0000_04B9,
        ));
        return;
    };
    if !read_flags_are_valid(request.read_flags(), false) {
        responses.extend_from_slice(&rop_error_response(
            0x11,
            request.response_handle_index(),
            0x8007_0057,
        ));
        return;
    }
    if let MapiObject::PublicFolderItem {
        folder_id, item_id, ..
    } = object
    {
        let Some(item) = snapshot.public_folder_item_for_id(folder_id, item_id) else {
            responses.extend_from_slice(&rop_error_response(
                0x11,
                request.response_handle_index(),
                0x8004_010F,
            ));
            return;
        };
        let unread = unread_from_read_flags(request.read_flags());
        let changed = unread.is_some_and(|unread| unread == item.item.is_read);
        if let Some(unread) = unread {
            let patch = lpe_storage::PublicFolderPerUserStatePatch {
                item_id: item.item.id,
                is_read: !unread,
                last_seen_change: Some(item.item.change_counter),
                private_json: None,
            };
            if store
                .patch_public_folder_per_user_state(
                    principal.account_id,
                    item.item.public_folder_id,
                    &[patch],
                )
                .await
                .is_err()
            {
                responses.extend_from_slice(&rop_error_response(
                    0x11,
                    request.response_handle_index(),
                    0x8004_010F,
                ));
                return;
            }
        }
        if changed {
            session.record_notification(MapiNotificationEvent::content(folder_id, None));
        }
        responses.extend_from_slice(&rop_set_message_read_flag_response(request, changed));
        return;
    }
    let MapiObject::Message {
        folder_id,
        message_id,
        saved_email,
        ..
    } = object
    else {
        responses.extend_from_slice(&rop_error_response(
            0x11,
            request.response_handle_index(),
            0x0000_04B9,
        ));
        return;
    };
    let Some(email) = message_for_id(folder_id, message_id, mailboxes, emails)
        .or(saved_email.as_ref().map(|saved| &saved.email))
    else {
        responses.extend_from_slice(&rop_error_response(
            0x11,
            request.response_handle_index(),
            0x8004_010F,
        ));
        return;
    };
    let unread = unread_from_read_flags(request.read_flags());
    let changed = unread.is_some_and(|unread| unread != email.unread);
    if let Some(unread) = unread {
        if !snapshot
            .folder_access_for_principal(folder_id, principal.account_id)
            .map(|access| access.may_write)
            .unwrap_or(true)
        {
            responses.extend_from_slice(&rop_error_response(
                0x11,
                request.response_handle_index(),
                0x8007_0005,
            ));
            return;
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
            return;
        }
    }
    if changed {
        session.record_notification(MapiNotificationEvent::content(folder_id, None));
    }
    responses.extend_from_slice(&rop_set_message_read_flag_response(request, changed));
}

pub(super) async fn append_set_read_flags_response<S>(
    store: &S,
    principal: &AccountPrincipal,
    session: &MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    responses: &mut Vec<u8>,
) where
    S: ExchangeStore,
{
    let folder_id = match input_object(session, handle_slots, request) {
        Some(MapiObject::Folder { folder_id, .. }) => *folder_id,
        _ => {
            responses.extend_from_slice(&rop_error_response(
                0x66,
                request.response_handle_index(),
                0x0000_04B9,
            ));
            return;
        }
    };
    if request.want_asynchronous().is_none() || !read_flags_are_valid(request.read_flags(), true) {
        responses.extend_from_slice(&rop_error_response(
            0x66,
            request.response_handle_index(),
            0x8007_0057,
        ));
        return;
    }
    let unread = unread_from_read_flags(request.read_flags());
    let mut partial_completion = false;
    let message_ids = request.message_ids();
    if let Some(folder) = snapshot.public_folder_for_id(folder_id) {
        if let Some(unread) = unread {
            let mut patches = Vec::new();
            for message_id in message_ids {
                let Some(item) = snapshot.public_folder_item_for_id(folder_id, message_id) else {
                    partial_completion = true;
                    continue;
                };
                patches.push(lpe_storage::PublicFolderPerUserStatePatch {
                    item_id: item.item.id,
                    is_read: !unread,
                    last_seen_change: Some(item.item.change_counter),
                    private_json: None,
                });
            }
            if !patches.is_empty()
                && store
                    .patch_public_folder_per_user_state(
                        principal.account_id,
                        folder.folder.id,
                        &patches,
                    )
                    .await
                    .is_err()
            {
                partial_completion = true;
            }
        }
        responses.extend_from_slice(&rop_set_read_flags_response(request, partial_completion));
        return;
    }
    for message_id in message_ids {
        let Some(email) = message_for_id(folder_id, message_id, mailboxes, emails).or_else(|| {
            emails
                .iter()
                .find(|email| mapi_item_id_matches(&email.id, message_id))
        }) else {
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
    responses.extend_from_slice(&rop_set_read_flags_response(request, partial_completion));
}

pub(super) async fn append_move_copy_messages_response<S>(
    store: &S,
    principal: &AccountPrincipal,
    session: &mut MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    responses: &mut Vec<u8>,
) where
    S: ExchangeStore,
{
    if request.move_copy_want_asynchronous().is_none()
        || request.move_copy_want_copy_raw().is_none()
    {
        responses.extend_from_slice(&rop_error_response(
            0x33,
            request.response_handle_index(),
            0x8007_0057,
        ));
        return;
    }
    let source_folder_id = match input_object(session, handle_slots, request) {
        Some(MapiObject::Folder { folder_id, .. }) => *folder_id,
        _ => {
            tracing::info!(
                adapter = "mapi",
                endpoint = "emsmdb",
                mailbox = %principal.email,
                request_type = "Execute",
                request_rop_id = "0x33",
                input_handle_index = request.input_handle_index().unwrap_or(0),
                message_ids = %format_debug_object_ids(&request.move_copy_message_ids()),
                want_copy = request.move_copy_want_copy(),
                failure = "source_handle_not_folder",
                "rca debug mapi move copy messages failure"
            );
            responses.extend_from_slice(&rop_error_response(
                0x33,
                request.response_handle_index(),
                0x0000_04B9,
            ));
            return;
        }
    };
    let target_folder_id = match request
        .move_copy_target_handle(handle_slots)
        .and_then(|handle| {
            session
                .handles
                .get(&handle)
                .and_then(|object| object.folder_id())
        }) {
        Some(folder_id) => folder_id,
        None => {
            tracing::info!(
                adapter = "mapi",
                endpoint = "emsmdb",
                mailbox = %principal.email,
                request_type = "Execute",
                request_rop_id = "0x33",
                source_folder_id = format_args!("0x{source_folder_id:016x}"),
                message_ids = %format_debug_object_ids(&request.move_copy_message_ids()),
                want_copy = request.move_copy_want_copy(),
                failure = "target_handle_not_folder",
                "rca debug mapi move copy messages failure"
            );
            responses.extend_from_slice(&rop_error_response(
                0x33,
                request.response_handle_index(),
                0x8004_010F,
            ));
            return;
        }
    };
    if source_folder_id == crate::mapi::identity::RECOVERABLE_ITEMS_ROOT_FOLDER_ID {
        tracing::info!(
            adapter = "mapi",
            endpoint = "emsmdb",
            mailbox = %principal.email,
            request_type = "Execute",
            request_rop_id = "0x33",
            source_folder_id = format_args!("0x{source_folder_id:016x}"),
            target_folder_id = format_args!("0x{target_folder_id:016x}"),
            message_ids = %format_debug_object_ids(&request.move_copy_message_ids()),
            want_copy = request.move_copy_want_copy(),
            failure = "recoverable_items_root_source",
            "rca debug mapi move copy messages failure"
        );
        responses.extend_from_slice(&rop_error_response(
            0x33,
            request.response_handle_index(),
            0x8004_0102,
        ));
        return;
    }
    if matches!(source_folder_id, NOTES_FOLDER_ID | JOURNAL_FOLDER_ID) {
        let mut partial_completion = false;
        for message_id in request.move_copy_message_ids() {
            if source_folder_id == NOTES_FOLDER_ID {
                let Some(note) = snapshot.note_for_id(source_folder_id, message_id) else {
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
            let Some(entry) = snapshot.journal_entry_for_id(source_folder_id, message_id) else {
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
        return;
    }
    if crate::mapi_store::recoverable_storage_folder(source_folder_id).is_some() {
        if request.move_copy_want_copy() {
            responses.extend_from_slice(&rop_error_response(
                0x33,
                request.response_handle_index(),
                0x8004_0102,
            ));
            return;
        }
        let Some(target_mailbox) = folder_row_for_id(target_folder_id, mailboxes) else {
            responses.extend_from_slice(&rop_error_response(
                0x33,
                request.response_handle_index(),
                0x8004_010F,
            ));
            return;
        };
        let mut partial_completion = false;
        for message_id in request.move_copy_message_ids() {
            let Some(item) = snapshot.recoverable_item_for_id(source_folder_id, message_id) else {
                partial_completion = true;
                continue;
            };
            if store
                .restore_recoverable_item(
                    principal.account_id,
                    item.canonical_id,
                    Some(target_mailbox.id),
                    AuditEntryInput {
                        actor: principal.email.clone(),
                        action: "mapi-restore-recoverable-message".to_string(),
                        subject: format!(
                            "recoverable:{}->{}",
                            item.canonical_id, target_mailbox.id
                        ),
                    },
                )
                .await
                .is_err()
            {
                partial_completion = true;
            }
        }
        responses.extend_from_slice(&rop_partial_completion_response(
            0x33,
            request.response_handle_index(),
            partial_completion,
        ));
        return;
    }
    if snapshot.public_folder_for_id(source_folder_id).is_some() {
        let Some(target_folder) = snapshot.public_folder_for_id(target_folder_id) else {
            responses.extend_from_slice(&rop_error_response(
                0x33,
                request.response_handle_index(),
                0x8004_010F,
            ));
            return;
        };
        let mut partial_completion = false;
        for message_id in request.move_copy_message_ids() {
            let Some(item) = snapshot.public_folder_item_for_id(source_folder_id, message_id)
            else {
                partial_completion = true;
                continue;
            };
            let copied = store
                .upsert_public_folder_item(
                    UpsertPublicFolderItemInput {
                        id: None,
                        account_id: principal.account_id,
                        public_folder_id: target_folder.folder.id,
                        item_kind: item.item.item_kind.clone(),
                        message_class: item.item.message_class.clone(),
                        subject: item.item.subject.clone(),
                        body_text: item.item.body_text.clone(),
                        body_html_sanitized: item.item.body_html_sanitized.clone(),
                        source_payload_json: item.item.source_payload_json.clone(),
                    },
                    AuditEntryInput {
                        actor: principal.email.clone(),
                        action: if request.move_copy_want_copy() {
                            "mapi-copy-public-folder-item".to_string()
                        } else {
                            "mapi-move-public-folder-item-copy".to_string()
                        },
                        subject: format!("{}->{}", item.item.id, target_folder.folder.id),
                    },
                )
                .await;
            let Ok(copied) = copied else {
                partial_completion = true;
                continue;
            };
            if remember_created_mapi_identity(
                store,
                principal,
                MapiIdentityObjectKind::PublicFolderItem,
                copied.id,
                None,
                None,
            )
            .await
            .is_err()
            {
                partial_completion = true;
                continue;
            }
            if !request.move_copy_want_copy()
                && store
                    .delete_public_folder_item(
                        principal.account_id,
                        item.item.public_folder_id,
                        item.item.id,
                        AuditEntryInput {
                            actor: principal.email.clone(),
                            action: "mapi-move-public-folder-item-delete".to_string(),
                            subject: item.item.id.to_string(),
                        },
                    )
                    .await
                    .is_err()
            {
                partial_completion = true;
            }
        }
        if !partial_completion {
            session.record_notification(MapiNotificationEvent::content(source_folder_id, None));
            session.record_notification(MapiNotificationEvent::content(target_folder_id, None));
        }
        responses.extend_from_slice(&rop_partial_completion_response(
            0x33,
            request.response_handle_index(),
            partial_completion,
        ));
        return;
    }
    let Some(target_mailbox) = folder_row_for_id(target_folder_id, mailboxes) else {
        responses.extend_from_slice(&rop_error_response(
            0x33,
            request.response_handle_index(),
            0x8004_010F,
        ));
        return;
    };
    let mut partial_completion = false;
    for message_id in request.move_copy_message_ids() {
        let Some(email) = message_for_id(source_folder_id, message_id, mailboxes, emails) else {
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

pub(super) async fn append_delete_messages_response<S>(
    store: &S,
    principal: &AccountPrincipal,
    session: &mut MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    responses: &mut Vec<u8>,
) where
    S: ExchangeStore,
{
    if request.delete_messages_want_asynchronous().is_none()
        || request.delete_messages_notify_non_read().is_none()
    {
        responses.extend_from_slice(&rop_error_response(
            request.rop_id,
            request.response_handle_index(),
            0x8007_0057,
        ));
        return;
    }
    let folder_id = match input_object(session, handle_slots, request) {
        Some(MapiObject::Folder { folder_id, .. }) => *folder_id,
        _ if request.rop_id == RopId::HardDeleteMessages.as_u8() => {
            responses.extend_from_slice(&unsupported_rop_response(
                request.rop_id,
                request.response_handle_index(),
            ));
            return;
        }
        _ => {
            responses.extend_from_slice(&rop_error_response(
                request.rop_id,
                request.response_handle_index(),
                0x0000_04B9,
            ));
            return;
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
        return;
    }
    if folder_id == crate::mapi::identity::RECOVERABLE_ITEMS_ROOT_FOLDER_ID {
        responses.extend_from_slice(&rop_error_response(
            request.rop_id,
            request.response_handle_index(),
            0x8004_0102,
        ));
        return;
    }
    for message_id in request.message_ids() {
        if crate::mapi_store::recoverable_storage_folder(folder_id).is_some() {
            if request.rop_id == RopId::DeleteMessages.as_u8() {
                partial_completion = true;
                continue;
            }
            let Some(item) = snapshot.recoverable_item_for_id(folder_id, message_id) else {
                partial_completion = true;
                continue;
            };
            if store
                .purge_recoverable_item(
                    principal.account_id,
                    item.canonical_id,
                    AuditEntryInput {
                        actor: principal.email.clone(),
                        action: "mapi-purge-recoverable-message".to_string(),
                        subject: format!("recoverable:{}", item.canonical_id),
                    },
                )
                .await
                .is_err()
            {
                partial_completion = true;
            }
            continue;
        }
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
        if !mapi_calendar_content_items_suppressed(folder_id, snapshot) {
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
            } else {
                record_sync_upload_content_checkpoint(session, folder_id);
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
            } else {
                record_sync_upload_content_checkpoint(session, folder_id);
            }
            continue;
        }
        if let Some(message) = snapshot
            .conversation_action_message_for_id(message_id)
            .filter(|message| message.folder_id == folder_id)
        {
            if store
                .delete_conversation_action(principal.account_id, message.canonical_id)
                .await
                .is_err()
            {
                partial_completion = true;
            }
            continue;
        }
        if folder_id == crate::mapi::identity::COMMON_VIEWS_FOLDER_ID {
            if let Some(message) = snapshot
                .navigation_shortcut_message_for_id(message_id)
                .filter(|message| message.folder_id == folder_id)
            {
                if store
                    .delete_mapi_navigation_shortcut(principal.account_id, message.canonical_id)
                    .await
                    .is_err()
                {
                    partial_completion = true;
                }
                continue;
            }
        }
        if let Some(message) = snapshot
            .associated_config_message_for_id(message_id)
            .filter(|message| message.folder_id == folder_id)
            .or_else(|| {
                snapshot
                    .associated_config_message_for_folder_and_source_key_id(folder_id, message_id)
            })
        {
            if store
                .delete_mapi_associated_config(principal.account_id, message.canonical_id)
                .await
                .is_err()
            {
                partial_completion = true;
            } else {
                record_sync_upload_content_checkpoint(session, folder_id);
            }
            continue;
        }
        if folder_local_default_named_view_is_supported(snapshot, folder_id, message_id) {
            continue;
        }
        if let Some(item) = snapshot.public_folder_item_for_id(folder_id, message_id) {
            if store
                .delete_public_folder_item(
                    principal.account_id,
                    item.item.public_folder_id,
                    item.item.id,
                    AuditEntryInput {
                        actor: principal.email.clone(),
                        action: "mapi-delete-public-folder-item".to_string(),
                        subject: format!("public-folder-item:{}", item.item.id),
                    },
                )
                .await
                .is_err()
            {
                partial_completion = true;
            }
            continue;
        }
        let Some(email) = message_for_id(folder_id, message_id, mailboxes, emails) else {
            partial_completion = true;
            continue;
        };
        let result = if request.rop_id == 0x91
            || email.mailbox_role == "trash"
            || mailbox_is_trash_or_descendant(email.mailbox_id, mailboxes)
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
        } else if let Some(trash_mailbox) = mailboxes.iter().find(|mailbox| mailbox.role == "trash")
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
        } else {
            record_sync_upload_content_checkpoint(session, folder_id);
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

pub(super) fn append_message_status_response(
    session: &mut MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    responses: &mut Vec<u8>,
) {
    let response_rop_id = RopId::SetMessageStatus.as_u8();
    let folder_id = match input_object(session, handle_slots, request) {
        Some(MapiObject::Folder { folder_id, .. }) => *folder_id,
        Some(_) | None => {
            responses.extend_from_slice(&rop_error_response(
                response_rop_id,
                request.response_handle_index(),
                0x0000_04B9,
            ));
            return;
        }
    };
    let message_id = request.status_message_id().unwrap_or(0);
    let item_exists = message_for_id(folder_id, message_id, mailboxes, emails)
        .or_else(|| {
            emails
                .iter()
                .find(|email| mapi_item_id_matches(&email.id, message_id))
        })
        .is_some()
        || snapshot
            .public_folder_item_for_id(folder_id, message_id)
            .is_some()
        || snapshot.contact_for_id(folder_id, message_id).is_some()
        || snapshot.event_for_id(folder_id, message_id).is_some()
        || snapshot.task_for_id(folder_id, message_id).is_some();
    if !item_exists {
        responses.extend_from_slice(&rop_error_response(
            response_rop_id,
            request.response_handle_index(),
            0x8004_010F,
        ));
        return;
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
    responses.extend_from_slice(&rop_message_status_response(request, old_status));
}
