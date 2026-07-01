use super::*;

pub(super) fn is_attachment_rop(rop_id: RopId) -> bool {
    matches!(
        rop_id,
        RopId::GetValidAttachments
            | RopId::GetAttachmentTable
            | RopId::OpenAttachment
            | RopId::CreateAttachment
            | RopId::DeleteAttachment
            | RopId::OpenEmbeddedMessage
            | RopId::SaveChangesAttachment
    )
}

pub(super) async fn append_attachment_response<S, V>(
    store: &S,
    principal: &AccountPrincipal,
    session: &mut MapiSession,
    handle_slots: &mut Vec<u32>,
    request: &RopRequest,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    validator: &Validator<V>,
    responses: &mut Vec<u8>,
    output_handles: &mut Vec<u32>,
) where
    S: ExchangeStore,
    V: Detector,
{
    match RopId::from_u8(request.rop_id) {
        Some(RopId::GetValidAttachments) => {
            append_get_valid_attachments_response(
                session,
                handle_slots,
                request,
                snapshot,
                responses,
            );
        }
        Some(RopId::GetAttachmentTable) => {
            append_get_attachment_table_response(
                session,
                handle_slots,
                request,
                snapshot,
                responses,
                output_handles,
            );
        }
        Some(RopId::OpenAttachment) => {
            append_open_attachment_response(
                session,
                handle_slots,
                request,
                snapshot,
                responses,
                output_handles,
            );
        }
        Some(RopId::CreateAttachment) => {
            append_create_attachment_response(
                principal,
                session,
                handle_slots,
                request,
                mailboxes,
                emails,
                snapshot,
                responses,
                output_handles,
            );
        }
        Some(RopId::DeleteAttachment) => {
            append_delete_attachment_response(
                principal,
                session,
                handle_slots,
                request,
                snapshot,
                responses,
            );
        }
        Some(RopId::OpenEmbeddedMessage) => {
            append_open_embedded_message_response(
                store,
                principal,
                session,
                handle_slots,
                request,
                snapshot,
                responses,
                output_handles,
            )
            .await;
        }
        Some(RopId::SaveChangesAttachment) => {
            append_save_changes_attachment_response(
                store,
                principal,
                session,
                handle_slots,
                request,
                mailboxes,
                emails,
                snapshot,
                validator,
                responses,
            )
            .await;
        }
        _ => unreachable!("append_attachment_response called for non-attachment ROP"),
    }
}

pub(super) fn append_get_valid_attachments_response(
    session: &MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    snapshot: &MapiMailStoreSnapshot,
    responses: &mut Vec<u8>,
) {
    responses.extend_from_slice(&rop_get_valid_attachments_response(
        request,
        input_object(session, handle_slots, request),
        snapshot,
        &session.pending_attachment_deletions,
    ))
}

pub(super) fn append_get_attachment_table_response(
    session: &mut MapiSession,
    handle_slots: &mut Vec<u32>,
    request: &RopRequest,
    snapshot: &MapiMailStoreSnapshot,
    responses: &mut Vec<u8>,
    output_handles: &mut Vec<u32>,
) {
    if !get_attachment_table_flags_are_valid(request) {
        responses.extend_from_slice(&rop_error_response(
            0x21,
            request.output_handle_index.unwrap_or(0),
            0x8007_0057,
        ));
        return;
    }
    let (folder_id, message_id, is_calendar_event) =
        match input_object(session, handle_slots, request) {
            Some(MapiObject::PendingMessage { folder_id, .. }) => (*folder_id, 0, false),
            Some(MapiObject::Message {
                folder_id,
                message_id,
                ..
            }) => (*folder_id, *message_id, false),
            Some(MapiObject::Event {
                folder_id,
                event_id: message_id,
            }) => (*folder_id, *message_id, true),
            _ => {
                responses.extend_from_slice(&rop_error_response(
                    0x21,
                    request.output_handle_index.unwrap_or(0),
                    0x8004_010F,
                ));
                return;
            }
        };
    if is_calendar_event && snapshot.event_for_id(folder_id, message_id).is_none() {
        responses.extend_from_slice(&rop_error_response(
            0x21,
            request.output_handle_index.unwrap_or(0),
            0x8004_010F,
        ));
        return;
    }
    let handle = session.allocate_output_handle(
        request.output_handle_index,
        attachment_table_object(folder_id, message_id),
    );
    set_handle_slot(handle_slots, request.output_handle_index, handle);
    responses.extend_from_slice(&get_attachment_table_response(request));
    output_handles.push(handle);
}

pub(super) fn append_open_attachment_response(
    session: &mut MapiSession,
    handle_slots: &mut Vec<u32>,
    request: &RopRequest,
    snapshot: &MapiMailStoreSnapshot,
    responses: &mut Vec<u8>,
    output_handles: &mut Vec<u32>,
) {
    if !open_attachment_flags_are_valid(request) {
        responses.extend_from_slice(&rop_error_response(
            0x22,
            request.output_handle_index.unwrap_or(0),
            0x8007_0057,
        ));
        return;
    }
    let (folder_id, message_id, is_calendar_event) =
        match input_object(session, handle_slots, request) {
            Some(MapiObject::Message {
                folder_id,
                message_id,
                ..
            }) => (*folder_id, *message_id, false),
            Some(MapiObject::Event {
                folder_id,
                event_id: message_id,
            }) => (*folder_id, *message_id, true),
            _ => {
                responses.extend_from_slice(&rop_error_response(
                    0x22,
                    request.output_handle_index.unwrap_or(0),
                    0x8004_010F,
                ));
                return;
            }
        };
    if is_calendar_event && snapshot.event_for_id(folder_id, message_id).is_none() {
        responses.extend_from_slice(&rop_error_response(
            0x22,
            request.output_handle_index.unwrap_or(0),
            0x8004_010F,
        ));
        return;
    }
    let attach_num = request.attach_num().unwrap_or(u32::MAX);
    if session
        .pending_attachment_deletions
        .contains(&(folder_id, message_id, attach_num))
    {
        responses.extend_from_slice(&rop_error_response(
            0x22,
            request.output_handle_index.unwrap_or(0),
            0x8004_010F,
        ));
        return;
    }
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
        set_handle_slot(handle_slots, request.output_handle_index, handle);
        responses.extend_from_slice(&rop_open_attachment_response(request));
        output_handles.push(handle);
    } else {
        responses.extend_from_slice(&rop_error_response(
            0x22,
            request.output_handle_index.unwrap_or(0),
            0x8004_010F,
        ));
    }
}

pub(super) fn append_create_attachment_response(
    principal: &AccountPrincipal,
    session: &mut MapiSession,
    handle_slots: &mut Vec<u32>,
    request: &RopRequest,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    responses: &mut Vec<u8>,
    output_handles: &mut Vec<u32>,
) {
    let parent_message_handle = input_handle(handle_slots, request).filter(|handle| {
        matches!(
            session.handles.get(handle),
            Some(MapiObject::PendingMessage { .. })
        )
    });
    let (folder_id, message_id, is_calendar_event, is_pending_message) =
        match input_object(session, handle_slots, request) {
            Some(MapiObject::Event { folder_id, .. })
                if mapi_calendar_content_items_suppressed(*folder_id, snapshot) =>
            {
                responses.extend_from_slice(&rop_error_response(
                    0x23,
                    request.output_handle_index.unwrap_or(0),
                    0x8004_010F,
                ));
                return;
            }
            Some(MapiObject::Message {
                folder_id,
                message_id,
                ..
            }) => (*folder_id, *message_id, false, false),
            Some(MapiObject::PendingMessage { folder_id, .. }) => (*folder_id, 0, false, true),
            Some(MapiObject::Event {
                folder_id,
                event_id,
            }) => (*folder_id, *event_id, true, false),
            _ => {
                responses.extend_from_slice(&rop_error_response(
                    0x23,
                    request.output_handle_index.unwrap_or(0),
                    0x0000_04B9,
                ));
                return;
            }
        };
    if !is_calendar_event
        && !is_pending_message
        && message_for_id(folder_id, message_id, mailboxes, emails).is_none()
    {
        responses.extend_from_slice(&rop_error_response(
            0x23,
            request.output_handle_index.unwrap_or(0),
            0x8004_010F,
        ));
        return;
    }
    if is_calendar_event && snapshot.event_for_id(folder_id, message_id).is_none() {
        responses.extend_from_slice(&rop_error_response(
            0x23,
            request.output_handle_index.unwrap_or(0),
            0x8004_010F,
        ));
        return;
    }
    if !snapshot
        .folder_access_for_principal(folder_id, principal.account_id)
        .map(|access| access.may_write)
        .unwrap_or(true)
    {
        responses.extend_from_slice(&rop_error_response(
            0x23,
            request.output_handle_index.unwrap_or(0),
            0x8007_0005,
        ));
        return;
    }

    let attach_num = if let Some(parent_handle) = parent_message_handle {
        session
            .pending_message_attachments
            .get(&parent_handle)
            .and_then(|attachments| attachments.iter().map(|(attach_num, _)| *attach_num).max())
            .unwrap_or(u32::MAX)
            .saturating_add(1)
    } else {
        next_pending_attachment_num(session, folder_id, message_id, snapshot)
    };
    let created_at = current_windows_filetime();
    let handle = session.allocate_output_handle(
        request.output_handle_index,
        MapiObject::PendingAttachment {
            folder_id,
            message_id,
            attach_num,
            properties: HashMap::from([
                (PID_TAG_ATTACH_SIZE, MapiValue::U32(0)),
                (PID_TAG_ACCESS_LEVEL, MapiValue::U32(0)),
                (PID_TAG_CREATION_TIME, MapiValue::U64(created_at)),
                (PID_TAG_LAST_MODIFICATION_TIME, MapiValue::U64(created_at)),
            ]),
            data: Vec::new(),
        },
    );
    if let Some(parent_handle) = parent_message_handle {
        session
            .pending_attachment_parent_messages
            .insert(handle, parent_handle);
    }
    set_handle_slot(handle_slots, request.output_handle_index, handle);
    responses.extend_from_slice(&rop_create_attachment_response(request, attach_num));
    output_handles.push(handle);
}

pub(super) fn append_delete_attachment_response(
    principal: &AccountPrincipal,
    session: &mut MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    snapshot: &MapiMailStoreSnapshot,
    responses: &mut Vec<u8>,
) {
    let (folder_id, message_id, is_calendar_event) =
        match input_object(session, handle_slots, request) {
            Some(MapiObject::Event { folder_id, .. })
                if mapi_calendar_content_items_suppressed(*folder_id, snapshot) =>
            {
                responses.extend_from_slice(&rop_error_response(
                    0x24,
                    request.response_handle_index(),
                    0x8004_010F,
                ));
                return;
            }
            Some(MapiObject::Message {
                folder_id,
                message_id,
                ..
            }) => (*folder_id, *message_id, false),
            Some(MapiObject::Event {
                folder_id,
                event_id,
            }) => (*folder_id, *event_id, true),
            _ => {
                responses.extend_from_slice(&rop_error_response(
                    0x24,
                    request.response_handle_index(),
                    0x0000_04B9,
                ));
                return;
            }
        };
    let attach_num = request.attach_num().unwrap_or(u32::MAX);
    let Some(attachment) = snapshot.attachment_for_message(folder_id, message_id, attach_num)
    else {
        responses.extend_from_slice(&rop_error_response(
            0x24,
            request.response_handle_index(),
            0x8004_010F,
        ));
        return;
    };
    if !snapshot
        .folder_access_for_principal(folder_id, principal.account_id)
        .map(|access| access.may_write)
        .unwrap_or(true)
    {
        responses.extend_from_slice(&rop_error_response(
            0x24,
            request.response_handle_index(),
            0x8007_0005,
        ));
        return;
    }
    let _ = is_calendar_event;
    let _ = attachment;
    session
        .pending_attachment_deletions
        .insert((folder_id, message_id, attach_num));
    responses.extend_from_slice(&rop_simple_success_response(request));
}

pub(super) async fn append_open_embedded_message_response<S>(
    store: &S,
    principal: &AccountPrincipal,
    session: &mut MapiSession,
    handle_slots: &mut Vec<u32>,
    request: &RopRequest,
    snapshot: &MapiMailStoreSnapshot,
    responses: &mut Vec<u8>,
    output_handles: &mut Vec<u32>,
) where
    S: ExchangeStore,
{
    let Some(handle) = input_handle(handle_slots, request) else {
        responses.extend_from_slice(&rop_error_response(
            0x46,
            request.response_handle_index(),
            0x0000_04B9,
        ));
        return;
    };
    let open_mode = request.payload.get(2).copied().unwrap_or(0);
    if open_mode > 0x02 {
        responses.extend_from_slice(&rop_error_response(
            0x46,
            request.response_handle_index(),
            0x8007_0057,
        ));
        return;
    }
    let Some((folder_id, message_id, attach_num, embedded_properties)) =
        open_embedded_message_source(store, principal, session, snapshot, handle, open_mode).await
    else {
        responses.extend_from_slice(&rop_error_response(
            0x46,
            request.response_handle_index(),
            if open_mode == 0 {
                0x8004_010F
            } else {
                0x8007_0005
            },
        ));
        return;
    };
    let embedded_message_id = transient_embedded_message_id(folder_id, message_id, attach_num);
    let embedded_subject = embedded_message_open_subject(&embedded_properties);
    let embedded_handle = session.allocate_output_handle(
        request.output_handle_index,
        MapiObject::PendingMessage {
            folder_id,
            properties: embedded_properties,
            recipients: Vec::new(),
        },
    );
    session
        .pending_embedded_message_ids
        .insert(embedded_handle, embedded_message_id);
    session
        .pending_embedded_message_attachments
        .insert(embedded_handle, (folder_id, message_id, attach_num));
    set_handle_slot(handle_slots, request.output_handle_index, embedded_handle);
    responses.extend_from_slice(&rop_open_embedded_message_response(
        request,
        embedded_message_id,
        &embedded_subject,
        0,
    ));
    output_handles.push(embedded_handle);
}

pub(super) async fn append_save_changes_attachment_response<S, V>(
    store: &S,
    principal: &AccountPrincipal,
    session: &mut MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    validator: &Validator<V>,
    responses: &mut Vec<u8>,
) where
    S: ExchangeStore,
    V: Detector,
{
    let Some(handle) = input_handle(handle_slots, request) else {
        responses.extend_from_slice(&rop_error_response(
            0x25,
            request.response_handle_index(),
            0x8004_010F,
        ));
        return;
    };
    if !save_flags_are_supported(request) {
        responses.extend_from_slice(&rop_error_response(
            0x25,
            request.response_handle_index(),
            0x8004_0102,
        ));
        return;
    }
    let save_attachment_object = session.handles.get(&handle).cloned();
    session.record_recent_probe_action(format!(
        "SaveChangesAttachment(in={},handle={},kind={},folder={})",
        request.input_handle_index().unwrap_or(0),
        handle,
        mapi_object_debug_kind(save_attachment_object.as_ref()),
        mapi_object_debug_folder_id(save_attachment_object.as_ref())
    ));
    tracing::info!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = "emsmdb",
        mailbox = %principal.email,
        request_type = "Execute",
        request_rop_id = "0x25",
        input_handle_index = request.input_handle_index().unwrap_or(0),
        input_handle_value = handle,
        object_kind = mapi_object_debug_kind(save_attachment_object.as_ref()),
        folder_id = %mapi_object_debug_folder_id(save_attachment_object.as_ref()),
        "rca debug mapi save changes before inbox probe"
    );
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
        return;
    };
    if !snapshot
        .folder_access_for_principal(folder_id, principal.account_id)
        .map(|access| access.may_write)
        .unwrap_or(true)
    {
        responses.extend_from_slice(&rop_error_response(
            0x25,
            request.response_handle_index(),
            0x8007_0005,
        ));
        return;
    }
    let mut attachment = pending_attachment_upload(attach_num, &properties, data);
    let attach_method = properties
        .get(&PID_TAG_ATTACH_METHOD)
        .and_then(MapiValue::as_i64)
        .unwrap_or(1);
    let mut generated_embedded_attachment = false;
    if attach_method == 5 {
        if let Some(embedded_properties) = session
            .saved_embedded_messages
            .get(&(folder_id, message_id, attach_num))
        {
            attachment = pending_embedded_message_attachment_upload(
                attach_num,
                &properties,
                embedded_properties,
            );
            generated_embedded_attachment = true;
        }
    }
    let mut attachment = attachment;
    if !generated_embedded_attachment {
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
            return;
        };
        if outcome.policy_decision != PolicyDecision::Accept {
            responses.extend_from_slice(&rop_error_response(
                0x25,
                request.response_handle_index(),
                0x8004_0102,
            ));
            return;
        }
        if attachment.media_type == "application/octet-stream"
            && !outcome.detected_mime.trim().is_empty()
        {
            attachment.media_type = outcome.detected_mime;
        }
    }
    if let Some(parent_handle) = session
        .pending_attachment_parent_messages
        .get(&handle)
        .copied()
    {
        session
            .pending_message_attachments
            .entry(parent_handle)
            .or_default()
            .retain(|(existing_attach_num, _)| *existing_attach_num != attach_num);
        session
            .pending_message_attachments
            .entry(parent_handle)
            .or_default()
            .push((attach_num, attachment.clone()));
        session.handles.insert(
            handle,
            MapiObject::SavedAttachment {
                folder_id,
                message_id,
                attach_num,
                file_reference: format!("pending-message:{parent_handle}:{attach_num}"),
                file_name: attachment.file_name,
                media_type: attachment.media_type,
                disposition: attachment.disposition,
                content_id: attachment.content_id,
                size_octets: attachment.blob_bytes.len() as u64,
            },
        );
        responses.extend_from_slice(&rop_simple_success_response(request));
        return;
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
                if upsert_custom_property_values_from_map(
                    store,
                    principal,
                    MapiCustomPropertyObjectKind::Attachment,
                    stored.id,
                    &properties,
                )
                .await
                .is_err()
                {
                    responses.extend_from_slice(&rop_error_response(
                        0x25,
                        request.response_handle_index(),
                        0x8004_010F,
                    ));
                    return;
                }
                session.handles.insert(
                    handle,
                    MapiObject::SavedAttachment {
                        folder_id,
                        message_id,
                        attach_num,
                        file_reference: stored.file_reference,
                        file_name: stored.file_name,
                        media_type: stored.media_type,
                        disposition: stored.disposition,
                        content_id: stored.content_id,
                        size_octets: stored.size_octets,
                    },
                );
                responses.extend_from_slice(&rop_simple_success_response(request));
            }
            _ => responses.extend_from_slice(&rop_error_response(
                0x25,
                request.response_handle_index(),
                0x8004_010F,
            )),
        }
    } else if !mapi_calendar_content_items_suppressed(folder_id, snapshot) {
        if let Some(event) = snapshot.event_for_id(folder_id, message_id) {
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
                    if upsert_custom_property_values_from_map(
                        store,
                        principal,
                        MapiCustomPropertyObjectKind::Attachment,
                        stored.id,
                        &properties,
                    )
                    .await
                    .is_err()
                    {
                        responses.extend_from_slice(&rop_error_response(
                            0x25,
                            request.response_handle_index(),
                            0x8004_010F,
                        ));
                        return;
                    }
                    session.handles.insert(
                        handle,
                        MapiObject::SavedAttachment {
                            folder_id,
                            message_id,
                            attach_num,
                            file_reference: stored.file_reference,
                            file_name: stored.file_name,
                            media_type: stored.media_type,
                            disposition: None,
                            content_id: None,
                            size_octets: stored.size_octets,
                        },
                    );
                    responses.extend_from_slice(&rop_simple_success_response(request));
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
    } else {
        responses.extend_from_slice(&rop_error_response(
            0x25,
            request.response_handle_index(),
            0x8004_010F,
        ));
    }
}

pub(super) async fn mapi_submit_attachments_from_email<S>(
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
            disposition: attachment.disposition,
            content_id: attachment.content_id,
            blob_bytes: content.blob_bytes,
        });
    }
    Ok(uploads)
}

pub(super) async fn sync_attachment_facts_for_with_embedded_content<S: ExchangeStore>(
    store: &S,
    account_id: Uuid,
    folder_id: u64,
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
) -> Vec<mapi_mailstore::MessageAttachmentSyncFacts> {
    let mut facts = sync_attachment_facts_for(folder_id, emails, snapshot);
    for message_facts in &mut facts {
        for attachment in &mut message_facts.attachments {
            if !mapi_mailstore::attachment_sync_fact_is_embedded_message(attachment) {
                continue;
            }
            if let Ok(Some(content)) = store
                .fetch_attachment_content(account_id, &attachment.file_reference)
                .await
            {
                attachment.embedded_message_blob = Some(content.blob_bytes);
            }
        }
    }
    facts
}

pub(super) fn transient_embedded_message_id(
    folder_id: u64,
    message_id: u64,
    attach_num: u32,
) -> u64 {
    let folder_counter =
        crate::mapi::identity::global_counter_from_store_id(folder_id).unwrap_or(1);
    let message_counter =
        crate::mapi::identity::global_counter_from_store_id(message_id).unwrap_or(1);
    crate::mapi::identity::mapi_store_id(
        crate::mapi::identity::MAX_PERSISTED_GLOBAL_COUNTER
            .saturating_add(folder_counter)
            .saturating_add(message_counter)
            .saturating_add(u64::from(attach_num))
            .saturating_add(1),
    )
}

pub(super) fn embedded_message_open_subject(properties: &HashMap<u32, MapiValue>) -> String {
    optional_pending_text_property(
        properties,
        &[PID_TAG_NORMALIZED_SUBJECT_W, PID_TAG_SUBJECT_W],
    )
    .unwrap_or_default()
}

pub(super) async fn open_embedded_message_source<S: ExchangeStore>(
    store: &S,
    principal: &AccountPrincipal,
    session: &MapiSession,
    snapshot: &MapiMailStoreSnapshot,
    handle: u32,
    open_mode: u8,
) -> Option<(u64, u64, u32, HashMap<u32, MapiValue>)> {
    match session.handles.get(&handle)?.clone() {
        MapiObject::PendingAttachment {
            folder_id,
            message_id,
            attach_num,
            properties,
            ..
        } => {
            let attach_method = properties
                .get(&PID_TAG_ATTACH_METHOD)
                .and_then(MapiValue::as_i64)
                .unwrap_or(i64::from(ATTACH_EMBEDDED_MESSAGE));
            if attach_method != i64::from(ATTACH_EMBEDDED_MESSAGE) {
                return None;
            }
            Some((
                folder_id,
                message_id,
                attach_num,
                default_embedded_message_properties(),
            ))
        }
        MapiObject::Attachment {
            folder_id,
            message_id,
            attach_num,
        } => {
            if open_mode != 0 {
                return None;
            }
            let attachment = snapshot.attachment_for_message(folder_id, message_id, attach_num)?;
            if !attachment_is_embedded_message(&attachment) {
                return None;
            }
            let properties =
                embedded_message_properties_from_attachment(store, principal, &attachment).await;
            Some((folder_id, message_id, attach_num, properties))
        }
        MapiObject::SavedAttachment {
            folder_id,
            message_id,
            attach_num,
            file_reference,
            file_name,
            media_type,
            ..
        } => {
            if open_mode != 0 || !attachment_metadata_is_embedded_message(&media_type, &file_name) {
                return None;
            }
            let properties = embedded_message_properties_from_attachment_metadata(
                store,
                principal,
                &file_reference,
                &file_name,
            )
            .await;
            Some((folder_id, message_id, attach_num, properties))
        }
        _ => None,
    }
}

async fn embedded_message_properties_from_attachment<S: ExchangeStore>(
    store: &S,
    principal: &AccountPrincipal,
    attachment: &crate::mapi_store::MapiAttachment,
) -> HashMap<u32, MapiValue> {
    embedded_message_properties_from_attachment_metadata(
        store,
        principal,
        &attachment.file_reference,
        &attachment.file_name,
    )
    .await
}

async fn embedded_message_properties_from_attachment_metadata<S: ExchangeStore>(
    store: &S,
    principal: &AccountPrincipal,
    file_reference: &str,
    file_name: &str,
) -> HashMap<u32, MapiValue> {
    let content = store
        .fetch_attachment_content(principal.account_id, file_reference)
        .await
        .ok()
        .flatten()
        .map(|content| content.blob_bytes)
        .unwrap_or_default();
    embedded_message_properties_from_blob(file_name, &content)
}

fn default_embedded_message_properties() -> HashMap<u32, MapiValue> {
    HashMap::from([(
        PID_TAG_MESSAGE_CLASS_W,
        MapiValue::String("IPM.Note".to_string()),
    )])
}

fn embedded_message_properties_from_blob(file_name: &str, blob: &[u8]) -> HashMap<u32, MapiValue> {
    let mut properties = default_embedded_message_properties();
    let text = String::from_utf8_lossy(blob);
    if let Some(subject) = text
        .split_once("Subject:")
        .and_then(|(_, rest)| rest.split_once("\r\n").map(|(value, _)| value))
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        properties.insert(PID_TAG_SUBJECT_W, MapiValue::String(subject.to_string()));
    } else if let Some(subject) = file_name
        .trim()
        .strip_suffix(".msg")
        .filter(|value| !value.is_empty())
    {
        properties.insert(PID_TAG_SUBJECT_W, MapiValue::String(subject.to_string()));
    }
    if let Some(body_text) = text
        .split_once("Body-Length:")
        .and_then(|(_, rest)| rest.split_once("\r\n").map(|(_, body)| body))
        .map(|body| {
            body.split_once("\r\nHtml-Length:")
                .map(|(value, _)| value)
                .unwrap_or(body)
        })
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        properties.insert(PID_TAG_BODY_W, MapiValue::String(body_text.to_string()));
    }
    properties
}

pub(super) fn pending_embedded_message_attachment_upload(
    attach_num: u32,
    attachment_properties: &HashMap<u32, MapiValue>,
    embedded_properties: &HashMap<u32, MapiValue>,
) -> AttachmentUploadInput {
    let subject = optional_pending_text_property(
        embedded_properties,
        &[PID_TAG_SUBJECT_W, PID_TAG_NORMALIZED_SUBJECT_W],
    )
    .unwrap_or_else(|| "Embedded message".to_string());
    let body =
        optional_pending_text_property(embedded_properties, &[PID_TAG_BODY_W]).unwrap_or_default();
    let body_html = optional_pending_text_property(embedded_properties, &[PID_TAG_BODY_HTML_W])
        .unwrap_or_default();
    let file_name = optional_pending_text_property(
        attachment_properties,
        &[PID_TAG_ATTACH_LONG_FILENAME_W, PID_TAG_ATTACH_FILENAME_W],
    )
    .unwrap_or_else(|| format!("{subject}.msg"));
    let mut payload = Vec::new();
    payload.extend_from_slice(b"LPE-MAPI-EMBEDDED-MESSAGE\0");
    payload.extend_from_slice(format!("Subject:{subject}\r\n").as_bytes());
    payload.extend_from_slice(format!("Body-Length:{}\r\n", body.len()).as_bytes());
    payload.extend_from_slice(body.as_bytes());
    payload.extend_from_slice(b"\r\nHtml-Length:");
    payload.extend_from_slice(body_html.len().to_string().as_bytes());
    payload.extend_from_slice(b"\r\n");
    payload.extend_from_slice(body_html.as_bytes());

    AttachmentUploadInput {
        file_name,
        media_type: "application/vnd.ms-outlook".to_string(),
        disposition: Some("attachment".to_string()),
        content_id: None,
        blob_bytes: if payload.is_empty() {
            format!("Embedded message {attach_num}").into_bytes()
        } else {
            payload
        },
    }
}
