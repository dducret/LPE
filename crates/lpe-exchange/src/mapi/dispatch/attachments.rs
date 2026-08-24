use super::*;
use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};

mod save_contract;

use save_contract::save_attachment_parent_handle;

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
    let parent_handle = input_handle(handle_slots, request);
    if let (Some(parent_handle), Some(object)) =
        (parent_handle, input_object(session, handle_slots, request))
    {
        let event = match object {
            MapiObject::Event {
                folder_id,
                event_id,
                ..
            } => Some((*folder_id, *event_id, false)),
            MapiObject::PendingEvent { folder_id, .. } => Some((*folder_id, 0, true)),
            _ => None,
        };
        if let Some((folder_id, message_id, is_pending)) = event {
            if !is_pending && snapshot.event_for_id(folder_id, message_id).is_none() {
                responses.extend_from_slice(&rop_error_response(
                    0x52,
                    request.response_handle_index(),
                    0x8004_010F,
                ));
                return;
            }
            let attach_nums = event_attachments_for_parent_handle(
                session,
                parent_handle,
                folder_id,
                message_id,
                snapshot,
            )
            .into_iter()
            .map(|attachment| attachment.attach_num)
            .collect::<Vec<_>>();
            responses.extend_from_slice(&rop_get_valid_attachment_numbers_response(
                request,
                &attach_nums,
            ));
            return;
        }
    }
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
    let parent_handle = input_handle(handle_slots, request);
    let (folder_id, message_id, is_calendar_event, is_pending_event, is_contact) =
        match input_object(session, handle_slots, request) {
            Some(MapiObject::PendingMessage { folder_id, .. }) => {
                (*folder_id, 0, false, false, false)
            }
            Some(MapiObject::Message {
                folder_id,
                message_id,
                ..
            }) => (*folder_id, *message_id, false, false, false),
            Some(MapiObject::Event {
                folder_id,
                event_id: message_id,
                ..
            }) => (*folder_id, *message_id, true, false, false),
            Some(MapiObject::PendingEvent { folder_id, .. }) => (*folder_id, 0, true, true, false),
            Some(MapiObject::Contact {
                folder_id,
                contact_id,
                ..
            }) => (*folder_id, *contact_id, false, false, true),
            Some(MapiObject::PendingContact { folder_id, .. }) => {
                (*folder_id, 0, false, false, true)
            }
            _ => {
                responses.extend_from_slice(&rop_error_response(
                    0x21,
                    request.output_handle_index.unwrap_or(0),
                    0x8004_010F,
                ));
                return;
            }
        };
    if is_calendar_event
        && !is_pending_event
        && snapshot.event_for_id(folder_id, message_id).is_none()
    {
        responses.extend_from_slice(&rop_error_response(
            0x21,
            request.output_handle_index.unwrap_or(0),
            0x8004_010F,
        ));
        return;
    }
    let mut table = attachment_table_object(folder_id, message_id);
    if is_calendar_event {
        if let (
            Some(parent_handle),
            MapiObject::AttachmentTable {
                materialized_attachments,
                ..
            },
        ) = (parent_handle, &mut table)
        {
            *materialized_attachments = Some(event_attachments_for_parent_handle(
                session,
                parent_handle,
                folder_id,
                message_id,
                snapshot,
            ));
        }
    }
    if is_contact {
        if let MapiObject::AttachmentTable {
            materialized_attachments,
            ..
        } = &mut table
        {
            *materialized_attachments = Some(
                snapshot
                    .contact_for_id(folder_id, message_id)
                    .and_then(contact_photo_attachment)
                    .into_iter()
                    .collect::<Vec<_>>(),
            );
        }
    }
    let handle = session.allocate_output_handle(request.output_handle_index, table);
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
    let parent_handle = input_handle(handle_slots, request);
    let (folder_id, message_id, is_calendar_event, is_pending_event, is_contact) =
        match input_object(session, handle_slots, request) {
            Some(MapiObject::Message {
                folder_id,
                message_id,
                ..
            }) => (*folder_id, *message_id, false, false, false),
            Some(MapiObject::Event {
                folder_id,
                event_id: message_id,
                ..
            }) => (*folder_id, *message_id, true, false, false),
            Some(MapiObject::PendingEvent { folder_id, .. }) => (*folder_id, 0, true, true, false),
            Some(MapiObject::Contact {
                folder_id,
                contact_id,
                ..
            }) => (*folder_id, *contact_id, false, false, true),
            _ => {
                responses.extend_from_slice(&rop_error_response(
                    0x22,
                    request.output_handle_index.unwrap_or(0),
                    0x8004_010F,
                ));
                return;
            }
        };
    if is_calendar_event
        && !is_pending_event
        && snapshot.event_for_id(folder_id, message_id).is_none()
    {
        responses.extend_from_slice(&rop_error_response(
            0x22,
            request.output_handle_index.unwrap_or(0),
            0x8004_010F,
        ));
        return;
    }
    let attach_num = request.attach_num().unwrap_or(u32::MAX);
    if is_contact {
        let Some(contact) = snapshot.contact_for_id(folder_id, message_id) else {
            responses.extend_from_slice(&rop_error_response(
                0x22,
                request.output_handle_index.unwrap_or(0),
                0x8004_010F,
            ));
            return;
        };
        let Some(object) = contact_photo_attachment_object(folder_id, message_id, contact) else {
            responses.extend_from_slice(&rop_error_response(
                0x22,
                request.output_handle_index.unwrap_or(0),
                0x8004_010F,
            ));
            return;
        };
        if attach_num != 0 {
            responses.extend_from_slice(&rop_error_response(
                0x22,
                request.output_handle_index.unwrap_or(0),
                0x8004_010F,
            ));
            return;
        }
        let handle = session.allocate_output_handle(request.output_handle_index, object);
        set_handle_slot(handle_slots, request.output_handle_index, handle);
        responses.extend_from_slice(&rop_open_attachment_response(request));
        output_handles.push(handle);
        return;
    }
    if is_calendar_event {
        let Some(parent_handle) = parent_handle else {
            responses.extend_from_slice(&rop_error_response(
                0x22,
                request.output_handle_index.unwrap_or(0),
                0x8004_010F,
            ));
            return;
        };
        let visible_attachment = event_attachments_for_parent_handle(
            session,
            parent_handle,
            folder_id,
            message_id,
            snapshot,
        )
        .into_iter()
        .find(|attachment| attachment.attach_num == attach_num);
        let Some(attachment) = visible_attachment else {
            responses.extend_from_slice(&rop_error_response(
                0x22,
                request.output_handle_index.unwrap_or(0),
                0x8004_010F,
            ));
            return;
        };
        let object = if attachment.canonical_id.is_nil() {
            let Some(upsert) = session
                .pending_event_attachment_transactions
                .get(&parent_handle)
                .and_then(|changes| {
                    changes
                        .upserts
                        .iter()
                        .find(|upsert| upsert.attach_num == attach_num)
                })
            else {
                responses.extend_from_slice(&rop_error_response(
                    0x22,
                    request.output_handle_index.unwrap_or(0),
                    0x8004_010F,
                ));
                return;
            };
            pending_event_attachment_object(folder_id, message_id, upsert)
        } else {
            MapiObject::Attachment {
                folder_id,
                message_id,
                attach_num,
            }
        };
        let handle = session.allocate_output_handle(request.output_handle_index, object);
        set_handle_slot(handle_slots, request.output_handle_index, handle);
        responses.extend_from_slice(&rop_open_attachment_response(request));
        output_handles.push(handle);
        return;
    }
    if session
        .pending_attachment_deletions
        .contains_key(&(folder_id, message_id, attach_num))
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
    let parent_handle = input_handle(handle_slots, request);
    let parent_message_handle = parent_handle.filter(|handle| {
        matches!(
            session.handles.get(handle),
            Some(MapiObject::Message { .. } | MapiObject::PendingMessage { .. })
        )
    });
    let parent_event_handle = parent_handle.filter(|handle| {
        matches!(
            session.handles.get(handle),
            Some(MapiObject::Event { .. } | MapiObject::PendingEvent { .. })
        )
    });
    let parent_contact_handle = parent_handle.filter(|handle| {
        matches!(
            session.handles.get(handle),
            Some(MapiObject::Contact { .. } | MapiObject::PendingContact { .. })
        )
    });
    let (
        folder_id,
        message_id,
        is_calendar_event,
        is_pending_message,
        is_pending_event,
        is_contact,
    ) = match input_object(session, handle_slots, request) {
        Some(MapiObject::Message {
            folder_id,
            message_id,
            ..
        }) => (*folder_id, *message_id, false, false, false, false),
        Some(MapiObject::PendingMessage { folder_id, .. }) => {
            (*folder_id, 0, false, true, false, false)
        }
        Some(MapiObject::Event {
            folder_id,
            event_id,
            ..
        }) => (*folder_id, *event_id, true, false, false, false),
        Some(MapiObject::PendingEvent { folder_id, .. }) => {
            (*folder_id, 0, true, false, true, false)
        }
        Some(MapiObject::Contact {
            folder_id,
            contact_id,
            ..
        }) => (*folder_id, *contact_id, false, false, false, true),
        Some(MapiObject::PendingContact { folder_id, .. }) => {
            (*folder_id, 0, false, false, false, true)
        }
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
        && !is_contact
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
    if is_calendar_event
        && !is_pending_event
        && snapshot.event_for_id(folder_id, message_id).is_none()
    {
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
    if let Some(MapiObject::Event { transaction, .. }) =
        parent_event_handle.and_then(|handle| session.handles.get(&handle))
    {
        let may_write = snapshot
            .event_for_id(folder_id, message_id)
            .map(|event| event.event.rights.may_write)
            .unwrap_or(false);
        if !event_handle_is_writable(transaction.open_mode_flags, may_write) {
            responses.extend_from_slice(&rop_error_response(
                0x23,
                request.output_handle_index.unwrap_or(0),
                0x8007_0005,
            ));
            return;
        }
    }

    let attach_num = if is_contact {
        0
    } else if is_pending_message {
        let parent_handle = parent_message_handle.expect("pending Message has a parent handle");
        session
            .pending_message_attachments
            .get(&parent_handle)
            .and_then(|attachments| attachments.iter().map(|(attach_num, _)| *attach_num).max())
            .unwrap_or(u32::MAX)
            .saturating_add(1)
    } else if let Some(parent_handle) = parent_event_handle {
        next_pending_event_attachment_num(session, parent_handle, folder_id, message_id, snapshot)
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
    if let Some(parent_handle) = parent_message_handle
        .or(parent_event_handle)
        .or(parent_contact_handle)
    {
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
    let parent_handle = input_handle(handle_slots, request);
    let (folder_id, message_id, is_calendar_event) =
        match input_object(session, handle_slots, request) {
            Some(MapiObject::Message {
                folder_id,
                message_id,
                ..
            }) => (*folder_id, *message_id, false),
            Some(MapiObject::Event {
                folder_id,
                event_id,
                ..
            }) => (*folder_id, *event_id, true),
            Some(MapiObject::PendingEvent { folder_id, .. }) => (*folder_id, 0, true),
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
    if is_calendar_event {
        let Some(parent_handle) = parent_handle else {
            responses.extend_from_slice(&rop_error_response(
                0x24,
                request.response_handle_index(),
                0x8004_010F,
            ));
            return;
        };
        if let Some(MapiObject::Event { transaction, .. }) = session.handles.get(&parent_handle) {
            let may_write = snapshot
                .event_for_id(folder_id, message_id)
                .map(|event| event.event.rights.may_write)
                .unwrap_or(false);
            if !event_handle_is_writable(transaction.open_mode_flags, may_write) {
                responses.extend_from_slice(&rop_error_response(
                    0x24,
                    request.response_handle_index(),
                    0x8007_0005,
                ));
                return;
            }
        }
        let Some(attachment) = event_attachments_for_parent_handle(
            session,
            parent_handle,
            folder_id,
            message_id,
            snapshot,
        )
        .into_iter()
        .find(|attachment| attachment.attach_num == attach_num) else {
            responses.extend_from_slice(&rop_error_response(
                0x24,
                request.response_handle_index(),
                0x8004_010F,
            ));
            return;
        };
        let changes = session
            .pending_event_attachment_transactions
            .entry(parent_handle)
            .or_default();
        if attachment.canonical_id.is_nil() {
            changes
                .upserts
                .retain(|upsert| upsert.attach_num != attach_num);
        } else if !changes
            .delete_attachment_ids
            .contains(&attachment.canonical_id)
        {
            changes.delete_attachment_ids.push(attachment.canonical_id);
        }
        responses.extend_from_slice(&rop_simple_success_response(request));
        return;
    }
    let Some(attachment) = snapshot.attachment_for_message(folder_id, message_id, attach_num)
    else {
        responses.extend_from_slice(&rop_error_response(
            0x24,
            request.response_handle_index(),
            0x8004_010F,
        ));
        return;
    };
    session
        .pending_attachment_deletions
        .insert((folder_id, message_id, attach_num), attachment.canonical_id);
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
    handle_slots: &mut Vec<u32>,
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
    let parent_handle = match save_attachment_parent_handle(
        session,
        handle_slots,
        request,
        handle,
        folder_id,
        message_id,
    ) {
        Ok(parent_handle) => parent_handle,
        Err(error) => {
            responses.extend_from_slice(&rop_error_response(
                0x25,
                request.response_handle_index(),
                error.as_u32(),
            ));
            return;
        }
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
        let parent_is_pending_message = matches!(
            session.handles.get(&parent_handle),
            Some(MapiObject::PendingMessage { .. })
        );
        let parent_is_saved_message = matches!(
            session.handles.get(&parent_handle),
            Some(MapiObject::Message { .. })
        );
        let parent_is_event = matches!(
            session.handles.get(&parent_handle),
            Some(MapiObject::Event { .. } | MapiObject::PendingEvent { .. })
        );
        let parent_is_contact = matches!(
            session.handles.get(&parent_handle),
            Some(MapiObject::Contact { .. } | MapiObject::PendingContact { .. })
        );
        if parent_is_saved_message {
            // [MS-OXCMSG] section 3.2.5.13 commits an existing Message's new
            // Attachment when SaveChangesAttachment succeeds. Continue to the
            // canonical write below; only a not-yet-saved parent Message keeps
            // the attachment in its handle-local transaction.
        } else if parent_is_pending_message {
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
        } else if parent_is_event {
            let changes = session
                .pending_event_attachment_transactions
                .entry(parent_handle)
                .or_default();
            changes
                .upserts
                .retain(|existing| existing.attach_num != attach_num);
            changes.upserts.push(MapiEventAttachmentUpsert {
                attach_num,
                attachment: attachment.clone(),
                custom_property_upserts: mapi_event_custom_property_values_from_map(&properties),
            });
        } else if parent_is_contact {
            if !properties
                .get(&PID_TAG_ATTACHMENT_CONTACT_PHOTO)
                .and_then(MapiValue::as_bool)
                .unwrap_or(false)
            {
                responses.extend_from_slice(&rop_error_response(
                    0x25,
                    request.response_handle_index(),
                    0x8004_0102,
                ));
                return;
            }
            session
                .pending_contact_photo_attachments
                .insert(parent_handle, attachment.clone());
            match session.handles.get_mut(&parent_handle) {
                Some(MapiObject::Contact { transaction, .. }) => {
                    transaction
                        .pending_properties
                        .insert(PID_LID_HAS_PICTURE_TAG, MapiValue::Bool(true));
                    transaction
                        .deleted_properties
                        .remove(&PID_LID_HAS_PICTURE_TAG);
                }
                Some(MapiObject::PendingContact { properties, .. }) => {
                    properties.insert(PID_LID_HAS_PICTURE_TAG, MapiValue::Bool(true));
                }
                _ => unreachable!("contact attachment parent changed during save"),
            }
        } else {
            responses.extend_from_slice(&rop_error_response(
                0x25,
                request.response_handle_index(),
                0x8004_010F,
            ));
            return;
        }
        if !parent_is_saved_message {
            session.handles.insert(
                handle,
                MapiObject::SavedAttachment {
                    folder_id,
                    message_id,
                    attach_num,
                    file_reference: format!(
                        "pending-{}:{parent_handle}:{attach_num}",
                        if parent_is_event {
                            "event"
                        } else if parent_is_contact {
                            "contact"
                        } else {
                            "message"
                        }
                    ),
                    file_name: attachment.file_name,
                    media_type: attachment.media_type,
                    disposition: attachment.disposition,
                    content_id: attachment.content_id,
                    size_octets: attachment.blob_bytes.len() as u64,
                },
            );
            set_handle_slot(
                handle_slots,
                Some(request.response_handle_index()),
                parent_handle,
            );
            responses.extend_from_slice(&rop_simple_success_response(request));
            return;
        }
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
            Ok(Some((updated_email, stored))) => {
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
                if let Some(MapiObject::Message { saved_email, .. }) =
                    session.handles.get_mut(&parent_handle)
                {
                    let durable_identity = saved_email
                        .as_ref()
                        .and_then(|saved| saved.durable_identity.clone());
                    *saved_email = Some(MapiSavedEmail {
                        email: updated_email,
                        durable_identity,
                    });
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
                set_handle_slot(
                    handle_slots,
                    Some(request.response_handle_index()),
                    parent_handle,
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
}

pub(super) fn event_attachments_for_parent_handle(
    session: &MapiSession,
    parent_handle: u32,
    folder_id: u64,
    message_id: u64,
    snapshot: &MapiMailStoreSnapshot,
) -> Vec<crate::mapi_store::MapiAttachment> {
    let mut attachments = snapshot
        .attachments_for_message(folder_id, message_id)
        .unwrap_or_default()
        .to_vec();
    let Some(changes) = session
        .pending_event_attachment_transactions
        .get(&parent_handle)
    else {
        return attachments;
    };
    attachments.retain(|attachment| {
        !changes
            .delete_attachment_ids
            .contains(&attachment.canonical_id)
    });
    attachments.extend(
        changes
            .upserts
            .iter()
            .map(|upsert| crate::mapi_store::MapiAttachment {
                attach_num: upsert.attach_num,
                canonical_id: Uuid::nil(),
                file_reference: format!("pending-event:{parent_handle}:{}", upsert.attach_num),
                file_name: upsert.attachment.file_name.clone(),
                media_type: upsert.attachment.media_type.clone(),
                disposition: upsert.attachment.disposition.clone(),
                content_id: upsert.attachment.content_id.clone(),
                size_octets: upsert.attachment.blob_bytes.len() as u64,
            }),
    );
    attachments.sort_by_key(|attachment| attachment.attach_num);
    attachments
}

fn contact_photo_attachment(
    contact: &crate::mapi_store::MapiContact,
) -> Option<crate::mapi_store::MapiAttachment> {
    let photo_data = contact.contact.photo_data.as_deref()?.trim();
    let bytes = BASE64_STANDARD.decode(photo_data).ok()?;
    let media_type = contact
        .contact
        .photo_content_type
        .clone()
        .unwrap_or_else(|| "image/jpeg".to_string());
    Some(crate::mapi_store::MapiAttachment {
        attach_num: 0,
        canonical_id: Uuid::nil(),
        file_reference: format!("contact-photo:{}", contact.canonical_id),
        file_name: contact_photo_file_name(&media_type),
        media_type,
        disposition: Some("inline".to_string()),
        content_id: None,
        size_octets: bytes.len() as u64,
    })
}

fn contact_photo_attachment_object(
    folder_id: u64,
    contact_id: u64,
    contact: &crate::mapi_store::MapiContact,
) -> Option<MapiObject> {
    let photo_data = contact.contact.photo_data.as_deref()?.trim();
    let data = BASE64_STANDARD.decode(photo_data).ok()?;
    let media_type = contact
        .contact
        .photo_content_type
        .clone()
        .unwrap_or_else(|| "image/jpeg".to_string());
    let file_name = contact_photo_file_name(&media_type);
    Some(MapiObject::PendingAttachment {
        folder_id,
        message_id: contact_id,
        attach_num: 0,
        properties: HashMap::from([
            (PID_TAG_ATTACHMENT_CONTACT_PHOTO, MapiValue::Bool(true)),
            (PID_TAG_ATTACH_MIME_TAG_W, MapiValue::String(media_type)),
            (PID_TAG_ATTACH_LONG_FILENAME_W, MapiValue::String(file_name)),
            (PID_TAG_ATTACH_METHOD, MapiValue::U32(ATTACH_BY_VALUE)),
            (PID_TAG_ATTACH_SIZE, MapiValue::U32(data.len() as u32)),
            (PID_TAG_ATTACHMENT_HIDDEN, MapiValue::Bool(true)),
        ]),
        data,
    })
}

fn contact_photo_file_name(media_type: &str) -> String {
    if media_type.eq_ignore_ascii_case("image/png") {
        "ContactPhoto.png".to_string()
    } else {
        "ContactPhoto.jpg".to_string()
    }
}

fn pending_event_attachment_object(
    folder_id: u64,
    message_id: u64,
    upsert: &MapiEventAttachmentUpsert,
) -> MapiObject {
    let mut properties = HashMap::from([
        (
            PID_TAG_ATTACH_LONG_FILENAME_W,
            MapiValue::String(upsert.attachment.file_name.clone()),
        ),
        (
            PID_TAG_ATTACH_MIME_TAG_W,
            MapiValue::String(upsert.attachment.media_type.clone()),
        ),
    ]);
    if let Some(content_id) = &upsert.attachment.content_id {
        properties.insert(
            PID_TAG_ATTACH_CONTENT_ID_W,
            MapiValue::String(content_id.clone()),
        );
    }
    if upsert
        .attachment
        .disposition
        .as_deref()
        .is_some_and(|value| value.eq_ignore_ascii_case("inline"))
    {
        properties.insert(PID_TAG_ATTACHMENT_HIDDEN, MapiValue::Bool(true));
    }
    MapiObject::PendingAttachment {
        folder_id,
        message_id,
        attach_num: upsert.attach_num,
        properties,
        data: upsert.attachment.blob_bytes.clone(),
    }
}

pub(super) fn apply_event_attachment_overlay_property(
    session: &MapiSession,
    parent_handle: Option<u32>,
    snapshot: &MapiMailStoreSnapshot,
    object: &mut MapiObject,
) {
    let Some(parent_handle) = parent_handle.filter(|handle| {
        session
            .pending_event_attachment_transactions
            .contains_key(handle)
    }) else {
        return;
    };
    let (folder_id, message_id) = match object {
        MapiObject::Event {
            folder_id,
            event_id,
            ..
        } => (*folder_id, *event_id),
        MapiObject::PendingEvent { folder_id, .. } => (*folder_id, 0),
        _ => return,
    };
    let has_attachments = !event_attachments_for_parent_handle(
        session,
        parent_handle,
        folder_id,
        message_id,
        snapshot,
    )
    .is_empty();
    match object {
        MapiObject::Event { transaction, .. } => {
            transaction
                .pending_properties
                .insert(PID_TAG_HAS_ATTACHMENTS, MapiValue::Bool(has_attachments));
        }
        MapiObject::PendingEvent { properties, .. } => {
            properties.insert(PID_TAG_HAS_ATTACHMENTS, MapiValue::Bool(has_attachments));
        }
        _ => {}
    }
}

pub(super) fn attachment_overlay_object(
    session: &MapiSession,
    handle_slots: &[u32],
    request: &RopRequest,
    snapshot: &MapiMailStoreSnapshot,
) -> Option<MapiObject> {
    let parent_handle = input_handle(handle_slots, request);
    let mut object = input_object(session, handle_slots, request).cloned();
    if let Some(object) = object.as_mut() {
        apply_event_attachment_overlay_property(session, parent_handle, snapshot, object);
    }
    object
}

fn next_pending_event_attachment_num(
    session: &MapiSession,
    parent_handle: u32,
    folder_id: u64,
    message_id: u64,
    snapshot: &MapiMailStoreSnapshot,
) -> u32 {
    let visible_max = event_attachments_for_parent_handle(
        session,
        parent_handle,
        folder_id,
        message_id,
        snapshot,
    )
    .into_iter()
    .map(|attachment| attachment.attach_num)
    .max();
    let child_max = session
        .pending_attachment_parent_messages
        .iter()
        .filter(|(_, pending_parent)| **pending_parent == parent_handle)
        .filter_map(
            |(child_handle, _)| match session.handles.get(child_handle) {
                Some(
                    MapiObject::PendingAttachment { attach_num, .. }
                    | MapiObject::SavedAttachment { attach_num, .. },
                ) => Some(*attach_num),
                _ => None,
            },
        )
        .max();
    visible_max
        .into_iter()
        .chain(child_max)
        .max()
        .map(|value| value.saturating_add(1))
        .unwrap_or(0)
}

pub(super) fn clear_event_attachment_transaction(session: &mut MapiSession, parent_handle: u32) {
    session
        .pending_event_attachment_transactions
        .remove(&parent_handle);
    session
        .pending_attachment_parent_messages
        .retain(|_, pending_parent| *pending_parent != parent_handle);
}

pub(super) fn abandon_event_attachment_transaction(session: &mut MapiSession, parent_handle: u32) {
    let child_handles = session
        .pending_attachment_parent_messages
        .iter()
        .filter_map(|(child_handle, pending_parent)| {
            (*pending_parent == parent_handle).then_some(*child_handle)
        })
        .collect::<Vec<_>>();
    clear_event_attachment_transaction(session, parent_handle);
    for child_handle in child_handles {
        session.forget_handle(child_handle);
    }
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
        is_scheduling_body: false,
        blob_bytes: if payload.is_empty() {
            format!("Embedded message {attach_num}").into_bytes()
        } else {
            payload
        },
    }
}
