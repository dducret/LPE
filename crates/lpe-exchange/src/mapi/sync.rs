use super::rop::*;
use super::session::*;
use super::tables::*;
use super::*;

pub(in crate::mapi) use super::identity::{
    long_term_id_from_object_id, object_id_from_long_term_id, CALENDAR_FOLDER_ID,
    COMMON_VIEWS_FOLDER_ID, CONTACTS_FOLDER_ID, DEFERRED_ACTION_FOLDER_ID, DRAFTS_FOLDER_ID,
    INBOX_FOLDER_ID, IPM_SUBTREE_FOLDER_ID, OUTBOX_FOLDER_ID, ROOT_FOLDER_ID, SCHEDULE_FOLDER_ID,
    SEARCH_FOLDER_ID, SENT_FOLDER_ID, SHORTCUTS_FOLDER_ID, SPOOLER_QUEUE_FOLDER_ID,
    STORE_REPLICA_ID, TRASH_FOLDER_ID, VIEWS_FOLDER_ID,
};

pub(in crate::mapi) const PRIVATE_LOGON_SPECIAL_FOLDER_IDS: [u64; 13] = [
    ROOT_FOLDER_ID,
    DEFERRED_ACTION_FOLDER_ID,
    SPOOLER_QUEUE_FOLDER_ID,
    IPM_SUBTREE_FOLDER_ID,
    INBOX_FOLDER_ID,
    OUTBOX_FOLDER_ID,
    SENT_FOLDER_ID,
    TRASH_FOLDER_ID,
    COMMON_VIEWS_FOLDER_ID,
    SCHEDULE_FOLDER_ID,
    SEARCH_FOLDER_ID,
    VIEWS_FOLDER_ID,
    SHORTCUTS_FOLDER_ID,
];

pub(in crate::mapi) fn rop_synchronization_configure_response(request: &RopRequest) -> Vec<u8> {
    let mut response = vec![0x70, request.output_handle_index.unwrap_or(0)];
    write_u32(&mut response, 0);
    response
}

pub(in crate::mapi) fn rop_fast_transfer_source_copy_response(request: &RopRequest) -> Vec<u8> {
    let mut response = vec![request.rop_id, request.output_handle_index.unwrap_or(0)];
    write_u32(&mut response, 0);
    response
}

pub(in crate::mapi) fn rop_fast_transfer_source_get_buffer_response(
    request: &RopRequest,
    transfer_buffer: &[u8],
    transfer_position: &mut usize,
) -> Vec<u8> {
    let requested = request
        .fast_transfer_buffer_size()
        .clamp(1, u16::MAX as usize);
    let end = transfer_position
        .saturating_add(requested)
        .min(transfer_buffer.len());
    let chunk = transfer_buffer[*transfer_position..end].to_vec();
    *transfer_position = end;
    let done = *transfer_position >= transfer_buffer.len();
    let total_steps = transfer_buffer
        .len()
        .div_ceil(requested)
        .min(u16::MAX as usize) as u16;
    let completed_steps = if total_steps == 0 {
        0
    } else {
        (*transfer_position)
            .div_ceil(requested)
            .min(u16::MAX as usize) as u16
    };

    let mut response = vec![0x4E, request.response_handle_index()];
    write_u32(&mut response, 0);
    response.extend_from_slice(&(if done { 0x0003u16 } else { 0x0001u16 }).to_le_bytes());
    response.extend_from_slice(&completed_steps.to_le_bytes());
    response.extend_from_slice(&total_steps.to_le_bytes());
    response.push(0);
    response.extend_from_slice(&(chunk.len().min(u16::MAX as usize) as u16).to_le_bytes());
    response.extend_from_slice(&chunk);
    response
}

pub(in crate::mapi) fn rop_synchronization_get_transfer_state_response(
    request: &RopRequest,
) -> Vec<u8> {
    let mut response = vec![0x82, request.output_handle_index.unwrap_or(0)];
    write_u32(&mut response, 0);
    response
}

pub(in crate::mapi) fn commit_uploaded_sync_state(
    state: &mut Vec<u8>,
    state_upload_buffer: &mut Vec<u8>,
) {
    if state_upload_buffer.is_empty() {
        return;
    }
    if state.is_empty() {
        *state = std::mem::take(state_upload_buffer);
    } else {
        state.extend_from_slice(&std::mem::take(state_upload_buffer));
    }
}

pub(in crate::mapi) fn rop_synchronization_import_message_change_response(
    request: &RopRequest,
    message_id: u64,
) -> Vec<u8> {
    let mut response = vec![0x72, request.output_handle_index.unwrap_or(0)];
    write_u32(&mut response, 0);
    write_u64(&mut response, message_id);
    response
}

pub(in crate::mapi) fn rop_synchronization_import_hierarchy_change_response(
    request: &RopRequest,
    folder_id: u64,
) -> Vec<u8> {
    let mut response = vec![0x73, request.response_handle_index()];
    write_u32(&mut response, 0);
    write_u64(&mut response, folder_id);
    response
}

pub(in crate::mapi) fn rop_synchronization_import_message_move_response(
    request: &RopRequest,
    message_id: u64,
) -> Vec<u8> {
    let mut response = vec![0x78, request.response_handle_index()];
    write_u32(&mut response, 0);
    write_u64(&mut response, message_id);
    response
}

pub(in crate::mapi) fn rop_get_local_replica_ids_response(
    request: &RopRequest,
    first_global_counter: u64,
) -> Vec<u8> {
    let mut response = vec![0x7F, request.response_handle_index()];
    write_u32(&mut response, 0);
    response.extend_from_slice(&mapi_mailstore::STORE_REPLICA_GUID);
    response.extend_from_slice(&crate::mapi::identity::globcnt_bytes(first_global_counter));
    response
}

pub(in crate::mapi) fn emails_for_folder<'a>(
    folder_id: u64,
    mailboxes: &[JmapMailbox],
    emails: &'a [JmapEmail],
) -> Vec<&'a JmapEmail> {
    emails
        .iter()
        .filter(|email| email_matches_folder(email, folder_id, mailboxes))
        .collect()
}

pub(in crate::mapi) fn sync_mailboxes_for(
    folder_id: u64,
    sync_type: u8,
    mailboxes: &[JmapMailbox],
) -> Vec<JmapMailbox> {
    if sync_type == 0x02 && is_root_hierarchy_folder(folder_id) {
        return mailboxes.to_vec();
    }

    folder_row_for_id(folder_id, mailboxes)
        .cloned()
        .into_iter()
        .collect()
}

pub(in crate::mapi) fn sync_emails_for(
    folder_id: u64,
    sync_type: u8,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
) -> Vec<JmapEmail> {
    if sync_type == 0x02 {
        return Vec::new();
    }

    emails_for_folder(folder_id, mailboxes, emails)
        .into_iter()
        .cloned()
        .collect()
}

pub(in crate::mapi) fn sync_checkpoint_kind(sync_type: u8) -> MapiCheckpointKind {
    if sync_type == 0x02 {
        MapiCheckpointKind::Hierarchy
    } else {
        MapiCheckpointKind::Content
    }
}

pub(in crate::mapi) fn sync_checkpoint_mailbox_id(
    folder_id: u64,
    sync_type: u8,
    mailboxes: &[JmapMailbox],
) -> Option<Uuid> {
    if sync_type == 0x02 {
        return None;
    }
    mailboxes
        .iter()
        .find(|mailbox| mapi_folder_id(mailbox) == folder_id)
        .map(|mailbox| mailbox.id)
}

pub(in crate::mapi) fn changed_sync_mailboxes(
    mailboxes: Vec<JmapMailbox>,
    changed_ids: &[Uuid],
) -> Vec<JmapMailbox> {
    if changed_ids.is_empty() {
        return Vec::new();
    }
    mailboxes
        .into_iter()
        .filter(|mailbox| changed_ids.contains(&mailbox.id))
        .collect()
}

pub(in crate::mapi) fn changed_sync_emails(
    emails: Vec<JmapEmail>,
    changed_ids: &[Uuid],
) -> Vec<JmapEmail> {
    if changed_ids.is_empty() {
        return Vec::new();
    }
    emails
        .into_iter()
        .filter(|email| changed_ids.contains(&email.id))
        .collect()
}

pub(in crate::mapi) fn sync_attachment_facts_for(
    folder_id: u64,
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
) -> Vec<mapi_mailstore::MessageAttachmentSyncFacts> {
    emails
        .iter()
        .filter_map(|email| {
            let attachments = snapshot
                .attachments_for_message(folder_id, mapi_message_id(email))
                .unwrap_or_default();
            if attachments.is_empty() {
                return None;
            }
            Some(mapi_mailstore::MessageAttachmentSyncFacts {
                message_id: email.id,
                attachments: attachments
                    .iter()
                    .map(|attachment| mapi_mailstore::AttachmentSyncFact {
                        id: attachment.canonical_id,
                        file_reference: attachment.file_reference.clone(),
                        file_name: attachment.file_name.clone(),
                        media_type: attachment.media_type.clone(),
                        size_octets: attachment.size_octets,
                    })
                    .collect(),
            })
        })
        .collect()
}

pub(in crate::mapi) fn fast_transfer_manifest_for_object(
    object: &MapiObject,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
) -> Option<(u64, Vec<u8>)> {
    match object {
        MapiObject::Folder { folder_id, .. } => {
            let folder = folder_row_for_id(*folder_id, mailboxes)
                .cloned()
                .into_iter()
                .collect::<Vec<_>>();
            let messages = emails_for_folder(*folder_id, mailboxes, emails)
                .into_iter()
                .cloned()
                .collect::<Vec<_>>();
            let attachment_facts = sync_attachment_facts_for(*folder_id, &messages, snapshot);
            Some((
                *folder_id,
                mapi_mailstore::fast_transfer_manifest_buffer_with_attachments(
                    *folder_id,
                    &folder,
                    &messages,
                    &attachment_facts,
                ),
            ))
        }
        MapiObject::Message {
            folder_id,
            message_id,
        } => {
            let message = message_for_id(*folder_id, *message_id, mailboxes, emails)?.clone();
            let folder = folder_row_for_id(*folder_id, mailboxes)
                .cloned()
                .into_iter()
                .collect::<Vec<_>>();
            let messages = vec![message];
            let attachment_facts = sync_attachment_facts_for(*folder_id, &messages, snapshot);
            Some((
                *folder_id,
                mapi_mailstore::fast_transfer_manifest_buffer_with_attachments(
                    *folder_id,
                    &folder,
                    &messages,
                    &attachment_facts,
                ),
            ))
        }
        _ => None,
    }
}

pub(in crate::mapi) fn message_for_id<'a>(
    folder_id: u64,
    message_id: u64,
    mailboxes: &[JmapMailbox],
    emails: &'a [JmapEmail],
) -> Option<&'a JmapEmail> {
    emails.iter().find(|email| {
        mapi_message_id(email) == message_id && email_matches_folder(email, folder_id, mailboxes)
    })
}

pub(in crate::mapi) fn next_pending_attachment_num(
    session: &MapiSession,
    folder_id: u64,
    message_id: u64,
    snapshot: &MapiMailStoreSnapshot,
) -> u32 {
    let snapshot_max = snapshot
        .attachments_for_message(folder_id, message_id)
        .unwrap_or_default()
        .iter()
        .map(|attachment| attachment.attach_num)
        .max();
    let session_max = session
        .handles
        .values()
        .filter_map(|object| match object {
            MapiObject::PendingAttachment {
                folder_id: pending_folder_id,
                message_id: pending_message_id,
                attach_num,
                ..
            }
            | MapiObject::SavedAttachment {
                folder_id: pending_folder_id,
                message_id: pending_message_id,
                attach_num,
                ..
            } if *pending_folder_id == folder_id && *pending_message_id == message_id => {
                Some(*attach_num)
            }
            _ => None,
        })
        .max();
    snapshot_max
        .into_iter()
        .chain(session_max)
        .max()
        .map(|value| value.saturating_add(1))
        .unwrap_or(0)
}

pub(in crate::mapi) fn email_matches_folder(
    email: &JmapEmail,
    folder_id: u64,
    mailboxes: &[JmapMailbox],
) -> bool {
    if let Some(role) = role_for_folder_id(folder_id) {
        return email.mailbox_role == role;
    }

    mailboxes
        .iter()
        .find(|mailbox| mapi_folder_id(mailbox) == folder_id)
        .is_some_and(|mailbox| email.mailbox_id == mailbox.id)
}
