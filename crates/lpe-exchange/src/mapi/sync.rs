use super::rop::*;
use super::session::*;
use super::tables::*;
use super::*;

use crate::mapi::properties::*;

pub(in crate::mapi) use super::identity::{
    long_term_id_from_object_id, object_id_from_long_term_id, ARCHIVE_FOLDER_ID,
    CALENDAR_FOLDER_ID, COMMON_VIEWS_FOLDER_ID, CONFLICTS_FOLDER_ID, CONTACTS_FOLDER_ID,
    CONTACTS_SEARCH_FOLDER_ID, CONVERSATION_ACTION_SETTINGS_FOLDER_ID,
    CONVERSATION_HISTORY_FOLDER_ID, DEFERRED_ACTION_FOLDER_ID, DOCUMENT_LIBRARIES_FOLDER_ID,
    DRAFTS_FOLDER_ID, FREEBUSY_DATA_FOLDER_ID, IM_CONTACT_LIST_FOLDER_ID, INBOX_FOLDER_ID,
    IPM_SUBTREE_FOLDER_ID, JOURNAL_FOLDER_ID, JUNK_FOLDER_ID, LOCAL_FAILURES_FOLDER_ID,
    NOTES_FOLDER_ID, OUTBOX_FOLDER_ID, QUICK_CONTACTS_FOLDER_ID, REMINDERS_FOLDER_ID,
    ROOT_FOLDER_ID, RSS_FEEDS_FOLDER_ID, SCHEDULE_FOLDER_ID, SEARCH_FOLDER_ID, SENT_FOLDER_ID,
    SERVER_FAILURES_FOLDER_ID, SHORTCUTS_FOLDER_ID, SPOOLER_QUEUE_FOLDER_ID, STORE_REPLICA_ID,
    SUGGESTED_CONTACTS_FOLDER_ID, SYNC_ISSUES_FOLDER_ID, TASKS_FOLDER_ID, TODO_SEARCH_FOLDER_ID,
    TRACKED_MAIL_PROCESSING_FOLDER_ID, TRASH_FOLDER_ID, VIEWS_FOLDER_ID,
};

pub(in crate::mapi) const PRIVATE_LOGON_SPECIAL_FOLDER_IDS: [u64; 14] = [
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
    FREEBUSY_DATA_FOLDER_ID,
];

const IPM_SUBTREE_VIRTUAL_FOLDER_IDS: [u64; 27] = [
    INBOX_FOLDER_ID,
    DRAFTS_FOLDER_ID,
    OUTBOX_FOLDER_ID,
    SENT_FOLDER_ID,
    TRASH_FOLDER_ID,
    CONTACTS_FOLDER_ID,
    SUGGESTED_CONTACTS_FOLDER_ID,
    QUICK_CONTACTS_FOLDER_ID,
    IM_CONTACT_LIST_FOLDER_ID,
    CONTACTS_SEARCH_FOLDER_ID,
    CALENDAR_FOLDER_ID,
    JOURNAL_FOLDER_ID,
    NOTES_FOLDER_ID,
    TASKS_FOLDER_ID,
    REMINDERS_FOLDER_ID,
    DOCUMENT_LIBRARIES_FOLDER_ID,
    SYNC_ISSUES_FOLDER_ID,
    CONFLICTS_FOLDER_ID,
    LOCAL_FAILURES_FOLDER_ID,
    SERVER_FAILURES_FOLDER_ID,
    JUNK_FOLDER_ID,
    RSS_FEEDS_FOLDER_ID,
    TRACKED_MAIL_PROCESSING_FOLDER_ID,
    TODO_SEARCH_FOLDER_ID,
    CONVERSATION_ACTION_SETTINGS_FOLDER_ID,
    ARCHIVE_FOLDER_ID,
    CONVERSATION_HISTORY_FOLDER_ID,
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
    if sync_type == 0x02 {
        let mut folder_ids = HashSet::new();
        let mut rows = mailboxes
            .iter()
            .filter(|mailbox| mailbox_is_hierarchy_descendant(mailbox, folder_id, mailboxes))
            .filter(|mailbox| folder_ids.insert(mapi_folder_id(mailbox)))
            .cloned()
            .collect::<Vec<_>>();
        for &special_folder_id in hierarchy_virtual_folder_ids(folder_id) {
            if !special_folder_is_in_sync_scope(special_folder_id, folder_id) {
                continue;
            }
            if folder_ids.insert(special_folder_id) {
                if let Some(mailbox) = mapi_mailstore::virtual_special_mailbox(special_folder_id) {
                    rows.push(mailbox);
                }
            }
        }
        return rows;
    }

    folder_row_for_id(folder_id, mailboxes)
        .cloned()
        .into_iter()
        .collect()
}

fn hierarchy_virtual_folder_ids(sync_root_folder_id: u64) -> &'static [u64] {
    if sync_root_folder_id == IPM_SUBTREE_FOLDER_ID {
        &IPM_SUBTREE_VIRTUAL_FOLDER_IDS
    } else {
        &PRIVATE_LOGON_SPECIAL_FOLDER_IDS
    }
}

fn mailbox_is_hierarchy_descendant(
    mailbox: &JmapMailbox,
    sync_root_folder_id: u64,
    mailboxes: &[JmapMailbox],
) -> bool {
    let mut parent_folder_id = mailbox_parent_folder_id(mailbox, mailboxes);
    let mut visited = HashSet::new();
    while parent_folder_id != 0 && visited.insert(parent_folder_id) {
        if parent_folder_id == sync_root_folder_id {
            return true;
        }
        parent_folder_id = parent_folder_id_for_folder_id(parent_folder_id, mailboxes).unwrap_or(0);
    }
    false
}

fn mailbox_parent_folder_id(mailbox: &JmapMailbox, mailboxes: &[JmapMailbox]) -> u64 {
    match mailbox.role.as_str() {
        "__mapi_ipm_subtree"
        | "__mapi_deferred_action"
        | "__mapi_spooler_queue"
        | "__mapi_common_views"
        | "__mapi_schedule"
        | "__mapi_search"
        | "__mapi_views"
        | "__mapi_shortcuts" => ROOT_FOLDER_ID,
        "conflicts" | "local_failures" | "server_failures" => SYNC_ISSUES_FOLDER_ID,
        _ => mailbox
            .parent_id
            .and_then(|parent_id| mailboxes.iter().find(|candidate| candidate.id == parent_id))
            .map(mapi_folder_id)
            .unwrap_or(IPM_SUBTREE_FOLDER_ID),
    }
}

fn parent_folder_id_for_folder_id(folder_id: u64, mailboxes: &[JmapMailbox]) -> Option<u64> {
    match folder_id {
        IPM_SUBTREE_FOLDER_ID
        | DEFERRED_ACTION_FOLDER_ID
        | SPOOLER_QUEUE_FOLDER_ID
        | COMMON_VIEWS_FOLDER_ID
        | SCHEDULE_FOLDER_ID
        | SEARCH_FOLDER_ID
        | VIEWS_FOLDER_ID
        | SHORTCUTS_FOLDER_ID => Some(ROOT_FOLDER_ID),
        INBOX_FOLDER_ID
        | DRAFTS_FOLDER_ID
        | OUTBOX_FOLDER_ID
        | SENT_FOLDER_ID
        | TRASH_FOLDER_ID
        | CONTACTS_FOLDER_ID
        | SUGGESTED_CONTACTS_FOLDER_ID
        | QUICK_CONTACTS_FOLDER_ID
        | IM_CONTACT_LIST_FOLDER_ID
        | CONTACTS_SEARCH_FOLDER_ID
        | CALENDAR_FOLDER_ID
        | JOURNAL_FOLDER_ID
        | NOTES_FOLDER_ID
        | TASKS_FOLDER_ID
        | REMINDERS_FOLDER_ID
        | DOCUMENT_LIBRARIES_FOLDER_ID
        | SYNC_ISSUES_FOLDER_ID
        | JUNK_FOLDER_ID
        | RSS_FEEDS_FOLDER_ID
        | TRACKED_MAIL_PROCESSING_FOLDER_ID
        | TODO_SEARCH_FOLDER_ID
        | CONVERSATION_ACTION_SETTINGS_FOLDER_ID
        | ARCHIVE_FOLDER_ID
        | CONVERSATION_HISTORY_FOLDER_ID => Some(IPM_SUBTREE_FOLDER_ID),
        CONFLICTS_FOLDER_ID | LOCAL_FAILURES_FOLDER_ID | SERVER_FAILURES_FOLDER_ID => {
            Some(SYNC_ISSUES_FOLDER_ID)
        }
        ROOT_FOLDER_ID => None,
        _ => mailboxes
            .iter()
            .find(|mailbox| mapi_folder_id(mailbox) == folder_id)
            .map(|mailbox| mailbox_parent_folder_id(mailbox, mailboxes)),
    }
}

fn special_folder_is_in_sync_scope(special_folder_id: u64, sync_root_folder_id: u64) -> bool {
    match sync_root_folder_id {
        ROOT_FOLDER_ID => special_folder_id != ROOT_FOLDER_ID,
        IPM_SUBTREE_FOLDER_ID => matches!(
            special_folder_id,
            INBOX_FOLDER_ID
                | DRAFTS_FOLDER_ID
                | OUTBOX_FOLDER_ID
                | SENT_FOLDER_ID
                | TRASH_FOLDER_ID
                | CONTACTS_FOLDER_ID
                | SUGGESTED_CONTACTS_FOLDER_ID
                | QUICK_CONTACTS_FOLDER_ID
                | IM_CONTACT_LIST_FOLDER_ID
                | CONTACTS_SEARCH_FOLDER_ID
                | CALENDAR_FOLDER_ID
                | JOURNAL_FOLDER_ID
                | NOTES_FOLDER_ID
                | TASKS_FOLDER_ID
                | REMINDERS_FOLDER_ID
                | DOCUMENT_LIBRARIES_FOLDER_ID
                | SYNC_ISSUES_FOLDER_ID
                | CONFLICTS_FOLDER_ID
                | LOCAL_FAILURES_FOLDER_ID
                | SERVER_FAILURES_FOLDER_ID
                | JUNK_FOLDER_ID
                | RSS_FEEDS_FOLDER_ID
                | TRACKED_MAIL_PROCESSING_FOLDER_ID
                | TODO_SEARCH_FOLDER_ID
                | CONVERSATION_ACTION_SETTINGS_FOLDER_ID
                | ARCHIVE_FOLDER_ID
                | CONVERSATION_HISTORY_FOLDER_ID
        ),
        _ => false,
    }
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

pub(in crate::mapi) fn special_sync_objects_for(
    folder_id: u64,
    sync_type: u8,
    snapshot: &MapiMailStoreSnapshot,
) -> Vec<mapi_mailstore::SpecialMessageSyncFact> {
    if sync_type == 0x02 {
        return Vec::new();
    }
    match folder_id {
        NOTES_FOLDER_ID => snapshot
            .notes_for_folder(folder_id)
            .into_iter()
            .map(|note| mapi_mailstore::SpecialMessageSyncFact {
                folder_id: note.folder_id,
                item_id: note.id,
                canonical_id: note.canonical_id,
                subject: note.note.title.clone(),
                body_text: note.note.body_text.clone(),
                message_class: "IPM.StickyNote".to_string(),
                updated_at: note.note.updated_at.clone(),
                message_size: note_size(&note.note),
                named_properties: vec![
                    (
                        PID_LID_NOTE_COLOR_TAG,
                        mapi_mailstore::SpecialMessagePropertyValue::I32(
                            note_property_value(
                                &note.note,
                                note.id,
                                note.folder_id,
                                PID_LID_NOTE_COLOR_TAG,
                            )
                            .and_then(|value| value.as_i64())
                            .unwrap_or(3) as i32,
                        ),
                    ),
                    (
                        PID_LID_NOTE_HEIGHT_TAG,
                        mapi_mailstore::SpecialMessagePropertyValue::I32(200),
                    ),
                    (
                        PID_LID_NOTE_WIDTH_TAG,
                        mapi_mailstore::SpecialMessagePropertyValue::I32(166),
                    ),
                    (
                        PID_LID_NOTE_X_TAG,
                        mapi_mailstore::SpecialMessagePropertyValue::I32(80),
                    ),
                    (
                        PID_LID_NOTE_Y_TAG,
                        mapi_mailstore::SpecialMessagePropertyValue::I32(80),
                    ),
                ],
            })
            .collect(),
        JOURNAL_FOLDER_ID => snapshot
            .journal_entries_for_folder(folder_id)
            .into_iter()
            .map(|entry| journal_sync_object(entry))
            .collect(),
        _ => Vec::new(),
    }
}

pub(in crate::mapi) fn changed_special_sync_objects(
    objects: Vec<mapi_mailstore::SpecialMessageSyncFact>,
    changed_ids: &[Uuid],
) -> Vec<mapi_mailstore::SpecialMessageSyncFact> {
    if changed_ids.is_empty() {
        return Vec::new();
    }
    objects
        .into_iter()
        .filter(|object| changed_ids.contains(&object.canonical_id))
        .collect()
}

fn journal_sync_object(
    entry: &crate::mapi_store::MapiJournalEntry,
) -> mapi_mailstore::SpecialMessageSyncFact {
    let companies = journal_entry_property_value(
        &entry.entry,
        entry.id,
        entry.folder_id,
        PID_LID_COMPANIES_TAG,
    )
    .and_then(|value| match value {
        MapiValue::MultiString(values) => Some(values),
        _ => None,
    })
    .unwrap_or_default();
    let contacts = journal_entry_property_value(
        &entry.entry,
        entry.id,
        entry.folder_id,
        PID_LID_CONTACTS_TAG,
    )
    .and_then(|value| match value {
        MapiValue::MultiString(values) => Some(values),
        _ => None,
    })
    .unwrap_or_default();
    let mut named_properties = vec![
        (
            PID_LID_LOG_TYPE_W_TAG,
            mapi_mailstore::SpecialMessagePropertyValue::String(entry.entry.entry_type.clone()),
        ),
        (
            PID_LID_LOG_TYPE_DESC_W_TAG,
            mapi_mailstore::SpecialMessagePropertyValue::String(entry.entry.entry_type.clone()),
        ),
        (
            PID_LID_COMPANIES_TAG,
            mapi_mailstore::SpecialMessagePropertyValue::MultiString(companies),
        ),
        (
            PID_LID_CONTACTS_TAG,
            mapi_mailstore::SpecialMessagePropertyValue::MultiString(contacts),
        ),
        (
            PID_LID_LOG_DURATION_TAG,
            mapi_mailstore::SpecialMessagePropertyValue::I32(0),
        ),
        (
            PID_LID_LOG_FLAGS_TAG,
            mapi_mailstore::SpecialMessagePropertyValue::I32(0),
        ),
    ];
    if let Some(starts_at) = entry
        .entry
        .starts_at
        .as_deref()
        .or(entry.entry.occurred_at.as_deref())
    {
        named_properties.push((
            PID_LID_COMMON_START_TAG,
            mapi_mailstore::SpecialMessagePropertyValue::Time(starts_at.to_string()),
        ));
        named_properties.push((
            PID_LID_LOG_START_TAG,
            mapi_mailstore::SpecialMessagePropertyValue::Time(starts_at.to_string()),
        ));
    }
    if let Some(ends_at) = entry.entry.ends_at.as_deref() {
        named_properties.push((
            PID_LID_COMMON_END_TAG,
            mapi_mailstore::SpecialMessagePropertyValue::Time(ends_at.to_string()),
        ));
        named_properties.push((
            PID_LID_LOG_END_TAG,
            mapi_mailstore::SpecialMessagePropertyValue::Time(ends_at.to_string()),
        ));
    }

    mapi_mailstore::SpecialMessageSyncFact {
        folder_id: entry.folder_id,
        item_id: entry.id,
        canonical_id: entry.canonical_id,
        subject: entry.entry.subject.clone(),
        body_text: entry.entry.body_text.clone(),
        message_class: entry.entry.message_class.clone(),
        updated_at: entry.entry.updated_at.clone(),
        message_size: journal_entry_size(&entry.entry),
        named_properties,
    }
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
        return email.mailbox_states.iter().any(|state| state.role == role)
            || email.mailbox_role == role;
    }

    mailboxes
        .iter()
        .find(|mailbox| mapi_folder_id(mailbox) == folder_id)
        .is_some_and(|mailbox| {
            email
                .mailbox_states
                .iter()
                .any(|state| state.mailbox_id == mailbox.id)
                || email.mailbox_id == mailbox.id
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mailbox(id: u128, role: &str, name: &str) -> JmapMailbox {
        JmapMailbox {
            id: Uuid::from_u128(id),
            parent_id: None,
            role: role.to_string(),
            name: name.to_string(),
            sort_order: 0,
            modseq: 1,
            total_emails: 0,
            unread_emails: 0,
            is_subscribed: true,
        }
    }

    #[test]
    fn hierarchy_sync_mailboxes_deduplicate_fixed_special_folder_ids() {
        let duplicate_folder_id = crate::mapi::identity::mapi_store_id(100);
        let first_id = Uuid::from_u128(0x11111111111111111111111111111111);
        let second_id = Uuid::from_u128(0x22222222222222222222222222222222);
        crate::mapi::identity::remember_mapi_identity(first_id, duplicate_folder_id);
        crate::mapi::identity::remember_mapi_identity(second_id, duplicate_folder_id);
        let mailboxes = vec![
            mailbox(first_id.as_u128(), "custom", "Duplicate"),
            mailbox(second_id.as_u128(), "custom", "Duplicate"),
        ];

        let rows = sync_mailboxes_for(IPM_SUBTREE_FOLDER_ID, 0x02, &mailboxes);
        let duplicate_rows = rows
            .iter()
            .filter(|mailbox| mailbox.id == first_id || mailbox.id == second_id)
            .count();

        assert_eq!(duplicate_rows, 1);
    }

    #[test]
    fn hierarchy_sync_mailboxes_deduplicate_outlook_special_roles() {
        let roles = [
            ("suggested_contacts", SUGGESTED_CONTACTS_FOLDER_ID),
            ("quick_contacts", QUICK_CONTACTS_FOLDER_ID),
            ("im_contact_list", IM_CONTACT_LIST_FOLDER_ID),
            ("contacts_search", CONTACTS_SEARCH_FOLDER_ID),
            ("document_libraries", DOCUMENT_LIBRARIES_FOLDER_ID),
            ("sync_issues", SYNC_ISSUES_FOLDER_ID),
            ("conflicts", CONFLICTS_FOLDER_ID),
            ("local_failures", LOCAL_FAILURES_FOLDER_ID),
            ("server_failures", SERVER_FAILURES_FOLDER_ID),
            ("junk", JUNK_FOLDER_ID),
            ("rss_feeds", RSS_FEEDS_FOLDER_ID),
            ("tracked_mail_processing", TRACKED_MAIL_PROCESSING_FOLDER_ID),
            ("todo_search", TODO_SEARCH_FOLDER_ID),
            (
                "conversation_action_settings",
                CONVERSATION_ACTION_SETTINGS_FOLDER_ID,
            ),
            ("archive", ARCHIVE_FOLDER_ID),
            ("conversation_history", CONVERSATION_HISTORY_FOLDER_ID),
        ];
        let mailboxes = roles
            .iter()
            .enumerate()
            .map(|(index, (role, _))| {
                mailbox(
                    0x33333333333333333333333333333330 + index as u128,
                    role,
                    role,
                )
            })
            .collect::<Vec<_>>();

        let rows = sync_mailboxes_for(IPM_SUBTREE_FOLDER_ID, 0x02, &mailboxes);

        for (role, folder_id) in roles {
            assert_eq!(
                rows.iter()
                    .filter(|mailbox| mailbox.role == role && mapi_folder_id(mailbox) == folder_id)
                    .count(),
                1,
                "{role} should appear once"
            );
        }
    }
}
