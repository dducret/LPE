use super::rop::*;
use super::session::*;
use super::tables::*;
use super::*;

use crate::mapi::properties::*;
use crate::mapi::wire::RopId;

pub(in crate::mapi) use super::identity::{
    long_term_id_from_object_id, ARCHIVE_FOLDER_ID, CALENDAR_FOLDER_ID, COMMON_VIEWS_FOLDER_ID,
    CONFLICTS_FOLDER_ID, CONTACTS_FOLDER_ID, CONTACTS_SEARCH_FOLDER_ID,
    CONVERSATION_ACTION_SETTINGS_FOLDER_ID, CONVERSATION_HISTORY_FOLDER_ID,
    DEFERRED_ACTION_FOLDER_ID, DOCUMENT_LIBRARIES_FOLDER_ID, DRAFTS_FOLDER_ID,
    FREEBUSY_DATA_FOLDER_ID, IM_CONTACT_LIST_FOLDER_ID, INBOX_FOLDER_ID, IPM_SUBTREE_FOLDER_ID,
    JOURNAL_FOLDER_ID, JUNK_FOLDER_ID, LOCAL_FAILURES_FOLDER_ID, NOTES_FOLDER_ID, OUTBOX_FOLDER_ID,
    PUBLIC_FOLDERS_ROOT_FOLDER_ID, QUICK_CONTACTS_FOLDER_ID, QUICK_STEP_SETTINGS_FOLDER_ID,
    REMINDERS_FOLDER_ID, ROOT_FOLDER_ID, RSS_FEEDS_FOLDER_ID, SCHEDULE_FOLDER_ID, SEARCH_FOLDER_ID,
    SENT_FOLDER_ID, SERVER_FAILURES_FOLDER_ID, SHORTCUTS_FOLDER_ID, SPOOLER_QUEUE_FOLDER_ID,
    STORE_REPLICA_ID, SUGGESTED_CONTACTS_FOLDER_ID, SYNC_ISSUES_FOLDER_ID, TASKS_FOLDER_ID,
    TODO_SEARCH_FOLDER_ID, TRACKED_MAIL_PROCESSING_FOLDER_ID, TRASH_FOLDER_ID, VIEWS_FOLDER_ID,
};

pub(in crate::mapi) const PID_TAG_ROAMING_DATATYPES: u32 = 0x7C06_0003;
pub(in crate::mapi) const PID_TAG_ROAMING_DICTIONARY: u32 = 0x7C07_0102;
pub(in crate::mapi) const PID_TAG_ROAMING_XML_STREAM: u32 = 0x7C08_0102;

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

pub(in crate::mapi) const PUBLIC_LOGON_SPECIAL_FOLDER_IDS: [u64; 1] =
    [PUBLIC_FOLDERS_ROOT_FOLDER_ID];

const ROOT_VIRTUAL_FOLDER_IDS: [u64; 35] = [
    ROOT_FOLDER_ID,
    DEFERRED_ACTION_FOLDER_ID,
    SPOOLER_QUEUE_FOLDER_ID,
    IPM_SUBTREE_FOLDER_ID,
    COMMON_VIEWS_FOLDER_ID,
    SCHEDULE_FOLDER_ID,
    SEARCH_FOLDER_ID,
    VIEWS_FOLDER_ID,
    SHORTCUTS_FOLDER_ID,
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
    ARCHIVE_FOLDER_ID,
    FREEBUSY_DATA_FOLDER_ID,
];

const IPM_SUBTREE_VIRTUAL_FOLDER_IDS: [u64; 21] = [
    IPM_SUBTREE_FOLDER_ID,
    INBOX_FOLDER_ID,
    DRAFTS_FOLDER_ID,
    OUTBOX_FOLDER_ID,
    SENT_FOLDER_ID,
    TRASH_FOLDER_ID,
    CONTACTS_FOLDER_ID,
    SUGGESTED_CONTACTS_FOLDER_ID,
    QUICK_CONTACTS_FOLDER_ID,
    IM_CONTACT_LIST_FOLDER_ID,
    CALENDAR_FOLDER_ID,
    JOURNAL_FOLDER_ID,
    NOTES_FOLDER_ID,
    TASKS_FOLDER_ID,
    SYNC_ISSUES_FOLDER_ID,
    CONFLICTS_FOLDER_ID,
    LOCAL_FAILURES_FOLDER_ID,
    SERVER_FAILURES_FOLDER_ID,
    JUNK_FOLDER_ID,
    RSS_FEEDS_FOLDER_ID,
    ARCHIVE_FOLDER_ID,
];

const SEARCH_VIRTUAL_FOLDER_IDS: [u64; 1] = [CONTACTS_SEARCH_FOLDER_ID];

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

pub(in crate::mapi) fn rop_synchronization_import_message_change_response(
    request: &RopRequest,
) -> Vec<u8> {
    let mut response = vec![0x72, request.output_handle_index.unwrap_or(0)];
    write_u32(&mut response, 0);
    write_object_id(&mut response, 0);
    response
}

pub(in crate::mapi) fn rop_synchronization_import_hierarchy_change_response(
    request: &RopRequest,
) -> Vec<u8> {
    let mut response = vec![0x73, request.response_handle_index()];
    write_u32(&mut response, 0);
    write_object_id(&mut response, 0);
    response
}

pub(in crate::mapi) fn rop_synchronization_import_message_move_response(
    request: &RopRequest,
) -> Vec<u8> {
    let mut response = vec![0x78, request.response_handle_index()];
    write_u32(&mut response, 0);
    write_object_id(&mut response, 0);
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

#[cfg(test)]
pub(in crate::mapi) fn sync_mailboxes_for(
    folder_id: u64,
    sync_type: u8,
    mailboxes: &[JmapMailbox],
) -> Vec<JmapMailbox> {
    sync_mailboxes_for_excluding_deleted(folder_id, sync_type, mailboxes, &HashSet::new())
}

pub(in crate::mapi) fn sync_mailboxes_for_excluding_deleted(
    folder_id: u64,
    sync_type: u8,
    mailboxes: &[JmapMailbox],
    deleted_advertised_special_folders: &HashSet<u64>,
) -> Vec<JmapMailbox> {
    if sync_type == 0x02 {
        let mut folder_ids = HashSet::new();
        let mut rows = mailboxes
            .iter()
            .filter(|mailbox| {
                mapi_folder_id(mailbox) == folder_id
                    || mailbox_is_hierarchy_descendant(mailbox, folder_id, mailboxes)
            })
            .filter(|mailbox| {
                !mailbox_shadowed_by_active_outlook_special_folder(
                    mailbox,
                    deleted_advertised_special_folders,
                )
            })
            .filter(|mailbox| mapi_folder_id(mailbox) != REMINDERS_FOLDER_ID)
            .filter(|mailbox| folder_ids.insert(mapi_folder_id(mailbox)))
            .cloned()
            .collect::<Vec<_>>();
        for special_folder_id in hierarchy_virtual_folder_ids(folder_id) {
            if !special_folder_is_in_sync_scope(special_folder_id, folder_id) {
                continue;
            }
            if deleted_advertised_special_folders.contains(&special_folder_id) {
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

#[cfg(test)]
pub(in crate::mapi) fn sync_state_mailboxes_for(
    folder_id: u64,
    sync_type: u8,
    mailboxes: &[JmapMailbox],
) -> Vec<JmapMailbox> {
    sync_mailboxes_for(folder_id, sync_type, mailboxes)
}

pub(in crate::mapi) fn sync_state_mailboxes_for_excluding_deleted(
    folder_id: u64,
    sync_type: u8,
    mailboxes: &[JmapMailbox],
    deleted_advertised_special_folders: &HashSet<u64>,
) -> Vec<JmapMailbox> {
    sync_mailboxes_for_excluding_deleted(
        folder_id,
        sync_type,
        mailboxes,
        deleted_advertised_special_folders,
    )
}

fn hierarchy_virtual_folder_ids(sync_root_folder_id: u64) -> Vec<u64> {
    match sync_root_folder_id {
        ROOT_FOLDER_ID => ROOT_VIRTUAL_FOLDER_IDS.to_vec(),
        IPM_SUBTREE_FOLDER_ID => IPM_SUBTREE_VIRTUAL_FOLDER_IDS.to_vec(),
        SEARCH_FOLDER_ID => SEARCH_VIRTUAL_FOLDER_IDS.to_vec(),
        _ => PRIVATE_LOGON_SPECIAL_FOLDER_IDS.to_vec(),
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
        | "__mapi_shortcuts"
        | "__mapi_freebusy_data" => ROOT_FOLDER_ID,
        "__mapi_collaboration_calendar" => IPM_SUBTREE_FOLDER_ID,
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
        | SHORTCUTS_FOLDER_ID
        | REMINDERS_FOLDER_ID
        | DOCUMENT_LIBRARIES_FOLDER_ID
        | TRACKED_MAIL_PROCESSING_FOLDER_ID
        | TODO_SEARCH_FOLDER_ID
        | FREEBUSY_DATA_FOLDER_ID => Some(ROOT_FOLDER_ID),
        CONTACTS_SEARCH_FOLDER_ID => Some(SEARCH_FOLDER_ID),
        INBOX_FOLDER_ID
        | DRAFTS_FOLDER_ID
        | OUTBOX_FOLDER_ID
        | SENT_FOLDER_ID
        | TRASH_FOLDER_ID
        | CONTACTS_FOLDER_ID
        | SUGGESTED_CONTACTS_FOLDER_ID
        | QUICK_CONTACTS_FOLDER_ID
        | IM_CONTACT_LIST_FOLDER_ID
        | CALENDAR_FOLDER_ID
        | JOURNAL_FOLDER_ID
        | NOTES_FOLDER_ID
        | TASKS_FOLDER_ID
        | SYNC_ISSUES_FOLDER_ID
        | JUNK_FOLDER_ID
        | RSS_FEEDS_FOLDER_ID
        | CONVERSATION_ACTION_SETTINGS_FOLDER_ID
        | QUICK_STEP_SETTINGS_FOLDER_ID
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
        ROOT_FOLDER_ID => true,
        IPM_SUBTREE_FOLDER_ID => matches!(
            special_folder_id,
            IPM_SUBTREE_FOLDER_ID
                | INBOX_FOLDER_ID
                | DRAFTS_FOLDER_ID
                | OUTBOX_FOLDER_ID
                | SENT_FOLDER_ID
                | TRASH_FOLDER_ID
                | CONTACTS_FOLDER_ID
                | SUGGESTED_CONTACTS_FOLDER_ID
                | QUICK_CONTACTS_FOLDER_ID
                | IM_CONTACT_LIST_FOLDER_ID
                | CALENDAR_FOLDER_ID
                | JOURNAL_FOLDER_ID
                | NOTES_FOLDER_ID
                | TASKS_FOLDER_ID
                | SYNC_ISSUES_FOLDER_ID
                | CONFLICTS_FOLDER_ID
                | LOCAL_FAILURES_FOLDER_ID
                | SERVER_FAILURES_FOLDER_ID
                | JUNK_FOLDER_ID
                | RSS_FEEDS_FOLDER_ID
                | ARCHIVE_FOLDER_ID
                | CONVERSATION_HISTORY_FOLDER_ID
        ),
        SEARCH_FOLDER_ID => special_folder_id == CONTACTS_SEARCH_FOLDER_ID,
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
        .or_else(|| {
            crate::mapi_mailstore::virtual_special_mailbox(folder_id).map(|mailbox| mailbox.id)
        })
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
    account_id: Uuid,
) -> Vec<mapi_mailstore::SpecialMessageSyncFact> {
    if sync_type == 0x02 {
        return Vec::new();
    }
    let mut objects = Vec::new();
    if folder_id == CALENDAR_FOLDER_ID
        || snapshot
            .collaboration_folder_for_id(folder_id)
            .is_some_and(|folder| {
                folder.kind == crate::mapi_store::MapiCollaborationFolderKind::Calendar
            })
    {
        objects.extend(
            snapshot
                .events_for_folder(folder_id)
                .into_iter()
                .map(|event| {
                    calendar_sync_object(
                        event,
                        snapshot.reminder_for_source("event", event.canonical_id),
                    )
                }),
        );
    } else if snapshot
        .collaboration_folder_for_id(folder_id)
        .is_some_and(|folder| {
            folder.kind == crate::mapi_store::MapiCollaborationFolderKind::Contacts
        })
    {
        objects.extend(
            snapshot
                .contacts_for_folder(folder_id)
                .into_iter()
                .map(contact_sync_object),
        );
    } else if snapshot
        .collaboration_folder_for_id(folder_id)
        .is_some_and(|folder| folder.kind == crate::mapi_store::MapiCollaborationFolderKind::Task)
    {
        objects.extend(
            snapshot
                .tasks_for_folder(folder_id)
                .into_iter()
                .map(|task| {
                    task_sync_object(
                        task,
                        snapshot.reminder_for_source("task", task.canonical_id),
                    )
                }),
        );
    } else if snapshot.public_folder_for_id(folder_id).is_some() {
        objects.extend(
            snapshot
                .public_folder_items_for_folder(folder_id)
                .into_iter()
                .map(public_folder_item_sync_object),
        );
    } else {
        objects.extend(match folder_id {
            CONTACTS_SEARCH_FOLDER_ID => snapshot
                .contacts_search_results()
                .into_iter()
                .map(|contact| {
                    sync_object_projected_to_folder(
                        contact_sync_object(contact),
                        CONTACTS_SEARCH_FOLDER_ID,
                    )
                })
                .collect(),
            TODO_SEARCH_FOLDER_ID => snapshot
                .todo_search_results()
                .into_iter()
                .map(|task| {
                    sync_object_projected_to_folder(
                        task_sync_object(
                            task,
                            snapshot.reminder_for_source("task", task.canonical_id),
                        ),
                        TODO_SEARCH_FOLDER_ID,
                    )
                })
                .collect(),
            REMINDERS_FOLDER_ID => snapshot
                .reminder_tasks()
                .into_iter()
                .map(|task| {
                    sync_object_projected_to_folder(
                        task_sync_object(
                            task,
                            snapshot.reminder_for_source("task", task.canonical_id),
                        ),
                        REMINDERS_FOLDER_ID,
                    )
                })
                .collect(),
            NOTES_FOLDER_ID => snapshot
                .notes_for_folder(folder_id)
                .into_iter()
                .map(|note| mapi_mailstore::SpecialMessageSyncFact {
                    folder_id: note.folder_id,
                    item_id: note.id,
                    canonical_id: note.canonical_id,
                    associated: false,
                    subject: note.note.title.clone(),
                    body_text: note.note.body_text.clone(),
                    message_class: "IPM.StickyNote".to_string(),
                    last_modified_filetime: mapi_mailstore::filetime_from_rfc3339_utc(
                        &note.note.updated_at,
                    ),
                    message_size: note_size(&note.note),
                    read_state: None,
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
            COMMON_VIEWS_FOLDER_ID => snapshot
                .common_views_messages()
                .map(|message| common_views_sync_object(message, account_id))
                .collect(),
            CONVERSATION_ACTION_SETTINGS_FOLDER_ID => snapshot
                .conversation_action_table_messages()
                .iter()
                .map(conversation_action_sync_object)
                .collect(),
            FREEBUSY_DATA_FOLDER_ID => snapshot
                .delegate_freebusy_messages()
                .iter()
                .map(delegate_freebusy_sync_object)
                .collect(),
            _ => Vec::new(),
        });
    }
    objects.extend(
        snapshot
            .associated_config_sync_messages_for_folder(folder_id)
            .iter()
            .map(associated_config_sync_object),
    );
    objects
}

fn sync_object_projected_to_folder(
    mut object: mapi_mailstore::SpecialMessageSyncFact,
    folder_id: u64,
) -> mapi_mailstore::SpecialMessageSyncFact {
    object.folder_id = folder_id;
    object
}

fn public_folder_item_sync_object(
    item: &crate::mapi_store::MapiPublicFolderItem,
) -> mapi_mailstore::SpecialMessageSyncFact {
    let message_class = if item.item.message_class.trim().is_empty() {
        "IPM.Post".to_string()
    } else {
        item.item.message_class.clone()
    };
    let message_size = item
        .item
        .subject
        .len()
        .saturating_add(item.item.body_text.len())
        .min(i64::MAX as usize) as i64;

    mapi_mailstore::SpecialMessageSyncFact {
        folder_id: item.folder_id,
        item_id: item.id,
        canonical_id: item.item.id,
        associated: false,
        subject: item.item.subject.clone(),
        body_text: item.item.body_text.clone(),
        message_class,
        last_modified_filetime: mapi_mailstore::filetime_from_rfc3339_utc(&item.item.updated_at),
        message_size,
        read_state: Some(item.item.is_read),
        named_properties: vec![
            (
                PID_TAG_ACCESS,
                mapi_mailstore::SpecialMessagePropertyValue::U32(MAPI_MESSAGE_ACCESS),
            ),
            (
                PID_TAG_HAS_ATTACHMENTS,
                mapi_mailstore::SpecialMessagePropertyValue::Bool(false),
            ),
            (
                PID_TAG_READ,
                mapi_mailstore::SpecialMessagePropertyValue::Bool(item.item.is_read),
            ),
        ],
    }
}

fn contact_sync_object(
    contact: &crate::mapi_store::MapiContact,
) -> mapi_mailstore::SpecialMessageSyncFact {
    let mut properties = Vec::new();
    for property_tag in [
        PID_TAG_DISPLAY_NAME_W,
        PID_TAG_EMAIL_ADDRESS_W,
        PID_TAG_SMTP_ADDRESS_W,
        PID_TAG_MOBILE_TELEPHONE_NUMBER_W,
        PID_TAG_BUSINESS_TELEPHONE_NUMBER_W,
        PID_TAG_HOME_TELEPHONE_NUMBER_W,
        PID_TAG_COMPANY_NAME_W,
        PID_TAG_TITLE_W,
        PID_TAG_ACCESS,
        PID_TAG_HAS_ATTACHMENTS,
    ] {
        if let Some(value) = contact_property_value(
            &contact.contact,
            contact.id,
            contact.folder_id,
            property_tag,
        )
        .and_then(special_message_property_value)
        {
            properties.push((property_tag, value));
        }
    }
    let change_number = mapi_mailstore::change_number_for_store_id(contact.id);

    mapi_mailstore::SpecialMessageSyncFact {
        folder_id: contact.folder_id,
        item_id: contact.id,
        canonical_id: contact.canonical_id,
        associated: false,
        subject: contact.contact.name.clone(),
        body_text: contact.contact.notes.clone(),
        message_class: "IPM.Contact".to_string(),
        last_modified_filetime: mapi_mailstore::filetime_from_change_number(change_number),
        message_size: contact_size(&contact.contact),
        read_state: None,
        named_properties: properties,
    }
}

fn task_sync_object(
    task: &crate::mapi_store::MapiTask,
    reminder: Option<&lpe_storage::ClientReminder>,
) -> mapi_mailstore::SpecialMessageSyncFact {
    let mut properties = Vec::new();
    for property_tag in [
        PID_TAG_FLAG_STATUS,
        PID_TAG_ACCESS,
        PID_TAG_HAS_ATTACHMENTS,
        PID_TAG_LAST_MODIFICATION_TIME,
        PID_TAG_LOCAL_COMMIT_TIME,
        PID_LID_REMINDER_SET_TAG,
        PID_LID_REMINDER_DELTA_TAG,
        PID_LID_REMINDER_TIME_TAG,
        PID_LID_REMINDER_SIGNAL_TIME_TAG,
        PID_LID_REMINDER_OVERRIDE_TAG,
        PID_LID_REMINDER_PLAY_SOUND_TAG,
        PID_LID_REMINDER_FILE_PARAMETER_W_TAG,
    ] {
        if let Some(value) = task_property_value_with_reminder(
            &task.task,
            task.id,
            task.folder_id,
            property_tag,
            reminder,
        )
        .and_then(special_message_property_value)
        {
            properties.push((property_tag, value));
        }
    }

    mapi_mailstore::SpecialMessageSyncFact {
        folder_id: task.folder_id,
        item_id: task.id,
        canonical_id: task.canonical_id,
        associated: false,
        subject: task.task.title.clone(),
        body_text: task.task.description.clone(),
        message_class: "IPM.Task".to_string(),
        last_modified_filetime: mapi_mailstore::filetime_from_rfc3339_utc(&task.task.updated_at),
        message_size: task_size(&task.task),
        read_state: None,
        named_properties: properties,
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
        associated: false,
        subject: entry.entry.subject.clone(),
        body_text: entry.entry.body_text.clone(),
        message_class: entry.entry.message_class.clone(),
        last_modified_filetime: mapi_mailstore::filetime_from_rfc3339_utc(&entry.entry.updated_at),
        message_size: journal_entry_size(&entry.entry),
        read_state: None,
        named_properties,
    }
}

fn navigation_shortcut_sync_object(
    message: &crate::mapi_store::MapiNavigationShortcutMessage,
    account_id: Uuid,
) -> mapi_mailstore::SpecialMessageSyncFact {
    let mut named_properties = Vec::new();
    for property_tag in [
        PID_TAG_WLINK_TYPE,
        PID_TAG_WLINK_FLAGS,
        PID_TAG_WLINK_SAVE_STAMP,
        PID_TAG_WLINK_ORDINAL,
        PID_TAG_WLINK_ENTRY_ID,
        PID_TAG_WLINK_RECORD_KEY,
        PID_TAG_WLINK_STORE_ENTRY_ID,
        PID_TAG_WLINK_FOLDER_TYPE,
        PID_TAG_WLINK_GROUP_HEADER_ID,
        PID_TAG_WLINK_GROUP_CLSID,
        PID_TAG_WLINK_GROUP_NAME_W,
        PID_TAG_WLINK_SECTION,
    ] {
        if let Some(value) = navigation_shortcut_property_value(message, account_id, property_tag)
            .and_then(special_message_property_value)
        {
            named_properties.push((property_tag, value));
        }
    }
    let change_number = mapi_mailstore::change_number_for_store_id(message.id);

    mapi_mailstore::SpecialMessageSyncFact {
        folder_id: message.folder_id,
        item_id: message.id,
        canonical_id: message.canonical_id,
        associated: true,
        subject: message.subject.clone(),
        body_text: String::new(),
        message_class: "IPM.Microsoft.WunderBar.Link".to_string(),
        last_modified_filetime: mapi_mailstore::filetime_from_change_number(change_number),
        message_size: 128,
        read_state: None,
        named_properties,
    }
}

fn common_views_sync_object(
    message: crate::mapi_store::MapiCommonViewsMessage,
    account_id: Uuid,
) -> mapi_mailstore::SpecialMessageSyncFact {
    match message {
        crate::mapi_store::MapiCommonViewsMessage::NavigationShortcut(message) => {
            navigation_shortcut_sync_object(&message, account_id)
        }
        crate::mapi_store::MapiCommonViewsMessage::NamedView(message) => {
            common_view_named_view_sync_object(&message, account_id)
        }
        crate::mapi_store::MapiCommonViewsMessage::SearchFolderDefinition(_) => {
            mapi_mailstore::SpecialMessageSyncFact {
                folder_id: COMMON_VIEWS_FOLDER_ID,
                item_id: 0,
                canonical_id: Uuid::nil(),
                associated: true,
                subject: String::new(),
                body_text: String::new(),
                message_class: "IPM.Microsoft.WunderBar.SFInfo".to_string(),
                last_modified_filetime: 0,
                message_size: 0,
                read_state: None,
                named_properties: Vec::new(),
            }
        }
    }
}

fn common_view_named_view_sync_object(
    message: &crate::mapi_store::MapiCommonViewNamedViewMessage,
    account_id: Uuid,
) -> mapi_mailstore::SpecialMessageSyncFact {
    let mut named_properties = Vec::new();
    for property_tag in [
        PID_TAG_VIEW_DESCRIPTOR_CLSID,
        PID_TAG_VIEW_DESCRIPTOR_FLAGS,
        OUTLOOK_COMMON_VIEW_DESCRIPTOR_BINARY_6835,
        OUTLOOK_COMMON_VIEW_DESCRIPTOR_STRINGS_683C,
        PID_TAG_VIEW_DESCRIPTOR_VERSION,
        PID_TAG_VIEW_DESCRIPTOR_VERSION_CANONICAL,
        PID_TAG_VIEW_DESCRIPTOR_NAME_W,
        PID_TAG_VIEW_DESCRIPTOR_STRINGS_W,
        PID_TAG_VIEW_DESCRIPTOR_FOLDER_TYPE,
        PID_TAG_VIEW_DESCRIPTOR_VIEW_MODE,
        PID_TAG_VIEW_DESCRIPTOR_BINARY,
        PID_TAG_WLINK_GROUP_HEADER_ID,
    ] {
        if let Some(value) =
            common_view_named_view_property_value(message, account_id, property_tag)
                .and_then(special_message_property_value)
        {
            named_properties.push((property_tag, value));
        }
    }
    let change_number = mapi_mailstore::change_number_for_store_id(message.id);

    mapi_mailstore::SpecialMessageSyncFact {
        folder_id: message.folder_id,
        item_id: message.id,
        canonical_id: message.canonical_id,
        associated: true,
        subject: message.name.clone(),
        body_text: String::new(),
        message_class: "IPM.Microsoft.FolderDesign.NamedView".to_string(),
        last_modified_filetime: mapi_mailstore::filetime_from_change_number(change_number),
        message_size: 128,
        read_state: None,
        named_properties,
    }
}

fn conversation_action_sync_object(
    message: &crate::mapi_store::MapiConversationActionMessage,
) -> mapi_mailstore::SpecialMessageSyncFact {
    let mut named_properties = Vec::new();
    for property_tag in [
        PID_TAG_CONVERSATION_INDEX,
        PID_LID_CONVERSATION_ACTION_MOVE_FOLDER_EID_TAG,
        PID_LID_CONVERSATION_ACTION_MOVE_STORE_EID_TAG,
        PID_LID_CONVERSATION_ACTION_MAX_DELIVERY_TIME_TAG,
        PID_LID_CONVERSATION_PROCESSED_TAG,
        PID_LID_CONVERSATION_ACTION_LAST_APPLIED_TIME_TAG,
        PID_LID_CONVERSATION_ACTION_VERSION_TAG,
        PID_NAME_KEYWORDS_TAG,
    ] {
        if let Some(value) = conversation_action_property_value(message, property_tag)
            .and_then(special_message_property_value)
        {
            named_properties.push((property_tag, value));
        }
    }
    let change_number = mapi_mailstore::change_number_for_store_id(message.id);
    let message_size = conversation_action_property_value(message, PID_TAG_MESSAGE_SIZE)
        .and_then(|value| match value {
            MapiValue::I32(value) => Some(value),
            _ => None,
        })
        .unwrap_or(0) as i64;

    mapi_mailstore::SpecialMessageSyncFact {
        folder_id: message.folder_id,
        item_id: message.id,
        canonical_id: message.canonical_id,
        associated: true,
        subject: conversation_action_subject(&message.action),
        body_text: String::new(),
        message_class: "IPM.ConversationAction".to_string(),
        last_modified_filetime: mapi_mailstore::filetime_from_change_number(change_number),
        message_size,
        read_state: None,
        named_properties,
    }
}

fn delegate_freebusy_sync_object(
    message: &crate::mapi_store::MapiDelegateFreeBusyMessage,
) -> mapi_mailstore::SpecialMessageSyncFact {
    let change_number = mapi_mailstore::change_number_for_store_id(message.id);
    let message_size = message
        .message
        .subject
        .len()
        .saturating_add(message.message.body_text.len())
        .saturating_add(message.message.payload_json.len())
        .min(i64::MAX as usize) as i64;

    mapi_mailstore::SpecialMessageSyncFact {
        folder_id: message.folder_id,
        item_id: message.id,
        canonical_id: message.canonical_id,
        associated: true,
        subject: message.message.subject.clone(),
        body_text: message.message.body_text.clone(),
        message_class: if message.message.message_kind == "delegate" {
            "IPM.Microsoft.Delegate".to_string()
        } else {
            "IPM.Microsoft.ScheduleData.FreeBusy".to_string()
        },
        last_modified_filetime: mapi_mailstore::filetime_from_change_number(change_number),
        message_size,
        read_state: None,
        named_properties: Vec::new(),
    }
}

fn associated_config_sync_object(
    message: &crate::mapi_store::MapiAssociatedConfigMessage,
) -> mapi_mailstore::SpecialMessageSyncFact {
    let mut named_properties = Vec::new();
    let stored_properties = mapi_properties_from_json(&message.properties_json);
    for (tag, value) in stored_properties.clone() {
        if associated_config_standard_sync_tag(tag) {
            continue;
        }
        if let Some(value) = special_message_property_value(value) {
            named_properties.push((tag, value));
        }
    }
    for &tag in associated_config_default_sync_tags(message) {
        let canonical_tag = canonical_property_storage_tag(tag);
        if associated_config_standard_sync_tag(canonical_tag)
            || stored_properties.contains_key(&canonical_tag)
        {
            continue;
        }
        if let Some(value) =
            associated_config_property_value(message, tag).and_then(special_message_property_value)
        {
            named_properties.push((tag, value));
        }
    }
    let change_number = mapi_mailstore::change_number_for_store_id(message.id);
    let message_size = message
        .subject
        .len()
        .saturating_add(message.message_class.len())
        .saturating_add(message.properties_json.to_string().len())
        .min(i64::MAX as usize) as i64;

    mapi_mailstore::SpecialMessageSyncFact {
        folder_id: message.folder_id,
        item_id: message.id,
        canonical_id: message.canonical_id,
        associated: true,
        subject: message.subject.clone(),
        body_text: associated_config_text_property(message, PID_TAG_BODY_W),
        message_class: message.message_class.clone(),
        last_modified_filetime: mapi_mailstore::filetime_from_change_number(change_number),
        message_size,
        read_state: None,
        named_properties,
    }
}

fn associated_config_default_sync_tags(
    message: &crate::mapi_store::MapiAssociatedConfigMessage,
) -> &'static [u32] {
    if message.message_class.starts_with("IPM.Configuration.") {
        &[
            PID_TAG_ROAMING_DATATYPES,
            PID_TAG_ROAMING_DICTIONARY,
            OUTLOOK_ASSOCIATED_CONFIG_BINARY_0E0B,
            PID_NAME_CONTENT_CLASS_W_TAG,
            PID_NAME_CONTENT_TYPE_W_TAG,
        ]
    } else if message.message_class == crate::mapi_store::OUTLOOK_INBOX_COMPACT_VIEW_CONFIG_CLASS {
        &[
            PID_TAG_VIEW_DESCRIPTOR_CLSID,
            PID_TAG_VIEW_DESCRIPTOR_FLAGS,
            PID_TAG_VIEW_DESCRIPTOR_VERSION,
            PID_TAG_VIEW_DESCRIPTOR_VERSION_CANONICAL,
            PID_TAG_VIEW_DESCRIPTOR_NAME_W,
            PID_TAG_VIEW_DESCRIPTOR_STRINGS_W,
            PID_TAG_VIEW_DESCRIPTOR_FOLDER_TYPE,
            PID_TAG_VIEW_DESCRIPTOR_VIEW_MODE,
            PID_TAG_VIEW_DESCRIPTOR_BINARY,
            OUTLOOK_ASSOCIATED_CONFIG_BINARY_0E0B,
        ]
    } else {
        &[]
    }
}

fn associated_config_standard_sync_tag(tag: u32) -> bool {
    matches!(
        canonical_property_storage_tag(tag),
        PID_TAG_SOURCE_KEY
            | PID_TAG_PARENT_SOURCE_KEY
            | PID_TAG_CHANGE_KEY
            | PID_TAG_PREDECESSOR_CHANGE_LIST
            | PID_TAG_CHANGE_NUMBER
            | PID_TAG_FOLDER_ID
            | PID_TAG_MID
            | PID_TAG_INST_ID
            | PID_TAG_INSTANCE_NUM
            | PID_TAG_ENTRY_ID
            | PID_TAG_INSTANCE_KEY
            | PID_TAG_ASSOCIATED
            | PID_TAG_MESSAGE_SIZE
            | PID_TAG_MESSAGE_FLAGS
            | PID_TAG_SUBJECT_W
            | PID_TAG_NORMALIZED_SUBJECT_W
            | PID_TAG_MESSAGE_CLASS_W
            | PID_TAG_BODY_W
            | PID_TAG_LAST_MODIFICATION_TIME
            | PID_TAG_LOCAL_COMMIT_TIME
            | PID_TAG_MESSAGE_DELIVERY_TIME
            | PID_TAG_ACCESS
    )
}

fn associated_config_text_property(
    message: &crate::mapi_store::MapiAssociatedConfigMessage,
    tag: u32,
) -> String {
    mapi_properties_from_json(&message.properties_json)
        .remove(&tag)
        .and_then(MapiValue::into_text)
        .unwrap_or_default()
}

fn special_message_property_value(
    value: MapiValue,
) -> Option<mapi_mailstore::SpecialMessagePropertyValue> {
    match value {
        MapiValue::Binary(value) => {
            Some(mapi_mailstore::SpecialMessagePropertyValue::Binary(value))
        }
        MapiValue::Bool(value) => Some(mapi_mailstore::SpecialMessagePropertyValue::Bool(value)),
        MapiValue::Guid(value) => Some(mapi_mailstore::SpecialMessagePropertyValue::Guid(value)),
        MapiValue::I32(value) => Some(mapi_mailstore::SpecialMessagePropertyValue::I32(value)),
        MapiValue::I64(value) => Some(mapi_mailstore::SpecialMessagePropertyValue::I64(value)),
        MapiValue::U32(value) => Some(mapi_mailstore::SpecialMessagePropertyValue::U32(value)),
        MapiValue::U64(value) => Some(mapi_mailstore::SpecialMessagePropertyValue::U64(value)),
        MapiValue::String(value) => {
            Some(mapi_mailstore::SpecialMessagePropertyValue::String(value))
        }
        MapiValue::MultiString(values) => Some(
            mapi_mailstore::SpecialMessagePropertyValue::MultiString(values),
        ),
        _ => None,
    }
}

fn calendar_sync_object(
    event: &crate::mapi_store::MapiEvent,
    reminder: Option<&lpe_storage::ClientReminder>,
) -> mapi_mailstore::SpecialMessageSyncFact {
    let mut properties = Vec::new();
    for property_tag in [
        PID_TAG_START_DATE,
        PID_TAG_END_DATE,
        PID_LID_COMMON_START_TAG,
        PID_LID_COMMON_END_TAG,
        PID_LID_BUSY_STATUS_TAG,
        PID_LID_LOCATION_W_TAG,
        PID_LID_APPOINTMENT_START_WHOLE_TAG,
        PID_LID_APPOINTMENT_END_WHOLE_TAG,
        PID_LID_APPOINTMENT_DURATION_TAG,
        PID_LID_APPOINTMENT_SUB_TYPE_TAG,
        PID_LID_APPOINTMENT_RECUR_TAG,
        PID_LID_APPOINTMENT_STATE_FLAGS_TAG,
        PID_LID_TIME_ZONE_STRUCT_TAG,
        PID_LID_TIME_ZONE_DESCRIPTION_W_TAG,
        PID_LID_APPOINTMENT_TIME_ZONE_DEFINITION_START_DISPLAY_TAG,
        PID_LID_APPOINTMENT_TIME_ZONE_DEFINITION_END_DISPLAY_TAG,
        PID_LID_GLOBAL_OBJECT_ID_TAG,
        PID_LID_CLEAN_GLOBAL_OBJECT_ID_TAG,
        PID_TAG_LOCATION_W,
        PID_TAG_BODY_HTML_W,
        PID_TAG_SENDER_NAME_W,
        PID_TAG_SENDER_EMAIL_ADDRESS_W,
        PID_TAG_DISPLAY_TO_W,
        PID_TAG_DISPLAY_CC_W,
        PID_LID_ALL_ATTENDEES_STRING_W_TAG,
        PID_LID_TO_ATTENDEES_STRING_W_TAG,
        PID_LID_CC_ATTENDEES_STRING_W_TAG,
        PID_TAG_ACCESS,
        PID_TAG_HAS_ATTACHMENTS,
        PID_LID_REMINDER_SET_TAG,
        PID_LID_REMINDER_DELTA_TAG,
        PID_LID_REMINDER_TIME_TAG,
        PID_LID_REMINDER_SIGNAL_TIME_TAG,
        PID_LID_REMINDER_OVERRIDE_TAG,
        PID_LID_REMINDER_PLAY_SOUND_TAG,
        PID_LID_REMINDER_FILE_PARAMETER_W_TAG,
    ] {
        let value = if property_tag == PID_TAG_HAS_ATTACHMENTS {
            Some(mapi_mailstore::SpecialMessagePropertyValue::Bool(
                !event.attachments.is_empty(),
            ))
        } else {
            event_property_value_with_reminder(
                &event.event,
                event.id,
                event.folder_id,
                property_tag,
                reminder,
            )
            .and_then(special_message_property_value)
        };
        if let Some(value) = value {
            properties.push((property_tag, value));
        }
    }

    mapi_mailstore::SpecialMessageSyncFact {
        folder_id: event.folder_id,
        item_id: event.id,
        canonical_id: event.canonical_id,
        associated: false,
        subject: event.event.title.clone(),
        body_text: event.event.notes.clone(),
        message_class: "IPM.Appointment".to_string(),
        last_modified_filetime: event_start_filetime(&event.event),
        message_size: event_size(&event.event),
        read_state: None,
        named_properties: properties,
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
                        embedded_message_blob: None,
                    })
                    .collect(),
            })
        })
        .collect()
}

pub(in crate::mapi) fn fast_transfer_manifest_for_object(
    rop_id: u8,
    object: &MapiObject,
    account_id: Uuid,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
) -> Option<(u64, Vec<u8>)> {
    match object {
        MapiObject::Folder { folder_id, .. } => {
            if RopId::from_u8(rop_id) == Some(RopId::FastTransferSourceCopyFolder) {
                let copy_mailboxes = sync_mailboxes_for_excluding_deleted(
                    *folder_id,
                    0x02,
                    mailboxes,
                    &HashSet::new(),
                );
                let mut attachment_facts = Vec::new();
                for mailbox in &copy_mailboxes {
                    let copied_folder_id = mapi_folder_id(mailbox);
                    let folder_messages =
                        emails_for_folder(copied_folder_id, &copy_mailboxes, emails)
                            .into_iter()
                            .cloned()
                            .collect::<Vec<_>>();
                    attachment_facts.extend(sync_attachment_facts_for(
                        copied_folder_id,
                        &folder_messages,
                        snapshot,
                    ));
                }
                return Some((
                    *folder_id,
                    mapi_mailstore::fast_transfer_top_folder_buffer_with_attachments(
                        *folder_id,
                        &copy_mailboxes,
                        emails,
                        &attachment_facts,
                    ),
                ));
            }
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
            saved_email,
            ..
        } => {
            let message = message_for_id(*folder_id, *message_id, mailboxes, emails)
                .or(saved_email.as_ref().map(|saved| &saved.email))?
                .clone();
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
        MapiObject::AssociatedConfig {
            folder_id,
            config_id,
            saved_message,
        } => {
            let message = snapshot
                .associated_config_message_for_id(*config_id)
                .or_else(|| saved_message.clone())
                .filter(|message| message.folder_id == *folder_id)?;
            Some((
                *folder_id,
                mapi_mailstore::fast_transfer_manifest_buffer_with_special_objects(
                    *folder_id,
                    &[associated_config_sync_object(&message)],
                ),
            ))
        }
        MapiObject::ConversationAction {
            folder_id,
            conversation_action_id,
        } => {
            let message =
                snapshot.conversation_action_table_message_for_id(*conversation_action_id)?;
            if message.folder_id != *folder_id {
                return None;
            }
            Some((
                *folder_id,
                mapi_mailstore::fast_transfer_manifest_buffer_with_special_objects(
                    *folder_id,
                    &[conversation_action_sync_object(&message)],
                ),
            ))
        }
        MapiObject::NavigationShortcut {
            folder_id,
            shortcut_id,
        } => {
            let message = snapshot.navigation_shortcut_message_for_id(*shortcut_id)?;
            if message.folder_id != *folder_id {
                return None;
            }
            Some((
                *folder_id,
                mapi_mailstore::fast_transfer_manifest_buffer_with_special_objects(
                    *folder_id,
                    &[navigation_shortcut_sync_object(&message, account_id)],
                ),
            ))
        }
        MapiObject::CommonViewNamedView { folder_id, view_id } => {
            let message = snapshot.named_view_message_for_folder_and_id(*folder_id, *view_id)?;
            Some((
                *folder_id,
                mapi_mailstore::fast_transfer_manifest_buffer_with_special_objects(
                    *folder_id,
                    &[common_view_named_view_sync_object(&message, account_id)],
                ),
            ))
        }
        MapiObject::DelegateFreeBusyMessage {
            folder_id,
            message_id,
        } => {
            let message = snapshot.delegate_freebusy_message_for_id(*message_id)?;
            if message.folder_id != *folder_id {
                return None;
            }
            Some((
                *folder_id,
                mapi_mailstore::fast_transfer_manifest_buffer_with_special_objects(
                    *folder_id,
                    &[delegate_freebusy_sync_object(&message)],
                ),
            ))
        }
        MapiObject::PublicFolderItem {
            folder_id, item_id, ..
        } => {
            let item = snapshot.public_folder_item_for_id(*folder_id, *item_id)?;
            Some((
                *folder_id,
                mapi_mailstore::fast_transfer_manifest_buffer_with_special_objects(
                    *folder_id,
                    &[public_folder_item_sync_object(&item)],
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
        mapi_item_id_matches(&email.id, message_id)
            && email_matches_folder(email, folder_id, mailboxes)
    })
}

pub(in crate::mapi) fn mapi_item_id_matches(canonical_id: &Uuid, object_id: u64) -> bool {
    crate::mapi::identity::object_id_matches(canonical_id, object_id)
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
    use crate::mapi::rop::RopRequest;

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
            size_octets: 0,
            is_subscribed: true,
        }
    }

    fn assert_associated_fai_core_payload(item: &mapi_mailstore::ContentTransferFaiItemDebug) {
        assert_eq!(item.associated, Some(true));
        assert_has_tags(
            item,
            &[
                PID_TAG_SOURCE_KEY,
                PID_TAG_PARENT_SOURCE_KEY,
                PID_TAG_ENTRY_ID,
                PID_TAG_RECORD_KEY,
                PID_TAG_SEARCH_KEY,
                PID_TAG_CHANGE_KEY,
                PID_TAG_PREDECESSOR_CHANGE_LIST,
                PID_TAG_MESSAGE_CLASS_W,
                PID_TAG_SUBJECT_W,
                PID_TAG_ASSOCIATED,
                PID_TAG_MESSAGE_FLAGS,
                PID_TAG_MESSAGE_SIZE,
                PID_TAG_LAST_MODIFICATION_TIME,
            ],
        );
        assert!(item.source_key_len > 0);
        assert!(item.parent_source_key_len > 0);
        assert!(item.entry_id_len > 0);
        assert!(item.change_number_in_final_cnset_fai);
    }

    fn assert_has_tags(item: &mapi_mailstore::ContentTransferFaiItemDebug, tags: &[u32]) {
        for tag in tags {
            assert!(
                item.property_tags.contains(tag),
                "missing 0x{tag:08x} on {} / {}",
                item.message_class,
                item.subject
            );
        }
    }

    fn associated_content_sync_buffer(
        account_id: Uuid,
        folder_id: u64,
        objects: &[mapi_mailstore::SpecialMessageSyncFact],
    ) -> Vec<u8> {
        mapi_mailstore::sync_manifest_buffer_with_special_objects_and_final_state(
            account_id,
            0x01,
            0x0010,
            0x0000_0001 | 0x0000_0004 | 0x0000_0008,
            &[],
            folder_id,
            &[],
            &[],
            &[],
            objects,
            &[],
            &[],
            &[],
            &[],
            &[],
            objects,
            &[],
            &[],
            1,
        )
    }

    fn assert_fai_boundary_summary(
        buffer: &[u8],
        summary: &mapi_mailstore::ContentTransferFaiDebugSummary,
        expected_count: usize,
    ) {
        assert_eq!(summary.fai_items.len(), expected_count);
        let mut previous_end = 0usize;
        let mut total_item_bytes = 0usize;
        for item in &summary.fai_items {
            assert!(item.item_start_offset >= previous_end);
            assert!(item.item_end_offset > item.item_start_offset);
            assert_eq!(
                item.item_byte_len,
                item.item_end_offset - item.item_start_offset
            );
            assert!(item.item_end_offset <= buffer.len());
            assert!(!item.payload_preview_hex.is_empty());
            assert!(!item.payload_tail_hex.is_empty());
            assert!(item.source_key_len > 0);
            assert!(item.parent_source_key_len > 0);
            assert!(item.entry_id_len > 0);
            assert!(item.record_key_len > 0);
            assert!(item.change_key_len > 0);
            assert!(item.predecessor_change_list_len > 0);
            assert!(item.property_tags.contains(&PID_TAG_SOURCE_KEY));
            assert!(item.property_tags.contains(&PID_TAG_PARENT_SOURCE_KEY));
            assert!(item.property_tags.contains(&PID_TAG_MESSAGE_CLASS_W));
            assert!(item.property_tags.contains(&PID_TAG_SUBJECT_W));
            total_item_bytes += item.item_byte_len;
            previous_end = item.item_end_offset;
        }
        assert!(total_item_bytes > 0);
        assert!(buffer.len() >= total_item_bytes);
    }

    fn persisted_inbox_associated_configs(
        account_id: Uuid,
    ) -> Vec<crate::store::MapiAssociatedConfigRecord> {
        [
            (
                0x6d617069_6163_6350_8000_000000000101,
                crate::mapi::identity::mapi_store_id(0x7900),
                "IPM.Configuration.AccountPrefs",
            ),
            (
                0x6d617069_636f_6e76_8000_000000000101,
                crate::mapi::identity::mapi_store_id(0x7901),
                "IPM.Configuration.ConversationPrefs",
            ),
            (
                0x6d617069_7273_7352_8000_000000000101,
                crate::mapi::identity::mapi_store_id(0x7904),
                "IPM.Configuration.RssRule",
            ),
            (
                0x6d617069_7476_5072_8000_000000000101,
                crate::mapi::identity::mapi_store_id(0x7903),
                "IPM.Configuration.TableViewPreviewPrefs",
            ),
            (
                0x6d617069_7463_5072_8000_000000000101,
                crate::mapi::identity::mapi_store_id(0x7902),
                "IPM.Configuration.TCPrefs",
            ),
            (
                0x6d617069_6578_5275_8000_000000000101,
                crate::mapi::identity::mapi_store_id(0x7905),
                "IPM.ExtendedRule.Message",
            ),
        ]
        .into_iter()
        .map(|(id, item_id, class)| {
            let id = Uuid::from_u128(id);
            crate::mapi::identity::remember_mapi_identity(id, item_id);
            crate::store::MapiAssociatedConfigRecord {
                id,
                account_id,
                folder_id: INBOX_FOLDER_ID,
                message_class: class.to_string(),
                subject: class.to_string(),
                properties_json: serde_json::json!({
                    "0x7c060003": {"type": "u32", "value": 4},
                    "0x7c070102": {"type": "binary", "value": "392d30"}
                }),
            }
        })
        .collect()
    }

    fn persisted_common_views_shortcuts(
        account_id: Uuid,
    ) -> Vec<crate::store::MapiNavigationShortcutRecord> {
        [
            (
                0x6d617069_776c_496e_8000_000000000120,
                crate::mapi::identity::mapi_store_id(0x7800),
                "Inbox",
                INBOX_FOLDER_ID,
                127,
            ),
            (
                0x6d617069_776c_5365_8000_000000000120,
                crate::mapi::identity::mapi_store_id(0x7801),
                "Sent",
                SENT_FOLDER_ID,
                128,
            ),
            (
                0x6d617069_776c_5472_8000_000000000120,
                crate::mapi::identity::mapi_store_id(0x7802),
                "Trash",
                TRASH_FOLDER_ID,
                129,
            ),
            (
                0x6d617069_776c_4361_8000_000000000120,
                crate::mapi::identity::mapi_store_id(0x7803),
                "Calendar",
                CALENDAR_FOLDER_ID,
                130,
            ),
        ]
        .into_iter()
        .map(|(id, item_id, subject, target_folder_id, ordinal)| {
            let id = Uuid::from_u128(id);
            crate::mapi::identity::remember_mapi_identity(id, item_id);
            crate::store::MapiNavigationShortcutRecord {
                id,
                account_id,
                subject: subject.to_string(),
                target_folder_id: Some(target_folder_id),
                shortcut_type: 0,
                flags: 0,
                save_stamp: 0,
                section: 1,
                ordinal,
                group_header_id: Some(crate::mapi::properties::default_wlink_group_uuid()),
                group_name: "Mail".to_string(),
            }
        })
        .collect()
    }

    #[test]
    fn common_views_fai_fasttransfer_boundaries_cover_four_items() {
        let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
        let snapshot = MapiMailStoreSnapshot::empty()
            .with_navigation_shortcuts(persisted_common_views_shortcuts(account_id));
        let objects = special_sync_objects_for(COMMON_VIEWS_FOLDER_ID, 0x01, &snapshot, account_id);
        let buffer = associated_content_sync_buffer(account_id, COMMON_VIEWS_FOLDER_ID, &objects);

        let summary = mapi_mailstore::decode_content_transfer_fai_debug_summary(&buffer).unwrap();

        assert_fai_boundary_summary(&buffer, &summary, 4);
        assert!(summary
            .fai_items
            .iter()
            .all(|item| item.message_class == "IPM.Microsoft.WunderBar.Link"));
        let summary_property_count = summary
            .fai_items
            .iter()
            .map(|item| item.property_tags.len())
            .sum::<usize>();
        assert!(summary_property_count >= summary.fai_items.len());
        for item in &summary.fai_items {
            let item_id = item.item_id.unwrap();
            let special_object = objects.iter().find(|object| object.item_id == item_id);
            assert_eq!(
                mapi_mailstore::fai_debug_state_origin(
                    COMMON_VIEWS_FOLDER_ID,
                    special_object,
                    item_id
                ),
                "sql_associated"
            );
        }
    }

    #[test]
    fn inbox_fai_fasttransfer_boundaries_cover_six_persisted_items() {
        let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
        let snapshot = MapiMailStoreSnapshot::empty()
            .with_associated_configs(persisted_inbox_associated_configs(account_id));
        let objects = special_sync_objects_for(INBOX_FOLDER_ID, 0x01, &snapshot, account_id);
        let buffer = associated_content_sync_buffer(account_id, INBOX_FOLDER_ID, &objects);

        let summary = mapi_mailstore::decode_content_transfer_fai_debug_summary(&buffer).unwrap();

        assert_fai_boundary_summary(&buffer, &summary, 6);
        let summary_property_count = summary
            .fai_items
            .iter()
            .map(|item| item.property_tags.len())
            .sum::<usize>();
        assert!(summary_property_count >= summary.fai_items.len());
        for item in &summary.fai_items {
            let item_id = item.item_id.unwrap();
            let special_object = objects.iter().find(|object| object.item_id == item_id);
            assert_eq!(
                mapi_mailstore::fai_debug_state_origin(INBOX_FOLDER_ID, special_object, item_id),
                "sql_associated"
            );
        }
    }

    #[test]
    fn import_rop_success_responses_return_zero_object_ids() {
        let import_change = RopRequest {
            rop_id: 0x72,
            input_handle_index: Some(1),
            output_handle_index: Some(3),
            payload: Vec::new(),
        };
        let import_hierarchy = RopRequest {
            rop_id: 0x73,
            input_handle_index: Some(2),
            output_handle_index: None,
            payload: Vec::new(),
        };
        let import_move = RopRequest {
            rop_id: 0x78,
            input_handle_index: Some(4),
            output_handle_index: None,
            payload: Vec::new(),
        };

        assert_eq!(
            rop_synchronization_import_message_change_response(&import_change),
            vec![0x72, 0x03, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        );
        assert_eq!(
            rop_synchronization_import_hierarchy_change_response(&import_hierarchy),
            vec![0x73, 0x02, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        );
        assert_eq!(
            rop_synchronization_import_message_move_response(&import_move),
            vec![0x78, 0x04, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        );
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
    fn hierarchy_sync_mailboxes_include_custom_sync_root() {
        let root_id = Uuid::from_u128(0x11111111111111111111111111111112);
        let child_id = Uuid::from_u128(0x22222222222222222222222222222223);
        let root_folder_id = crate::mapi::identity::mapi_store_id(101);
        let child_folder_id = crate::mapi::identity::mapi_store_id(102);
        crate::mapi::identity::remember_mapi_identity(root_id, root_folder_id);
        crate::mapi::identity::remember_mapi_identity(child_id, child_folder_id);
        let root = mailbox(root_id.as_u128(), "custom", "Project");
        let mut child = mailbox(child_id.as_u128(), "custom", "Archive");
        child.parent_id = Some(root_id);
        let rows = sync_mailboxes_for(root_folder_id, 0x02, &[child, root]);
        let row_ids = rows.iter().map(mapi_folder_id).collect::<Vec<_>>();

        assert!(row_ids.contains(&root_folder_id));
        assert!(row_ids.contains(&child_folder_id));
    }

    #[test]
    fn calendar_sync_object_projects_canonical_attachment_presence() {
        let event_id = Uuid::from_u128(0x71717171717141719171717171717171);
        let event = crate::mapi_store::MapiEvent {
            id: crate::mapi::identity::mapi_store_id(123),
            folder_id: CALENDAR_FOLDER_ID,
            canonical_id: event_id,
            event: lpe_storage::AccessibleEvent {
                id: event_id,
                uid: event_id.to_string(),
                collection_id: "default".to_string(),
                owner_account_id: Uuid::nil(),
                owner_email: "alice@example.test".to_string(),
                owner_display_name: "Alice".to_string(),
                rights: lpe_storage::CollaborationRights {
                    may_read: true,
                    may_write: true,
                    may_delete: false,
                    may_share: false,
                },
                date: "2026-05-25".to_string(),
                time: "14:30".to_string(),
                time_zone: "UTC".to_string(),
                duration_minutes: 30,
                all_day: false,
                status: "confirmed".to_string(),
                sequence: 0,
                recurrence_rule: String::new(),
                recurrence_json: "{}".to_string(),
                recurrence_exceptions_json: "[]".to_string(),
                title: "Attachment sync".to_string(),
                location: String::new(),
                organizer_json: "{}".to_string(),
                attendees: String::new(),
                attendees_json: String::new(),
                notes: String::new(),
                body_html: String::new(),
            },
            attachments: vec![crate::mapi_store::MapiAttachment {
                canonical_id: Uuid::from_u128(0x81818181818141819181818181818181),
                attach_num: 0,
                file_reference: "calendar-attachment:ref".to_string(),
                file_name: "agenda.pdf".to_string(),
                media_type: "application/pdf".to_string(),
                disposition: None,
                content_id: None,
                size_octets: 12,
            }],
        };

        let sync = calendar_sync_object(&event, None);

        assert!(sync.named_properties.iter().any(|(tag, value)| {
            *tag == PID_TAG_HAS_ATTACHMENTS
                && matches!(
                    value,
                    mapi_mailstore::SpecialMessagePropertyValue::Bool(true)
                )
        }));
    }

    #[test]
    fn calendar_special_content_sync_advertises_appointment_objects() {
        let account_id = Uuid::from_u128(0xbc737006441349b9aefc3cb6e0088492);
        let event_id = Uuid::from_u128(0xbd6a6c500b7f4fad83d93b9ea082d726);
        crate::mapi::identity::remember_mapi_identity(
            event_id,
            crate::mapi::identity::mapi_store_id(0x43),
        );
        let snapshot = MapiMailStoreSnapshot::new(
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            vec![lpe_storage::AccessibleEvent {
                id: event_id,
                uid: event_id.to_string(),
                collection_id: "default".to_string(),
                owner_account_id: account_id,
                owner_email: "test@l-p-e.ch".to_string(),
                owner_display_name: "test".to_string(),
                rights: lpe_storage::CollaborationRights {
                    may_read: true,
                    may_write: true,
                    may_delete: true,
                    may_share: false,
                },
                date: "2026-06-01".to_string(),
                time: "10:00".to_string(),
                time_zone: String::new(),
                duration_minutes: 0,
                all_day: false,
                status: "confirmed".to_string(),
                sequence: 0,
                recurrence_rule: String::new(),
                recurrence_json: "{}".to_string(),
                recurrence_exceptions_json: "[]".to_string(),
                title: "Test".to_string(),
                location: String::new(),
                organizer_json: "{}".to_string(),
                attendees: String::new(),
                attendees_json: "[]".to_string(),
                notes: String::new(),
                body_html: String::new(),
            }],
            Vec::new(),
            Vec::new(),
        );

        let objects = special_sync_objects_for(CALENDAR_FOLDER_ID, 0x01, &snapshot, account_id);

        assert_eq!(objects.len(), 1);
        assert_eq!(objects[0].message_class, "IPM.Appointment");
        assert_eq!(objects[0].subject, "Test");
    }

    #[test]
    fn hierarchy_sync_mailboxes_deduplicate_outlook_special_roles() {
        let roles = [
            ("suggested_contacts", SUGGESTED_CONTACTS_FOLDER_ID),
            ("sync_issues", SYNC_ISSUES_FOLDER_ID),
            ("conflicts", CONFLICTS_FOLDER_ID),
            ("local_failures", LOCAL_FAILURES_FOLDER_ID),
            ("server_failures", SERVER_FAILURES_FOLDER_ID),
            ("junk", JUNK_FOLDER_ID),
            ("rss_feeds", RSS_FEEDS_FOLDER_ID),
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

    #[test]
    fn hierarchy_scope_places_reminders_under_root_not_ipm_subtree() {
        let mailboxes = vec![mailbox(
            0x44444444444444444444444444444444,
            "reminders",
            "Reminders",
        )];

        assert!(hierarchy_virtual_folder_ids(ROOT_FOLDER_ID).contains(&REMINDERS_FOLDER_ID));
        assert!(!hierarchy_virtual_folder_ids(IPM_SUBTREE_FOLDER_ID).contains(&REMINDERS_FOLDER_ID));
        assert_eq!(
            sync_mailboxes_for(ROOT_FOLDER_ID, 0x02, &mailboxes)
                .iter()
                .filter(|mailbox| mapi_folder_id(mailbox) == REMINDERS_FOLDER_ID)
                .count(),
            1
        );
    }

    #[test]
    fn hierarchy_scope_places_contacts_search_under_search_not_ipm_subtree() {
        let mailboxes = vec![mailbox(
            0x45454545454545454545454545454545,
            "contacts_search",
            "Contacts Search",
        )];

        assert!(hierarchy_virtual_folder_ids(ROOT_FOLDER_ID).contains(&CONTACTS_SEARCH_FOLDER_ID));
        assert!(!hierarchy_virtual_folder_ids(SEARCH_FOLDER_ID).contains(&SEARCH_FOLDER_ID));
        assert!(!hierarchy_virtual_folder_ids(IPM_SUBTREE_FOLDER_ID)
            .contains(&CONTACTS_SEARCH_FOLDER_ID));
        assert_eq!(
            sync_mailboxes_for(SEARCH_FOLDER_ID, 0x02, &[])
                .iter()
                .filter(|mailbox| mapi_folder_id(mailbox) == SEARCH_FOLDER_ID)
                .count(),
            0
        );
        assert_eq!(
            sync_mailboxes_for(SEARCH_FOLDER_ID, 0x02, &mailboxes)
                .iter()
                .filter(|mailbox| mapi_folder_id(mailbox) == CONTACTS_SEARCH_FOLDER_ID)
                .count(),
            1
        );
    }

    #[test]
    fn ipm_hierarchy_runtime_uses_outlook_safe_folder_projection() {
        std::env::set_var("LPE_MAPI_EXPERIMENT_MINIMAL_IPM_HIERARCHY", "1");
        std::env::set_var(
            "LPE_MAPI_EXPERIMENT_IPM_HIERARCHY_GROUPS",
            "minimal sync-issues",
        );
        let folder_ids = hierarchy_virtual_folder_ids(IPM_SUBTREE_FOLDER_ID);
        std::env::remove_var("LPE_MAPI_EXPERIMENT_MINIMAL_IPM_HIERARCHY");
        std::env::remove_var("LPE_MAPI_EXPERIMENT_IPM_HIERARCHY_GROUPS");

        assert_eq!(folder_ids.as_slice(), IPM_SUBTREE_VIRTUAL_FOLDER_IDS);
    }

    #[test]
    fn ipm_hierarchy_state_matches_emitted_folder_projection() {
        let rows = sync_mailboxes_for(IPM_SUBTREE_FOLDER_ID, 0x02, &[]);
        let state_rows = sync_state_mailboxes_for(IPM_SUBTREE_FOLDER_ID, 0x02, &[]);
        let row_ids = rows.iter().map(mapi_folder_id).collect::<Vec<_>>();
        let state_ids = state_rows.iter().map(mapi_folder_id).collect::<Vec<_>>();

        assert_eq!(row_ids.as_slice(), IPM_SUBTREE_VIRTUAL_FOLDER_IDS);
        assert_eq!(state_ids, row_ids);
    }

    #[test]
    fn common_views_shortcut_sync_uses_account_bound_entry_ids() {
        let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
        let shortcut_id = Uuid::from_u128(0x6d617069_776c_496e_8000_000000000002);
        crate::mapi::identity::remember_mapi_identity(
            shortcut_id,
            crate::mapi::identity::mapi_store_id(
                crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 101,
            ),
        );
        let snapshot = MapiMailStoreSnapshot::empty().with_navigation_shortcuts(vec![
            crate::store::MapiNavigationShortcutRecord {
                id: shortcut_id,
                account_id,
                subject: "Inbox".to_string(),
                target_folder_id: Some(INBOX_FOLDER_ID),
                shortcut_type: 0,
                flags: 0,
                save_stamp: 0,
                section: 0,
                ordinal: 0x81,
                group_header_id: Some(crate::mapi::properties::default_wlink_group_uuid()),
                group_name: "Mail".to_string(),
            },
        ]);

        let objects = special_sync_objects_for(COMMON_VIEWS_FOLDER_ID, 0x01, &snapshot, account_id);
        let inbox_shortcut = objects
            .iter()
            .find(|object| object.subject == "Inbox")
            .expect("persisted Inbox navigation shortcut");

        let property = |tag| {
            inbox_shortcut
                .named_properties
                .iter()
                .find_map(|(property_tag, value)| (*property_tag == tag).then_some(value))
                .expect("shortcut property")
        };
        assert_eq!(
            property(PID_TAG_WLINK_ENTRY_ID),
            &crate::mapi_mailstore::SpecialMessagePropertyValue::Binary(
                crate::mapi::identity::folder_entry_id_from_object_id(account_id, INBOX_FOLDER_ID,)
                    .unwrap()
            )
        );
        assert_eq!(
            property(PID_TAG_WLINK_STORE_ENTRY_ID),
            &crate::mapi_mailstore::SpecialMessagePropertyValue::Binary(
                crate::mapi_mailstore::private_store_entry_id(account_id)
            )
        );
        assert_eq!(
            property(PID_TAG_WLINK_FOLDER_TYPE),
            &crate::mapi_mailstore::SpecialMessagePropertyValue::Guid([
                0x0C, 0x78, 0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0xC0, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x46,
            ])
        );
    }

    #[test]
    fn common_views_shortcut_sync_does_not_emit_materialized_mail_header() {
        let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
        let shortcut_id = Uuid::from_u128(0x6d617069_776c_496e_8000_000000000012);
        crate::mapi::identity::remember_mapi_identity(
            shortcut_id,
            crate::mapi::identity::mapi_store_id(
                crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 112,
            ),
        );
        let snapshot = MapiMailStoreSnapshot::empty().with_navigation_shortcuts(vec![
            crate::store::MapiNavigationShortcutRecord {
                id: shortcut_id,
                account_id,
                subject: "Inbox".to_string(),
                target_folder_id: Some(INBOX_FOLDER_ID),
                shortcut_type: 0,
                flags: 0,
                save_stamp: 0,
                section: 1,
                ordinal: 0x81,
                group_header_id: Some(crate::mapi::properties::default_wlink_group_uuid()),
                group_name: "Mail".to_string(),
            },
        ]);

        let objects = special_sync_objects_for(COMMON_VIEWS_FOLDER_ID, 0x01, &snapshot, account_id);

        assert_eq!(
            objects
                .iter()
                .filter(|object| object.message_class == "IPM.Microsoft.WunderBar.Link")
                .count(),
            1
        );
        let default_mail_header_id = crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFE7);
        assert!(objects
            .iter()
            .all(|object| object.item_id != default_mail_header_id));
    }

    #[test]
    fn common_views_group_header_sync_includes_group_identity_without_target() {
        let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
        let shortcut_id = Uuid::from_u128(0x6d617069_776c_4361_8000_000000000101);
        let group_id = Uuid::from_u128(0x5ba943d8_daaa_462c_a63e_9136f65c8681);
        crate::mapi::identity::remember_mapi_identity(
            shortcut_id,
            crate::mapi::identity::mapi_store_id(
                crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 113,
            ),
        );
        let snapshot = MapiMailStoreSnapshot::empty().with_navigation_shortcuts(vec![
            crate::store::MapiNavigationShortcutRecord {
                id: shortcut_id,
                account_id,
                subject: "My Calendars".to_string(),
                target_folder_id: None,
                shortcut_type: 4,
                flags: 0,
                save_stamp: 0,
                section: 1,
                ordinal: 0x80,
                group_header_id: Some(group_id),
                group_name: "My Calendars".to_string(),
            },
        ]);

        let objects = special_sync_objects_for(COMMON_VIEWS_FOLDER_ID, 0x01, &snapshot, account_id);
        let group_header = objects
            .iter()
            .find(|object| object.subject == "My Calendars")
            .expect("persisted My Calendars group header");

        let property = |tag| {
            group_header
                .named_properties
                .iter()
                .find_map(|(property_tag, value)| (*property_tag == tag).then_some(value))
        };
        assert_eq!(
            property(PID_TAG_WLINK_TYPE),
            Some(&crate::mapi_mailstore::SpecialMessagePropertyValue::U32(4))
        );
        assert_eq!(
            property(PID_TAG_WLINK_GROUP_HEADER_ID),
            Some(&crate::mapi_mailstore::SpecialMessagePropertyValue::Guid(
                *group_id.as_bytes()
            ))
        );
        assert_eq!(
            property(PID_TAG_WLINK_GROUP_CLSID),
            Some(&crate::mapi_mailstore::SpecialMessagePropertyValue::Guid(
                *group_id.as_bytes()
            ))
        );
        assert_eq!(
            property(PID_TAG_WLINK_GROUP_NAME_W),
            Some(&crate::mapi_mailstore::SpecialMessagePropertyValue::String(
                "My Calendars".to_string()
            ))
        );
        assert_eq!(property(PID_TAG_WLINK_ENTRY_ID), None);
        assert_eq!(property(PID_TAG_WLINK_RECORD_KEY), None);
        assert_eq!(property(PID_TAG_WLINK_STORE_ENTRY_ID), None);
    }

    #[test]
    fn inbox_associated_content_sync_payload_emits_required_fai_properties() {
        let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
        let umolk_id = Uuid::from_u128(0x6d617069_756d_6f6c_8000_000000000201);
        let named_view_id = Uuid::from_u128(0x6d617069_696e_4e76_8000_000000000201);
        crate::mapi::identity::remember_mapi_identity(
            umolk_id,
            crate::mapi::identity::mapi_store_id(
                crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 201,
            ),
        );
        crate::mapi::identity::remember_mapi_identity(
            named_view_id,
            crate::mapi::identity::mapi_store_id(
                crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 202,
            ),
        );
        let persisted = [
            (
                umolk_id.as_u128(),
                crate::mapi::identity::mapi_store_id(
                    crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 201,
                ),
                "IPM.Configuration.UMOLK.UserOptions",
                "IPM.Configuration.UMOLK.UserOptions",
            ),
            (
                named_view_id.as_u128(),
                crate::mapi::identity::mapi_store_id(
                    crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 202,
                ),
                "IPM.Microsoft.FolderDesign.NamedView",
                "Compact",
            ),
            (
                0x6d617069_6163_6350_8000_000000000101,
                crate::mapi::identity::mapi_store_id(0x7900),
                "IPM.Configuration.AccountPrefs",
                "IPM.Configuration.AccountPrefs",
            ),
            (
                0x6d617069_636f_6e76_8000_000000000101,
                crate::mapi::identity::mapi_store_id(0x7901),
                "IPM.Configuration.ConversationPrefs",
                "IPM.Configuration.ConversationPrefs",
            ),
            (
                0x6d617069_7463_5072_8000_000000000101,
                crate::mapi::identity::mapi_store_id(0x7902),
                "IPM.Configuration.TCPrefs",
                "IPM.Configuration.TCPrefs",
            ),
            (
                0x6d617069_7476_5072_8000_000000000101,
                crate::mapi::identity::mapi_store_id(0x7903),
                "IPM.Configuration.TableViewPreviewPrefs",
                "IPM.Configuration.TableViewPreviewPrefs",
            ),
            (
                0x6d617069_7273_7352_8000_000000000101,
                crate::mapi::identity::mapi_store_id(0x7904),
                "IPM.Configuration.RssRule",
                "IPM.Configuration.RssRule",
            ),
            (
                0x6d617069_6578_5275_8000_000000000101,
                crate::mapi::identity::mapi_store_id(0x7905),
                "IPM.ExtendedRule.Message",
                "IPM.ExtendedRule.Message",
            ),
        ]
        .into_iter()
        .map(|(id, item_id, class, subject)| {
            let id = Uuid::from_u128(id);
            crate::mapi::identity::remember_mapi_identity(id, item_id);
            crate::store::MapiAssociatedConfigRecord {
                id,
                account_id,
                folder_id: INBOX_FOLDER_ID,
                message_class: class.to_string(),
                subject: subject.to_string(),
                properties_json: serde_json::json!({
                    "0x7c060003": {"type": "u32", "value": 4},
                    "0x7c070102": {"type": "binary", "value": "392d30"}
                }),
            }
        })
        .collect::<Vec<_>>();
        let snapshot = MapiMailStoreSnapshot::empty().with_associated_configs(persisted);
        let objects = special_sync_objects_for(INBOX_FOLDER_ID, 0x01, &snapshot, account_id);
        let buffer = mapi_mailstore::sync_manifest_buffer_with_special_objects_and_final_state(
            account_id,
            0x01,
            0x0010,
            0x0000_0001 | 0x0000_0004 | 0x0000_0008,
            &[],
            INBOX_FOLDER_ID,
            &[],
            &[],
            &[],
            &objects,
            &[],
            &[],
            &[],
            &[],
            &[],
            &objects,
            &[],
            &[],
            1,
        );
        let summary = mapi_mailstore::decode_content_transfer_fai_debug_summary(&buffer).unwrap();

        assert!(summary.fai_items.len() >= 8);
        for item in &summary.fai_items {
            assert_associated_fai_core_payload(item);
        }
        let expected = [
            (
                crate::mapi::identity::mapi_store_id(
                    crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 202,
                ),
                "IPM.Microsoft.FolderDesign.NamedView",
                "Compact",
            ),
            (
                crate::mapi::identity::mapi_store_id(
                    crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 201,
                ),
                "IPM.Configuration.UMOLK.UserOptions",
                "IPM.Configuration.UMOLK.UserOptions",
            ),
        ];
        for (item_id, message_class, subject) in expected {
            let item = summary
                .fai_items
                .iter()
                .find(|item| item.item_id == Some(item_id))
                .expect("expected Inbox FAI item");
            assert_eq!(item.message_class, message_class);
            assert_eq!(item.subject, subject);
        }
        assert!(!summary.fai_items.iter().any(|item| {
            item.item_id == Some(crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFF6))
                || item.item_id == Some(crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFFA))
        }));
        let named_view = summary
            .fai_items
            .iter()
            .find(|item| item.message_class == "IPM.Microsoft.FolderDesign.NamedView")
            .expect("persisted Inbox named view");
        assert_has_tags(
            named_view,
            &[
                PID_TAG_VIEW_DESCRIPTOR_NAME_W,
                PID_TAG_VIEW_DESCRIPTOR_VIEW_MODE,
                PID_TAG_VIEW_DESCRIPTOR_BINARY,
            ],
        );
        let umolk = summary
            .fai_items
            .iter()
            .find(|item| item.message_class == "IPM.Configuration.UMOLK.UserOptions")
            .expect("persisted UMOLK user options");
        assert_has_tags(
            umolk,
            &[
                PID_TAG_ROAMING_DATATYPES,
                PID_TAG_ROAMING_DICTIONARY,
                OUTLOOK_ASSOCIATED_CONFIG_BINARY_0E0B,
            ],
        );
        for class in [
            "IPM.Configuration.AccountPrefs",
            "IPM.Configuration.ConversationPrefs",
            "IPM.Configuration.TCPrefs",
            "IPM.Configuration.TableViewPreviewPrefs",
            "IPM.Configuration.RssRule",
            "IPM.ExtendedRule.Message",
        ] {
            assert!(
                summary
                    .fai_items
                    .iter()
                    .any(|item| item.message_class == class),
                "missing persisted class {class}"
            );
        }
    }

    #[test]
    fn common_views_associated_content_sync_payload_emits_view_and_wunderbar_properties() {
        let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
        let shortcut_id = Uuid::from_u128(0x6d617069_776c_496e_8000_000000000120);
        crate::mapi::identity::remember_mapi_identity(
            shortcut_id,
            crate::mapi::identity::mapi_store_id(
                crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER + 120,
            ),
        );
        let snapshot = MapiMailStoreSnapshot::empty().with_navigation_shortcuts(vec![
            crate::store::MapiNavigationShortcutRecord {
                id: shortcut_id,
                account_id,
                subject: "Inbox".to_string(),
                target_folder_id: Some(INBOX_FOLDER_ID),
                shortcut_type: 0,
                flags: 0,
                save_stamp: 0,
                section: 1,
                ordinal: 127,
                group_header_id: Some(crate::mapi::properties::default_wlink_group_uuid()),
                group_name: "Mail".to_string(),
            },
        ]);
        let mut objects =
            special_sync_objects_for(COMMON_VIEWS_FOLDER_ID, 0x01, &snapshot, account_id);
        objects.push(common_view_named_view_sync_object(
            &crate::mapi_store::MapiCommonViewNamedViewMessage {
                id: crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFF7),
                folder_id: COMMON_VIEWS_FOLDER_ID,
                canonical_id: Uuid::from_u128(0x6d617069_6376_4e76_8000_000000000001),
                name: "Compact".to_string(),
                view_flags: 14_745_605,
                view_type: 8,
            },
            account_id,
        ));
        let buffer = mapi_mailstore::sync_manifest_buffer_with_special_objects_and_final_state(
            account_id,
            0x01,
            0x0010,
            0x0000_0001 | 0x0000_0004 | 0x0000_0008,
            &[],
            COMMON_VIEWS_FOLDER_ID,
            &[],
            &[],
            &[],
            &objects,
            &[],
            &[],
            &[],
            &[],
            &[],
            &objects,
            &[],
            &[],
            1,
        );
        let summary = mapi_mailstore::decode_content_transfer_fai_debug_summary(&buffer).unwrap();

        assert!(!summary.fai_items.is_empty());
        for item in &summary.fai_items {
            assert_associated_fai_core_payload(item);
        }
        let named_view = summary
            .fai_items
            .iter()
            .find(|item| {
                item.message_class == "IPM.Microsoft.FolderDesign.NamedView"
                    && item.subject == "Compact"
            })
            .expect("Compact named view");
        assert_has_tags(
            named_view,
            &[
                PID_TAG_VIEW_DESCRIPTOR_NAME_W,
                PID_TAG_VIEW_DESCRIPTOR_VIEW_MODE,
                PID_TAG_VIEW_DESCRIPTOR_BINARY,
                PID_TAG_WLINK_GROUP_HEADER_ID,
            ],
        );
        let shortcut = summary
            .fai_items
            .iter()
            .find(|item| {
                item.message_class == "IPM.Microsoft.WunderBar.Link" && item.subject == "Inbox"
            })
            .expect("Inbox WunderBar shortcut");
        assert_has_tags(
            shortcut,
            &[
                PID_TAG_WLINK_TYPE,
                PID_TAG_WLINK_FLAGS,
                PID_TAG_WLINK_SAVE_STAMP,
                PID_TAG_WLINK_ENTRY_ID,
                PID_TAG_WLINK_RECORD_KEY,
                PID_TAG_WLINK_STORE_ENTRY_ID,
                PID_TAG_WLINK_GROUP_CLSID,
                PID_TAG_WLINK_SECTION,
                PID_TAG_WLINK_ORDINAL,
            ],
        );
    }

    #[test]
    fn fast_transfer_manifest_rejects_unbacked_common_views_shortcut() {
        let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
        let shortcut_id = crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFF9);
        let object = MapiObject::NavigationShortcut {
            folder_id: COMMON_VIEWS_FOLDER_ID,
            shortcut_id,
        };

        assert!(fast_transfer_manifest_for_object(
            RopId::FastTransferSourceCopyTo.as_u8(),
            &object,
            account_id,
            &[],
            &[],
            &MapiMailStoreSnapshot::empty(),
        )
        .is_none());
    }

    #[test]
    fn fast_transfer_manifest_exports_default_common_views_named_view() {
        let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
        let view_id = crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFF7);
        let object = MapiObject::CommonViewNamedView {
            folder_id: COMMON_VIEWS_FOLDER_ID,
            view_id,
        };

        let manifest = fast_transfer_manifest_for_object(
            RopId::FastTransferSourceCopyTo.as_u8(),
            &object,
            account_id,
            &[],
            &[],
            &MapiMailStoreSnapshot::empty(),
        )
        .expect("default common views named view manifest");

        assert_eq!(manifest.0, COMMON_VIEWS_FOLDER_ID);
        let utf16z = |value: &str| {
            let mut bytes = value
                .encode_utf16()
                .flat_map(u16::to_le_bytes)
                .collect::<Vec<_>>();
            bytes.extend_from_slice(&[0, 0]);
            bytes
        };
        let named_view_class = utf16z("IPM.Microsoft.FolderDesign.NamedView");
        let compact_name = utf16z("Compact");
        assert!(manifest
            .1
            .windows(named_view_class.len())
            .any(|window| window == named_view_class.as_slice()));
        assert!(manifest
            .1
            .windows(compact_name.len())
            .any(|window| window == compact_name.as_slice()));
    }

    #[test]
    fn common_view_named_view_sync_projects_canonical_descriptor_properties() {
        let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
        let message = crate::mapi_store::MapiCommonViewNamedViewMessage {
            id: crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFF7),
            folder_id: COMMON_VIEWS_FOLDER_ID,
            canonical_id: Uuid::from_u128(0x6d617069_6376_4e76_8000_000000000001),
            name: "Compact".to_string(),
            view_flags: 14_745_605,
            view_type: 8,
        };

        let sync = common_view_named_view_sync_object(&message, account_id);
        let property = |tag| {
            sync.named_properties
                .iter()
                .find_map(|(property_tag, value)| (*property_tag == tag).then_some(value))
                .expect("sync property")
        };

        assert_eq!(
            property(PID_TAG_VIEW_DESCRIPTOR_VERSION_CANONICAL),
            &mapi_mailstore::SpecialMessagePropertyValue::U32(8)
        );
        assert!(matches!(
            property(PID_TAG_VIEW_DESCRIPTOR_BINARY),
            mapi_mailstore::SpecialMessagePropertyValue::Binary(value) if !value.is_empty()
        ));
        assert!(matches!(
            property(PID_TAG_VIEW_DESCRIPTOR_STRINGS_W),
            mapi_mailstore::SpecialMessagePropertyValue::String(value)
                if value.contains("From") && value.contains("Received")
        ));
        assert!(matches!(
            property(OUTLOOK_COMMON_VIEW_DESCRIPTOR_STRINGS_683C),
            mapi_mailstore::SpecialMessagePropertyValue::Binary(value)
                if !value.is_empty() && value.starts_with(&[0x0a, 0x00])
        ));
    }

    #[test]
    fn fast_transfer_manifest_rejects_associated_config_default_from_wrong_folder() {
        let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
        let object = MapiObject::AssociatedConfig {
            folder_id: QUICK_STEP_SETTINGS_FOLDER_ID,
            config_id: crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFFC),
            saved_message: None,
        };

        let manifest = fast_transfer_manifest_for_object(
            RopId::FastTransferSourceCopyTo.as_u8(),
            &object,
            account_id,
            &[],
            &[],
            &MapiMailStoreSnapshot::empty(),
        );

        assert!(manifest.is_none());
    }

    #[test]
    fn fast_transfer_manifest_rejects_common_views_shortcut_from_wrong_folder() {
        let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
        let object = MapiObject::NavigationShortcut {
            folder_id: INBOX_FOLDER_ID,
            shortcut_id: crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFF9),
        };

        let manifest = fast_transfer_manifest_for_object(
            RopId::FastTransferSourceCopyTo.as_u8(),
            &object,
            account_id,
            &[],
            &[],
            &MapiMailStoreSnapshot::empty(),
        );

        assert!(manifest.is_none());
    }

    #[test]
    fn fast_transfer_manifest_rejects_common_views_named_view_from_wrong_folder() {
        let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
        let object = MapiObject::CommonViewNamedView {
            folder_id: INBOX_FOLDER_ID,
            view_id: crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFF7),
        };

        let manifest = fast_transfer_manifest_for_object(
            RopId::FastTransferSourceCopyTo.as_u8(),
            &object,
            account_id,
            &[],
            &[],
            &MapiMailStoreSnapshot::empty(),
        );

        assert!(manifest.is_none());
    }

    #[test]
    fn fast_transfer_manifest_rejects_conversation_action_default_from_wrong_folder() {
        let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
        let object = MapiObject::ConversationAction {
            folder_id: COMMON_VIEWS_FOLDER_ID,
            conversation_action_id: crate::mapi::identity::mapi_store_id(0x7FFF_FFFF_FFF2),
        };

        let manifest = fast_transfer_manifest_for_object(
            RopId::FastTransferSourceCopyTo.as_u8(),
            &object,
            account_id,
            &[],
            &[],
            &MapiMailStoreSnapshot::empty(),
        );

        assert!(manifest.is_none());
    }

    #[test]
    fn fast_transfer_manifest_rejects_delegate_freebusy_from_wrong_folder() {
        let account_id = Uuid::from_u128(0xea33944627b94a9cb0de873f03a35376);
        let message_id = Uuid::parse_str("56565656-5656-4656-8656-565656565656").unwrap();
        crate::mapi::identity::remember_mapi_identity(
            message_id,
            crate::mapi::identity::mapi_store_id(610),
        );
        let snapshot = MapiMailStoreSnapshot::empty().with_delegate_freebusy_messages(vec![
            lpe_storage::DelegateFreeBusyMessageObject {
                id: message_id,
                account_id,
                owner_account_id: Uuid::nil(),
                owner_email: "owner@example.test".to_string(),
                message_kind: "freebusy".to_string(),
                subject: "owner@example.test: busy".to_string(),
                body_text: "busy".to_string(),
                starts_at: None,
                ends_at: None,
                busy_status: None,
                payload_json: "{}".to_string(),
                updated_at: "2026-05-26T08:00:00Z".to_string(),
            },
        ]);
        let object = MapiObject::DelegateFreeBusyMessage {
            folder_id: INBOX_FOLDER_ID,
            message_id: snapshot.delegate_freebusy_messages()[0].id,
        };

        let manifest = fast_transfer_manifest_for_object(
            RopId::FastTransferSourceCopyTo.as_u8(),
            &object,
            account_id,
            &[],
            &[],
            &snapshot,
        );

        assert!(manifest.is_none());
    }
}
