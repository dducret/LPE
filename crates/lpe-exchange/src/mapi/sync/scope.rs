use super::*;

const ROOT_VIRTUAL_FOLDER_IDS: [u64; 33] = [
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

pub(in crate::mapi) const IPM_SUBTREE_VIRTUAL_FOLDER_IDS: [u64; 19] = [
    IPM_SUBTREE_FOLDER_ID,
    INBOX_FOLDER_ID,
    DRAFTS_FOLDER_ID,
    OUTBOX_FOLDER_ID,
    SENT_FOLDER_ID,
    TRASH_FOLDER_ID,
    CONTACTS_FOLDER_ID,
    SUGGESTED_CONTACTS_FOLDER_ID,
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

pub(in crate::mapi) fn hierarchy_virtual_folder_ids(sync_root_folder_id: u64) -> Vec<u64> {
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
