use super::*;

pub(crate) fn mapi_folder_id_for_mailbox(mailbox: &JmapMailbox, fallback: u64) -> u64 {
    match mailbox.role.as_str() {
        "__mapi_ipm_subtree" => crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
        "__mapi_deferred_action" => crate::mapi::identity::DEFERRED_ACTION_FOLDER_ID,
        "__mapi_spooler_queue" => crate::mapi::identity::SPOOLER_QUEUE_FOLDER_ID,
        "inbox" => crate::mapi::identity::INBOX_FOLDER_ID,
        "drafts" => crate::mapi::identity::DRAFTS_FOLDER_ID,
        "outbox" => crate::mapi::identity::OUTBOX_FOLDER_ID,
        "sent" => crate::mapi::identity::SENT_FOLDER_ID,
        "trash" => crate::mapi::identity::TRASH_FOLDER_ID,
        "__mapi_common_views" => crate::mapi::identity::COMMON_VIEWS_FOLDER_ID,
        "__mapi_schedule" => crate::mapi::identity::SCHEDULE_FOLDER_ID,
        "__mapi_search" => crate::mapi::identity::SEARCH_FOLDER_ID,
        "__mapi_views" => crate::mapi::identity::VIEWS_FOLDER_ID,
        "__mapi_shortcuts" => crate::mapi::identity::SHORTCUTS_FOLDER_ID,
        "contacts" => crate::mapi::identity::CONTACTS_FOLDER_ID,
        "calendar" => crate::mapi::identity::CALENDAR_FOLDER_ID,
        "journal" => crate::mapi::identity::JOURNAL_FOLDER_ID,
        "notes" => crate::mapi::identity::NOTES_FOLDER_ID,
        "tasks" => crate::mapi::identity::TASKS_FOLDER_ID,
        "reminders" => crate::mapi::identity::REMINDERS_FOLDER_ID,
        "suggested_contacts" => crate::mapi::identity::SUGGESTED_CONTACTS_FOLDER_ID,
        "quick_contacts" => crate::mapi::identity::QUICK_CONTACTS_FOLDER_ID,
        "im_contact_list" => crate::mapi::identity::IM_CONTACT_LIST_FOLDER_ID,
        "contacts_search" => crate::mapi::identity::CONTACTS_SEARCH_FOLDER_ID,
        "document_libraries" => crate::mapi::identity::DOCUMENT_LIBRARIES_FOLDER_ID,
        "sync_issues" => crate::mapi::identity::SYNC_ISSUES_FOLDER_ID,
        "conflicts" => crate::mapi::identity::CONFLICTS_FOLDER_ID,
        "local_failures" => crate::mapi::identity::LOCAL_FAILURES_FOLDER_ID,
        "server_failures" => crate::mapi::identity::SERVER_FAILURES_FOLDER_ID,
        "junk" => crate::mapi::identity::JUNK_FOLDER_ID,
        "rss_feeds" => crate::mapi::identity::RSS_FEEDS_FOLDER_ID,
        "tracked_mail_processing" => crate::mapi::identity::TRACKED_MAIL_PROCESSING_FOLDER_ID,
        "todo_search" => crate::mapi::identity::TODO_SEARCH_FOLDER_ID,
        "conversation_action_settings" => {
            crate::mapi::identity::CONVERSATION_ACTION_SETTINGS_FOLDER_ID
        }
        "quick_step_settings" => crate::mapi::identity::QUICK_STEP_SETTINGS_FOLDER_ID,
        "archive" => crate::mapi::identity::ARCHIVE_FOLDER_ID,
        "conversation_history" => crate::mapi::identity::CONVERSATION_HISTORY_FOLDER_ID,
        _ => crate::mapi::identity::mapped_mapi_object_id(&mailbox.id).unwrap_or(fallback),
    }
}

pub(crate) fn mapi_folder_parent_id_for_mailbox(
    mailbox: &JmapMailbox,
    mailboxes: &[JmapMailbox],
) -> u64 {
    if let Some((_, _, _, parent_folder_id, _)) =
        virtual_special_folder_metadata(mapi_folder_id_for_mailbox(mailbox, 0))
    {
        return parent_folder_id;
    }

    match mailbox.role.as_str() {
        "__mapi_ipm_subtree"
        | "__mapi_deferred_action"
        | "__mapi_spooler_queue"
        | "__mapi_common_views"
        | "__mapi_schedule"
        | "__mapi_search"
        | "__mapi_views"
        | "__mapi_shortcuts"
        | "__mapi_freebusy_data" => crate::mapi::identity::ROOT_FOLDER_ID,
        "journal"
        | "notes"
        | "tasks"
        | "__mapi_collaboration_calendar"
        | "suggested_contacts"
        | "quick_contacts"
        | "im_contact_list"
        | "contacts_search"
        | "document_libraries"
        | "sync_issues"
        | "junk"
        | "rss_feeds"
        | "tracked_mail_processing"
        | "todo_search"
        | "conversation_action_settings"
        | "quick_step_settings"
        | "archive"
        | "conversation_history" => crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
        "conflicts" | "local_failures" | "server_failures" => {
            crate::mapi::identity::SYNC_ISSUES_FOLDER_ID
        }
        _ => mailbox
            .parent_id
            .and_then(|parent_id| mailboxes.iter().find(|candidate| candidate.id == parent_id))
            .map(|parent| {
                let fallback = crate::mapi::identity::mapped_mapi_object_id(&parent.id)
                    .unwrap_or(crate::mapi::identity::IPM_SUBTREE_FOLDER_ID);
                mapi_folder_id_for_mailbox(parent, fallback)
            })
            .unwrap_or(crate::mapi::identity::IPM_SUBTREE_FOLDER_ID),
    }
}

pub(crate) fn hierarchy_entry_id_mailbox_guid(
    mailbox: &JmapMailbox,
    fallback_mailbox_guid: Uuid,
) -> Uuid {
    if mailbox.role == "__mapi_collaboration_calendar" {
        return mailbox.parent_id.unwrap_or(fallback_mailbox_guid);
    }
    fallback_mailbox_guid
}

pub(crate) fn mapi_folder_message_class(mailbox: &JmapMailbox) -> &'static str {
    virtual_special_folder_metadata(mapi_folder_id_for_mailbox(mailbox, 0))
        .map(|(_, _, _, _, message_class)| message_class)
        .unwrap_or(match mailbox.role.as_str() {
            "contacts" => "IPF.Contact",
            "calendar" => "IPF.Appointment",
            "__mapi_collaboration_calendar" => "IPF.Appointment",
            "journal" => "IPF.Journal",
            "notes" => "IPF.StickyNote",
            "tasks" => "IPF.Task",
            "reminders" => "Outlook.Reminder",
            "suggested_contacts" => "IPF.Contact",
            "quick_contacts" => "IPF.Contact.MOC.QuickContacts",
            "im_contact_list" => "IPF.Contact.MOC.ImContactList",
            "contacts_search" => "IPF.Contact",
            "__mapi_search_folder_contact" => "IPF.Contact",
            "__mapi_search_folder_task" => "IPF.Task",
            "__mapi_search_folder_mixed" | "__mapi_search_folder_message" => "IPF.Note",
            "document_libraries" => "IPF.ShortcutFolder",
            "rss_feeds" => "IPF.Note.OutlookHomepage",
            "todo_search" => "IPF.Task",
            "conversation_action_settings" => "IPF.Configuration",
            _ => "IPF.Note",
        })
}

pub(crate) fn mapi_folder_display_name(mailbox: &JmapMailbox) -> &str {
    if mailbox.role == "conversation_history" {
        return "Conversation History";
    }
    virtual_special_folder_metadata(mapi_folder_id_for_mailbox(mailbox, 0))
        .map(|(_, name, _, _, _)| name)
        .unwrap_or(&mailbox.name)
}

pub(crate) fn mapi_folder_has_subfolders(mailbox: &JmapMailbox, mailboxes: &[JmapMailbox]) -> bool {
    let folder_id = mapi_folder_id_for_mailbox(mailbox, 0);
    if matches!(
        folder_id,
        crate::mapi::identity::ROOT_FOLDER_ID
            | crate::mapi::identity::IPM_SUBTREE_FOLDER_ID
            | crate::mapi::identity::SYNC_ISSUES_FOLDER_ID
            | crate::mapi::identity::RECOVERABLE_ITEMS_ROOT_FOLDER_ID
    ) {
        return true;
    }
    mailboxes
        .iter()
        .any(|candidate| mapi_folder_parent_id_for_mailbox(candidate, mailboxes) == folder_id)
}

pub(crate) fn folder_content_counts(
    folder_id: u64,
    mailbox: &JmapMailbox,
    mailboxes: &[JmapMailbox],
    aggregate_emails: &[JmapEmail],
) -> (i32, i32, &'static str) {
    if aggregate_emails.is_empty() {
        return (
            mailbox.total_emails.min(i32::MAX as u32) as i32,
            mailbox.unread_emails.min(i32::MAX as u32) as i32,
            "mailbox",
        );
    }

    if mailbox.total_emails > 0
        && matches!(
            mailbox.role.as_str(),
            "contacts" | "calendar" | "tasks" | "journal" | "notes" | "suggested_contacts"
        )
    {
        return (
            mailbox.total_emails.min(i32::MAX as u32) as i32,
            mailbox.unread_emails.min(i32::MAX as u32) as i32,
            "collaboration",
        );
    }

    let mut total = 0u32;
    let mut unread = 0u32;
    for unread_in_folder in aggregate_emails
        .iter()
        .filter_map(|email| email_unread_in_manifest_folder(email, folder_id, mailboxes))
    {
        total = total.saturating_add(1);
        if unread_in_folder {
            unread = unread.saturating_add(1);
        }
    }

    (
        total.min(i32::MAX as u32) as i32,
        unread.min(i32::MAX as u32) as i32,
        "snapshot",
    )
}

pub(crate) fn email_unread_in_manifest_folder(
    email: &JmapEmail,
    folder_id: u64,
    mailboxes: &[JmapMailbox],
) -> Option<bool> {
    if let Some((role, _, _, _, _)) = virtual_special_folder_metadata(folder_id) {
        if role.starts_with("__mapi_") {
            return None;
        }
        return email
            .mailbox_states
            .iter()
            .find(|state| state.role == role)
            .map(|state| state.unread)
            .or_else(|| (email.mailbox_role == role).then_some(email.unread));
    }

    mailboxes
        .iter()
        .find(|mailbox| {
            let Some(mapped_folder_id) = crate::mapi::identity::mapped_mapi_object_id(&mailbox.id)
            else {
                return false;
            };
            mapi_folder_id_for_mailbox(mailbox, mapped_folder_id) == folder_id
        })
        .and_then(|mailbox| {
            email
                .mailbox_states
                .iter()
                .find(|state| state.mailbox_id == mailbox.id)
                .map(|state| state.unread)
                .or_else(|| (email.mailbox_id == mailbox.id).then_some(email.unread))
        })
}

pub(crate) fn hierarchy_sort_depth(
    sync_type: u8,
    sync_root_folder_id: u64,
    mailbox: &JmapMailbox,
    mailboxes: &[JmapMailbox],
) -> u8 {
    if sync_type != SYNC_TYPE_HIERARCHY {
        return 0;
    }
    if mapi_folder_id_for_mailbox(mailbox, 0) == sync_root_folder_id {
        return 0;
    }
    let mut parent_folder_id = mapi_folder_parent_id_for_mailbox(mailbox, mailboxes);
    if parent_folder_id == sync_root_folder_id {
        return 1;
    }

    let mut depth = 2u8;
    let mut visited = BTreeSet::new();
    while parent_folder_id != 0 && visited.insert(parent_folder_id) {
        let Some(next_parent_folder_id) =
            mapi_parent_folder_id_for_folder_id(parent_folder_id, mailboxes)
        else {
            break;
        };
        if next_parent_folder_id == sync_root_folder_id {
            break;
        }
        parent_folder_id = next_parent_folder_id;
        depth = depth.saturating_add(1);
    }
    depth
}

pub(crate) fn mapi_parent_folder_id_for_folder_id(
    folder_id: u64,
    mailboxes: &[JmapMailbox],
) -> Option<u64> {
    if folder_id == crate::mapi::identity::ROOT_FOLDER_ID {
        return None;
    }
    if let Some((_, _, _, parent_folder_id, _)) = virtual_special_folder_metadata(folder_id) {
        return Some(parent_folder_id);
    }
    mailboxes
        .iter()
        .find(|mailbox| {
            let fallback = crate::mapi::identity::mapped_mapi_object_id(&mailbox.id)
                .unwrap_or(crate::mapi::identity::IPM_SUBTREE_FOLDER_ID);
            mapi_folder_id_for_mailbox(mailbox, fallback) == folder_id
        })
        .map(|mailbox| mapi_folder_parent_id_for_mailbox(mailbox, mailboxes))
}

pub(crate) fn hierarchy_folder_sort_order(mailbox: &JmapMailbox) -> i32 {
    virtual_special_folder_metadata(mapi_folder_id_for_mailbox(mailbox, 0))
        .map(|(_, _, sort_order, _, _)| sort_order)
        .unwrap_or(i32::MAX)
}

pub(crate) fn virtual_special_mailbox_id(folder_id: u64) -> Uuid {
    Uuid::from_u128(VIRTUAL_SPECIAL_MAILBOX_UUID_PREFIX | u128::from(folder_id))
}

pub(crate) fn virtual_special_folder_metadata(
    folder_id: u64,
) -> Option<(&'static str, &'static str, i32, u64, &'static str)> {
    match folder_id {
        crate::mapi::identity::IPM_SUBTREE_FOLDER_ID => Some((
            "__mapi_ipm_subtree",
            "Top of Information Store",
            0,
            crate::mapi::identity::ROOT_FOLDER_ID,
            "IPF.Note",
        )),
        crate::mapi::identity::DEFERRED_ACTION_FOLDER_ID => Some((
            "__mapi_deferred_action",
            "Deferred Action",
            1,
            crate::mapi::identity::ROOT_FOLDER_ID,
            "",
        )),
        crate::mapi::identity::SPOOLER_QUEUE_FOLDER_ID => Some((
            "__mapi_spooler_queue",
            "Spooler Queue",
            2,
            crate::mapi::identity::ROOT_FOLDER_ID,
            "",
        )),
        crate::mapi::identity::INBOX_FOLDER_ID => Some((
            "inbox",
            "Inbox",
            20,
            crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
            "IPF.Note",
        )),
        crate::mapi::identity::DRAFTS_FOLDER_ID => Some((
            "drafts",
            "Drafts",
            25,
            crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
            "IPF.Note",
        )),
        crate::mapi::identity::OUTBOX_FOLDER_ID => Some((
            "outbox",
            "Outbox",
            30,
            crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
            "IPF.Note",
        )),
        crate::mapi::identity::SENT_FOLDER_ID => Some((
            "sent",
            "Sent Items",
            40,
            crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
            "IPF.Note",
        )),
        crate::mapi::identity::TRASH_FOLDER_ID => Some((
            "trash",
            "Deleted Items",
            50,
            crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
            "IPF.Note",
        )),
        crate::mapi::identity::CONTACTS_FOLDER_ID => Some((
            "contacts",
            "Contacts",
            55,
            crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
            "IPF.Contact",
        )),
        crate::mapi::identity::CALENDAR_FOLDER_ID => Some((
            "calendar",
            "Calendar",
            57,
            crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
            "IPF.Appointment",
        )),
        crate::mapi::identity::JOURNAL_FOLDER_ID => Some((
            "journal",
            "Journal",
            58,
            crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
            "IPF.Journal",
        )),
        crate::mapi::identity::NOTES_FOLDER_ID => Some((
            "notes",
            "Notes",
            59,
            crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
            "IPF.StickyNote",
        )),
        crate::mapi::identity::TASKS_FOLDER_ID => Some((
            "tasks",
            "Tasks",
            60,
            crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
            "IPF.Task",
        )),
        crate::mapi::identity::REMINDERS_FOLDER_ID => Some((
            "reminders",
            "Reminders",
            61,
            crate::mapi::identity::ROOT_FOLDER_ID,
            "Outlook.Reminder",
        )),
        crate::mapi::identity::SUGGESTED_CONTACTS_FOLDER_ID => Some((
            "suggested_contacts",
            "Suggested Contacts",
            62,
            crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
            "IPF.Contact",
        )),
        crate::mapi::identity::QUICK_CONTACTS_FOLDER_ID => Some((
            "quick_contacts",
            "Quick Contacts",
            63,
            crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
            "IPF.Contact.MOC.QuickContacts",
        )),
        crate::mapi::identity::IM_CONTACT_LIST_FOLDER_ID => Some((
            "im_contact_list",
            "IM Contact List",
            64,
            crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
            "IPF.Contact.MOC.ImContactList",
        )),
        crate::mapi::identity::CONTACTS_SEARCH_FOLDER_ID => Some((
            "contacts_search",
            "Contacts Search",
            65,
            crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
            "IPF.Contact",
        )),
        crate::mapi::identity::DOCUMENT_LIBRARIES_FOLDER_ID => Some((
            "document_libraries",
            "Document Libraries",
            66,
            crate::mapi::identity::ROOT_FOLDER_ID,
            "IPF.ShortcutFolder",
        )),
        crate::mapi::identity::SYNC_ISSUES_FOLDER_ID => Some((
            "sync_issues",
            "Sync Issues",
            67,
            crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
            "IPF.Note",
        )),
        crate::mapi::identity::CONFLICTS_FOLDER_ID => Some((
            "conflicts",
            "Conflicts",
            68,
            crate::mapi::identity::SYNC_ISSUES_FOLDER_ID,
            "IPF.Note",
        )),
        crate::mapi::identity::LOCAL_FAILURES_FOLDER_ID => Some((
            "local_failures",
            "Local Failures",
            69,
            crate::mapi::identity::SYNC_ISSUES_FOLDER_ID,
            "IPF.Note",
        )),
        crate::mapi::identity::SERVER_FAILURES_FOLDER_ID => Some((
            "server_failures",
            "Server Failures",
            70,
            crate::mapi::identity::SYNC_ISSUES_FOLDER_ID,
            "IPF.Note",
        )),
        crate::mapi::identity::COMMON_VIEWS_FOLDER_ID => Some((
            "__mapi_common_views",
            "Common Views",
            80,
            crate::mapi::identity::ROOT_FOLDER_ID,
            "",
        )),
        crate::mapi::identity::SCHEDULE_FOLDER_ID => Some((
            "__mapi_schedule",
            "Schedule",
            90,
            crate::mapi::identity::ROOT_FOLDER_ID,
            "",
        )),
        crate::mapi::identity::SEARCH_FOLDER_ID => Some((
            "__mapi_search",
            "Search",
            100,
            crate::mapi::identity::ROOT_FOLDER_ID,
            "",
        )),
        crate::mapi::identity::VIEWS_FOLDER_ID => Some((
            "__mapi_views",
            "Personal Views",
            110,
            crate::mapi::identity::ROOT_FOLDER_ID,
            "",
        )),
        crate::mapi::identity::SHORTCUTS_FOLDER_ID => Some((
            "__mapi_shortcuts",
            "Shortcuts",
            120,
            crate::mapi::identity::ROOT_FOLDER_ID,
            "IPF.ShortcutFolder",
        )),
        crate::mapi::identity::JUNK_FOLDER_ID => Some((
            "junk",
            "Junk E-mail",
            130,
            crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
            "IPF.Note",
        )),
        crate::mapi::identity::RSS_FEEDS_FOLDER_ID => Some((
            "rss_feeds",
            "RSS Feeds",
            140,
            crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
            "IPF.Note.OutlookHomepage",
        )),
        crate::mapi::identity::TRACKED_MAIL_PROCESSING_FOLDER_ID => Some((
            "tracked_mail_processing",
            "Tracked Mail Processing",
            150,
            crate::mapi::identity::ROOT_FOLDER_ID,
            "IPF.Note",
        )),
        crate::mapi::identity::TODO_SEARCH_FOLDER_ID => Some((
            "todo_search",
            "To-Do",
            160,
            crate::mapi::identity::ROOT_FOLDER_ID,
            "IPF.Task",
        )),
        crate::mapi::identity::CONVERSATION_ACTION_SETTINGS_FOLDER_ID => Some((
            "conversation_action_settings",
            "Conversation Action Settings",
            170,
            crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
            "IPF.Configuration",
        )),
        crate::mapi::identity::QUICK_STEP_SETTINGS_FOLDER_ID => Some((
            "quick_step_settings",
            "Quick Step Settings",
            175,
            crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
            "IPF.Configuration",
        )),
        crate::mapi::identity::ARCHIVE_FOLDER_ID => Some((
            "archive",
            "Archive",
            180,
            crate::mapi::identity::IPM_SUBTREE_FOLDER_ID,
            "IPF.Note",
        )),
        crate::mapi::identity::FREEBUSY_DATA_FOLDER_ID => Some((
            "__mapi_freebusy_data",
            "FreeBusy Data",
            190,
            crate::mapi::identity::ROOT_FOLDER_ID,
            "",
        )),
        crate::mapi::identity::RECOVERABLE_ITEMS_ROOT_FOLDER_ID => Some((
            "recoverable_items_root",
            "Recoverable Items",
            210,
            crate::mapi::identity::ROOT_FOLDER_ID,
            "IPF.Note",
        )),
        crate::mapi::identity::RECOVERABLE_ITEMS_DELETIONS_FOLDER_ID => Some((
            "recoverable_items_deletions",
            "Deletions",
            211,
            crate::mapi::identity::RECOVERABLE_ITEMS_ROOT_FOLDER_ID,
            "IPF.Note",
        )),
        crate::mapi::identity::RECOVERABLE_ITEMS_VERSIONS_FOLDER_ID => Some((
            "recoverable_items_versions",
            "Versions",
            212,
            crate::mapi::identity::RECOVERABLE_ITEMS_ROOT_FOLDER_ID,
            "IPF.Note",
        )),
        crate::mapi::identity::RECOVERABLE_ITEMS_PURGES_FOLDER_ID => Some((
            "recoverable_items_purges",
            "Purges",
            213,
            crate::mapi::identity::RECOVERABLE_ITEMS_ROOT_FOLDER_ID,
            "IPF.Note",
        )),
        _ => None,
    }
}
