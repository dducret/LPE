use super::*;

pub(in crate::mapi) fn folder_message_class(mailbox: &JmapMailbox) -> &'static str {
    if let Some(folder_id) = mailbox_advertised_special_folder_id(mailbox) {
        return special_folder_metadata(folder_id).2;
    }
    match mailbox.role.as_str() {
        "__mapi_deferred_action"
        | "__mapi_spooler_queue"
        | "__mapi_common_views"
        | "__mapi_views"
        | "__mapi_shortcuts"
        | "__mapi_freebusy_data" => "",
        "__mapi_search_folder_contact" => "IPF.Contact",
        "__mapi_search_folder_task" => "IPF.Task",
        "__mapi_search_folder_mixed" | "__mapi_search_folder_message" => "IPF.Note",
        "suggested_contacts" | "contacts_search" => "IPF.Contact",
        "quick_contacts" => "IPF.Contact.MOC.QuickContacts",
        "im_contact_list" => "IPF.Contact.MOC.ImContactList",
        "contacts" => "IPF.Contact",
        "calendar" => "IPF.Appointment",
        "journal" => "IPF.Journal",
        "notes" => "IPF.StickyNote",
        "tasks" => "IPF.Task",
        _ => "IPF.Note",
    }
}

pub(in crate::mapi) fn mailbox_projects_hidden_attribute(mailbox: &JmapMailbox) -> bool {
    matches!(
        mailbox_advertised_special_folder_id(mailbox),
        Some(CONVERSATION_ACTION_SETTINGS_FOLDER_ID | QUICK_STEP_SETTINGS_FOLDER_ID)
    )
}

fn mailbox_advertised_special_folder_id(mailbox: &JmapMailbox) -> Option<u64> {
    if mapi_parent_folder_id(mailbox) != IPM_SUBTREE_FOLDER_ID {
        return None;
    }
    advertised_special_folder_id_for_create(IPM_SUBTREE_FOLDER_ID, mailbox.name.trim())
}

pub(super) fn folder_type(mailbox: &JmapMailbox) -> u32 {
    if mailbox.role.starts_with("__mapi_search_folder_") {
        FOLDER_SEARCH
    } else {
        FOLDER_GENERIC
    }
}

pub(in crate::mapi) fn collaboration_folder_message_class(
    kind: MapiCollaborationFolderKind,
) -> &'static str {
    match kind {
        MapiCollaborationFolderKind::Contacts => "IPF.Contact",
        MapiCollaborationFolderKind::Calendar => "IPF.Appointment",
        MapiCollaborationFolderKind::Task => "IPF.Task",
    }
}

pub(in crate::mapi) fn mapi_folder_id(mailbox: &JmapMailbox) -> u64 {
    try_mapi_folder_id(mailbox).expect("MAPI folder identity mapping missing")
}

pub(in crate::mapi) fn try_mapi_folder_id(mailbox: &JmapMailbox) -> Option<u64> {
    try_mapi_folder_id_for_role(&mailbox.role)
        .or_else(|| crate::mapi::identity::mapped_mapi_object_id(&mailbox.id))
}

fn try_mapi_folder_id_for_role(role: &str) -> Option<u64> {
    match role {
        "__mapi_ipm_subtree" => Some(IPM_SUBTREE_FOLDER_ID),
        "__mapi_deferred_action" => Some(DEFERRED_ACTION_FOLDER_ID),
        "__mapi_spooler_queue" => Some(SPOOLER_QUEUE_FOLDER_ID),
        "__mapi_common_views" => Some(COMMON_VIEWS_FOLDER_ID),
        "__mapi_schedule" => Some(SCHEDULE_FOLDER_ID),
        "__mapi_search" => Some(SEARCH_FOLDER_ID),
        "__mapi_views" => Some(VIEWS_FOLDER_ID),
        "__mapi_shortcuts" => Some(SHORTCUTS_FOLDER_ID),
        "__mapi_freebusy_data" => Some(FREEBUSY_DATA_FOLDER_ID),
        "inbox" => Some(INBOX_FOLDER_ID),
        "drafts" => Some(DRAFTS_FOLDER_ID),
        "outbox" => Some(OUTBOX_FOLDER_ID),
        "sent" => Some(SENT_FOLDER_ID),
        "trash" => Some(TRASH_FOLDER_ID),
        "contacts" => Some(CONTACTS_FOLDER_ID),
        "calendar" => Some(CALENDAR_FOLDER_ID),
        "journal" => Some(JOURNAL_FOLDER_ID),
        "notes" => Some(NOTES_FOLDER_ID),
        "tasks" => Some(TASKS_FOLDER_ID),
        "reminders" => Some(REMINDERS_FOLDER_ID),
        "suggested_contacts" => Some(SUGGESTED_CONTACTS_FOLDER_ID),
        "quick_contacts" => Some(QUICK_CONTACTS_FOLDER_ID),
        "im_contact_list" => Some(IM_CONTACT_LIST_FOLDER_ID),
        "contacts_search" => Some(CONTACTS_SEARCH_FOLDER_ID),
        "document_libraries" => Some(DOCUMENT_LIBRARIES_FOLDER_ID),
        "sync_issues" => Some(SYNC_ISSUES_FOLDER_ID),
        "conflicts" => Some(CONFLICTS_FOLDER_ID),
        "local_failures" => Some(LOCAL_FAILURES_FOLDER_ID),
        "server_failures" => Some(SERVER_FAILURES_FOLDER_ID),
        "junk" => Some(JUNK_FOLDER_ID),
        "rss_feeds" => Some(RSS_FEEDS_FOLDER_ID),
        "tracked_mail_processing" => Some(TRACKED_MAIL_PROCESSING_FOLDER_ID),
        "todo_search" => Some(TODO_SEARCH_FOLDER_ID),
        "conversation_action_settings" => Some(CONVERSATION_ACTION_SETTINGS_FOLDER_ID),
        "quick_step_settings" => Some(QUICK_STEP_SETTINGS_FOLDER_ID),
        "archive" => Some(ARCHIVE_FOLDER_ID),
        "conversation_history" => Some(CONVERSATION_HISTORY_FOLDER_ID),
        _ => None,
    }
}

pub(super) fn mapi_parent_folder_id(mailbox: &JmapMailbox) -> u64 {
    match mailbox.role.as_str() {
        "conflicts" | "local_failures" | "server_failures" => SYNC_ISSUES_FOLDER_ID,
        _ => mailbox
            .parent_id
            .and_then(|parent_id| crate::mapi::identity::mapped_mapi_object_id(&parent_id))
            .unwrap_or(IPM_SUBTREE_FOLDER_ID),
    }
}

pub(in crate::mapi) fn is_root_hierarchy_folder(folder_id: u64) -> bool {
    matches!(
        folder_id,
        ROOT_FOLDER_ID | IPM_SUBTREE_FOLDER_ID | PUBLIC_FOLDERS_ROOT_FOLDER_ID
    )
}

pub(super) fn is_queryable_hierarchy_folder(folder_id: u64) -> bool {
    is_root_hierarchy_folder(folder_id) || folder_id == SYNC_ISSUES_FOLDER_ID
}

pub(in crate::mapi) fn is_advertised_special_folder(folder_id: u64) -> bool {
    if folder_id == CONVERSATION_HISTORY_FOLDER_ID {
        return false;
    }
    matches!(
        folder_id,
        ROOT_FOLDER_ID
            | IPM_SUBTREE_FOLDER_ID
            | DEFERRED_ACTION_FOLDER_ID
            | SPOOLER_QUEUE_FOLDER_ID
            | COMMON_VIEWS_FOLDER_ID
            | SCHEDULE_FOLDER_ID
            | SEARCH_FOLDER_ID
            | VIEWS_FOLDER_ID
            | SHORTCUTS_FOLDER_ID
            | FREEBUSY_DATA_FOLDER_ID
            | RECOVERABLE_ITEMS_ROOT_FOLDER_ID
            | RECOVERABLE_ITEMS_DELETIONS_FOLDER_ID
            | RECOVERABLE_ITEMS_VERSIONS_FOLDER_ID
            | RECOVERABLE_ITEMS_PURGES_FOLDER_ID
            | PUBLIC_FOLDERS_ROOT_FOLDER_ID
    ) || role_for_folder_id(folder_id).is_some()
}

pub(in crate::mapi) fn role_for_folder_id(folder_id: u64) -> Option<&'static str> {
    match folder_id {
        INBOX_FOLDER_ID => Some("inbox"),
        DRAFTS_FOLDER_ID => Some("drafts"),
        SENT_FOLDER_ID => Some("sent"),
        TRASH_FOLDER_ID => Some("trash"),
        OUTBOX_FOLDER_ID => Some("outbox"),
        CONTACTS_FOLDER_ID => Some("contacts"),
        CALENDAR_FOLDER_ID => Some("calendar"),
        JOURNAL_FOLDER_ID => Some("journal"),
        NOTES_FOLDER_ID => Some("notes"),
        TASKS_FOLDER_ID => Some("tasks"),
        REMINDERS_FOLDER_ID => Some("reminders"),
        PUBLIC_FOLDERS_ROOT_FOLDER_ID => Some("public_folders_root"),
        SUGGESTED_CONTACTS_FOLDER_ID => Some("suggested_contacts"),
        QUICK_CONTACTS_FOLDER_ID => Some("quick_contacts"),
        IM_CONTACT_LIST_FOLDER_ID => Some("im_contact_list"),
        CONTACTS_SEARCH_FOLDER_ID => Some("contacts_search"),
        DOCUMENT_LIBRARIES_FOLDER_ID => Some("document_libraries"),
        SYNC_ISSUES_FOLDER_ID => Some("sync_issues"),
        CONFLICTS_FOLDER_ID => Some("conflicts"),
        LOCAL_FAILURES_FOLDER_ID => Some("local_failures"),
        SERVER_FAILURES_FOLDER_ID => Some("server_failures"),
        JUNK_FOLDER_ID => Some("junk"),
        RSS_FEEDS_FOLDER_ID => Some("rss_feeds"),
        TRACKED_MAIL_PROCESSING_FOLDER_ID => Some("tracked_mail_processing"),
        TODO_SEARCH_FOLDER_ID => Some("todo_search"),
        CONVERSATION_ACTION_SETTINGS_FOLDER_ID => Some("conversation_action_settings"),
        QUICK_STEP_SETTINGS_FOLDER_ID => Some("quick_step_settings"),
        ARCHIVE_FOLDER_ID => Some("archive"),
        CONVERSATION_HISTORY_FOLDER_ID => Some("conversation_history"),
        _ => None,
    }
}

pub(in crate::mapi) fn advertised_special_folder_id_for_create(
    parent_folder_id: u64,
    display_name: &str,
) -> Option<u64> {
    [
        INBOX_FOLDER_ID,
        OUTBOX_FOLDER_ID,
        SENT_FOLDER_ID,
        TRASH_FOLDER_ID,
        DRAFTS_FOLDER_ID,
        CONTACTS_FOLDER_ID,
        CALENDAR_FOLDER_ID,
        JOURNAL_FOLDER_ID,
        NOTES_FOLDER_ID,
        TASKS_FOLDER_ID,
        SUGGESTED_CONTACTS_FOLDER_ID,
        QUICK_CONTACTS_FOLDER_ID,
        IM_CONTACT_LIST_FOLDER_ID,
        CONTACTS_SEARCH_FOLDER_ID,
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
        QUICK_STEP_SETTINGS_FOLDER_ID,
        ARCHIVE_FOLDER_ID,
        FREEBUSY_DATA_FOLDER_ID,
        CONVERSATION_HISTORY_FOLDER_ID,
        REMINDERS_FOLDER_ID,
    ]
    .into_iter()
    .find(|folder_id| {
        let (name, parent_id, _, _) = special_folder_metadata(*folder_id);
        if parent_id != parent_folder_id {
            return false;
        }
        if name.eq_ignore_ascii_case(display_name.trim()) {
            return true;
        }
        matches!(
            (
                *folder_id,
                display_name.trim().to_ascii_lowercase().as_str()
            ),
            (SENT_FOLDER_ID, "sent items")
                | (TRASH_FOLDER_ID, "deleted")
                | (TRASH_FOLDER_ID, "trash")
                | (JUNK_FOLDER_ID, "junk email")
        )
    })
}

pub(in crate::mapi) fn serialize_special_folder_row(
    folder_id: u64,
    mailboxes: &[JmapMailbox],
    columns: &[u32],
    principal: Option<&AccountPrincipal>,
) -> Vec<u8> {
    match folder_id {
        IPM_SUBTREE_FOLDER_ID => serialize_ipm_subtree_folder_row(mailboxes, columns, principal),
        ROOT_FOLDER_ID => serialize_root_folder_row(mailboxes, columns, principal),
        _ => serialize_advertised_special_folder_row(folder_id, columns, principal),
    }
}

pub(in crate::mapi) fn serialize_special_folder_row_with_version(
    folder_id: u64,
    mailboxes: &[JmapMailbox],
    columns: &[u32],
    principal: Option<&AccountPrincipal>,
    version: Option<&crate::mapi_store::MapiFolderVersion>,
) -> Vec<u8> {
    let mut row = Vec::new();
    for column in columns {
        if let Some(value) =
            version.and_then(|version| folder_version_property_value(version, *column))
        {
            write_mapi_value(&mut row, *column, &value);
        } else {
            row.extend_from_slice(&serialize_special_folder_row(
                folder_id,
                mailboxes,
                &[*column],
                principal,
            ));
        }
    }
    row
}

fn serialize_advertised_special_folder_row(
    folder_id: u64,
    columns: &[u32],
    principal: Option<&AccountPrincipal>,
) -> Vec<u8> {
    serialize_advertised_special_folder_row_with_mailbox_guid(
        folder_id,
        columns,
        principal
            .map(|principal| principal.account_id)
            .unwrap_or_default(),
    )
}

pub(in crate::mapi) fn serialize_advertised_special_folder_row_with_mailbox_guid(
    folder_id: u64,
    columns: &[u32],
    mailbox_guid: Uuid,
) -> Vec<u8> {
    serialize_advertised_special_folder_row_with_counts(folder_id, columns, mailbox_guid, 0, 0, 0)
}

pub(super) fn serialize_advertised_special_folder_row_with_counts(
    folder_id: u64,
    columns: &[u32],
    mailbox_guid: Uuid,
    content_count: u32,
    unread_count: u32,
    deleted_count: u32,
) -> Vec<u8> {
    serialize_advertised_special_folder_row_with_counts_and_change_number(
        folder_id,
        columns,
        mailbox_guid,
        content_count,
        unread_count,
        deleted_count,
        mapi_mailstore::change_number_for_store_id(folder_id),
    )
}

pub(super) fn serialize_advertised_special_folder_row_with_counts_and_change_number(
    folder_id: u64,
    columns: &[u32],
    mailbox_guid: Uuid,
    content_count: u32,
    unread_count: u32,
    deleted_count: u32,
    change_number: u64,
) -> Vec<u8> {
    let mut row = Vec::new();
    let (display_name, parent_folder_id, message_class, has_subfolders) =
        special_folder_metadata(folder_id);
    for column in columns {
        match *column {
            PID_TAG_DISPLAY_NAME_W => write_utf16z(&mut row, display_name),
            PID_TAG_ENTRY_ID => {
                let entry_id =
                    crate::mapi::identity::folder_entry_id_from_object_id(mailbox_guid, folder_id)
                        .unwrap_or_else(|| {
                            crate::mapi::identity::instance_key_for_object_id(folder_id)
                        });
                write_u16_prefixed_bytes(&mut row, &entry_id);
            }
            PID_TAG_INSTANCE_KEY => write_u16_prefixed_bytes(
                &mut row,
                &crate::mapi::identity::instance_key_for_object_id(folder_id),
            ),
            PID_TAG_FOLDER_ID => write_object_id(&mut row, folder_id),
            PID_TAG_PARENT_FOLDER_ID => write_object_id(&mut row, parent_folder_id),
            PID_TAG_FOLDER_TYPE => write_u32(&mut row, special_folder_type(folder_id)),
            PID_TAG_ACCESS => write_u32(&mut row, MAPI_FOLDER_ACCESS),
            PID_TAG_CONTENT_COUNT => write_u32(&mut row, content_count),
            PID_TAG_CONTENT_UNREAD_COUNT => write_u32(&mut row, unread_count),
            PID_TAG_DELETED_COUNT_TOTAL => write_u32(&mut row, deleted_count),
            PID_TAG_SUBFOLDERS => {
                row.push((has_subfolders && folder_id != SYNC_ISSUES_FOLDER_ID) as u8)
            }
            PID_TAG_ATTRIBUTE_HIDDEN => row.push(matches!(
                folder_id,
                CONVERSATION_ACTION_SETTINGS_FOLDER_ID | QUICK_STEP_SETTINGS_FOLDER_ID
            ) as u8),
            PID_TAG_CONTAINER_CLASS_W | PID_TAG_MESSAGE_CLASS_W if message_class.is_empty() => {
                write_property_default(&mut row, *column)
            }
            PID_TAG_CONTAINER_CLASS_W => write_utf16z(&mut row, message_class),
            PID_TAG_MESSAGE_CLASS_W => write_utf16z(&mut row, message_class),
            PID_TAG_DEFAULT_POST_MESSAGE_CLASS_STRING8 => {
                match default_post_message_class_for_container_class(message_class) {
                    Some(default_class) => write_ascii_z(&mut row, default_class),
                    None => write_property_default(&mut row, *column),
                }
            }
            PID_TAG_DEFAULT_POST_MESSAGE_CLASS_W => {
                match default_post_message_class_for_container_class(message_class) {
                    Some(default_class) => write_utf16z(&mut row, default_class),
                    None => write_property_default(&mut row, *column),
                }
            }
            PID_TAG_DEFAULT_VIEW_ENTRY_ID
                if default_view_supported_folder(folder_id, message_class) =>
            {
                match default_folder_view_entry_id(mailbox_guid, folder_id, message_class) {
                    Some(value) => write_mapi_value(&mut row, *column, &value),
                    None => write_property_default(&mut row, *column),
                }
            }
            PID_TAG_LAST_MODIFICATION_TIME
            | PID_TAG_LOCAL_COMMIT_TIME
            | PID_TAG_LOCAL_COMMIT_TIME_MAX
            | PID_TAG_HIER_REV => write_u64(
                &mut row,
                mapi_mailstore::filetime_from_change_number(change_number),
            ),
            PID_TAG_SERIALIZED_REPLID_GUID_MAP => {
                write_u16_prefixed_bytes(&mut row, &serialized_replid_guid_map())
            }
            PID_TAG_HIERARCHY_CHANGE_NUMBER => {
                write_u32(&mut row, change_number.min(u64::from(u32::MAX)) as u32)
            }
            PID_TAG_SOURCE_KEY => write_u16_prefixed_bytes(
                &mut row,
                &mapi_mailstore::source_key_for_store_id(folder_id),
            ),
            PID_TAG_PARENT_SOURCE_KEY => write_u16_prefixed_bytes(
                &mut row,
                &mapi_mailstore::source_key_for_store_id(parent_folder_id),
            ),
            PID_TAG_CHANGE_KEY => write_u16_prefixed_bytes(
                &mut row,
                &mapi_mailstore::change_key_for_change_number(change_number),
            ),
            PID_TAG_PREDECESSOR_CHANGE_LIST => write_u16_prefixed_bytes(
                &mut row,
                &mapi_mailstore::predecessor_change_list(change_number),
            ),
            PID_TAG_CHANGE_NUMBER => write_u64(&mut row, change_number),
            _ if folder_id == INBOX_FOLDER_ID => {
                match special_folder_identification_property_value(mailbox_guid, *column) {
                    Some(value) => write_mapi_value(&mut row, *column, &value),
                    None => write_property_default(&mut row, *column),
                }
            }
            _ => write_property_default(&mut row, *column),
        }
    }
    row
}

pub(super) fn serialize_advertised_special_folder_row_with_counts_and_version(
    folder_id: u64,
    columns: &[u32],
    mailbox_guid: Uuid,
    content_count: u32,
    unread_count: u32,
    deleted_count: u32,
    version: Option<&crate::mapi_store::MapiFolderVersion>,
) -> Vec<u8> {
    let fallback_change_number = version
        .map(|version| version.change_number)
        .unwrap_or_else(|| mapi_mailstore::change_number_for_store_id(folder_id));
    let mut row = Vec::new();
    for column in columns {
        if let Some(value) =
            version.and_then(|version| folder_version_property_value(version, *column))
        {
            write_mapi_value(&mut row, *column, &value);
        } else {
            row.extend_from_slice(
                &serialize_advertised_special_folder_row_with_counts_and_change_number(
                    folder_id,
                    &[*column],
                    mailbox_guid,
                    content_count,
                    unread_count,
                    deleted_count,
                    fallback_change_number,
                ),
            );
        }
    }
    row
}

pub(super) fn special_folder_metadata(folder_id: u64) -> (&'static str, u64, &'static str, bool) {
    match folder_id {
        ROOT_FOLDER_ID => ("Root", 0, "", true),
        IPM_SUBTREE_FOLDER_ID => ("Top of Information Store", ROOT_FOLDER_ID, "IPF.Note", true),
        DEFERRED_ACTION_FOLDER_ID => ("Deferred Action", ROOT_FOLDER_ID, "", false),
        SPOOLER_QUEUE_FOLDER_ID => ("Spooler Queue", ROOT_FOLDER_ID, "", false),
        INBOX_FOLDER_ID => ("Inbox", IPM_SUBTREE_FOLDER_ID, "IPF.Note", false),
        OUTBOX_FOLDER_ID => ("Outbox", IPM_SUBTREE_FOLDER_ID, "IPF.Note", false),
        SENT_FOLDER_ID => ("Sent", IPM_SUBTREE_FOLDER_ID, "IPF.Note", false),
        TRASH_FOLDER_ID => ("Deleted Items", IPM_SUBTREE_FOLDER_ID, "IPF.Note", false),
        COMMON_VIEWS_FOLDER_ID => ("Common Views", ROOT_FOLDER_ID, "", false),
        SCHEDULE_FOLDER_ID => ("Schedule", ROOT_FOLDER_ID, "", false),
        SEARCH_FOLDER_ID => ("Search", ROOT_FOLDER_ID, "IPF.Note", false),
        VIEWS_FOLDER_ID => ("Personal Views", ROOT_FOLDER_ID, "", false),
        SHORTCUTS_FOLDER_ID => ("Shortcuts", ROOT_FOLDER_ID, "IPF.ShortcutFolder", false),
        DRAFTS_FOLDER_ID => ("Drafts", IPM_SUBTREE_FOLDER_ID, "IPF.Note", false),
        CONTACTS_FOLDER_ID => ("Contacts", IPM_SUBTREE_FOLDER_ID, "IPF.Contact", false),
        CALENDAR_FOLDER_ID => ("Calendar", IPM_SUBTREE_FOLDER_ID, "IPF.Appointment", false),
        JOURNAL_FOLDER_ID => ("Journal", IPM_SUBTREE_FOLDER_ID, "IPF.Journal", false),
        NOTES_FOLDER_ID => ("Notes", IPM_SUBTREE_FOLDER_ID, "IPF.StickyNote", false),
        TASKS_FOLDER_ID => ("Tasks", IPM_SUBTREE_FOLDER_ID, "IPF.Task", false),
        SUGGESTED_CONTACTS_FOLDER_ID => (
            "Suggested Contacts",
            IPM_SUBTREE_FOLDER_ID,
            "IPF.Contact",
            false,
        ),
        QUICK_CONTACTS_FOLDER_ID => (
            "Quick Contacts",
            IPM_SUBTREE_FOLDER_ID,
            "IPF.Contact.MOC.QuickContacts",
            false,
        ),
        IM_CONTACT_LIST_FOLDER_ID => (
            "IM Contact List",
            IPM_SUBTREE_FOLDER_ID,
            "IPF.Contact.MOC.ImContactList",
            false,
        ),
        CONTACTS_SEARCH_FOLDER_ID => ("Contacts Search", SEARCH_FOLDER_ID, "IPF.Contact", false),
        DOCUMENT_LIBRARIES_FOLDER_ID => (
            "Document Libraries",
            ROOT_FOLDER_ID,
            "IPF.ShortcutFolder",
            false,
        ),
        SYNC_ISSUES_FOLDER_ID => ("Sync Issues", IPM_SUBTREE_FOLDER_ID, "IPF.Note", true),
        CONFLICTS_FOLDER_ID => ("Conflicts", SYNC_ISSUES_FOLDER_ID, "IPF.Note", false),
        LOCAL_FAILURES_FOLDER_ID => ("Local Failures", SYNC_ISSUES_FOLDER_ID, "IPF.Note", false),
        SERVER_FAILURES_FOLDER_ID => ("Server Failures", SYNC_ISSUES_FOLDER_ID, "IPF.Note", false),
        JUNK_FOLDER_ID => ("Junk E-mail", IPM_SUBTREE_FOLDER_ID, "IPF.Note", false),
        RSS_FEEDS_FOLDER_ID => (
            "RSS Feeds",
            IPM_SUBTREE_FOLDER_ID,
            "IPF.Note.OutlookHomepage",
            false,
        ),
        TRACKED_MAIL_PROCESSING_FOLDER_ID => {
            ("Tracked Mail Processing", ROOT_FOLDER_ID, "IPF.Note", false)
        }
        TODO_SEARCH_FOLDER_ID => ("To-Do", ROOT_FOLDER_ID, "IPF.Task", false),
        RECOVERABLE_ITEMS_ROOT_FOLDER_ID => ("Recoverable Items", ROOT_FOLDER_ID, "IPF.Note", true),
        RECOVERABLE_ITEMS_DELETIONS_FOLDER_ID => (
            "Deletions",
            RECOVERABLE_ITEMS_ROOT_FOLDER_ID,
            "IPF.Note",
            false,
        ),
        RECOVERABLE_ITEMS_VERSIONS_FOLDER_ID => (
            "Versions",
            RECOVERABLE_ITEMS_ROOT_FOLDER_ID,
            "IPF.Note",
            false,
        ),
        RECOVERABLE_ITEMS_PURGES_FOLDER_ID => (
            "Purges",
            RECOVERABLE_ITEMS_ROOT_FOLDER_ID,
            "IPF.Note",
            false,
        ),
        CONVERSATION_ACTION_SETTINGS_FOLDER_ID => (
            "Conversation Action Settings",
            IPM_SUBTREE_FOLDER_ID,
            "IPF.Configuration",
            false,
        ),
        QUICK_STEP_SETTINGS_FOLDER_ID => (
            "Quick Step Settings",
            IPM_SUBTREE_FOLDER_ID,
            "IPF.Configuration",
            false,
        ),
        ARCHIVE_FOLDER_ID => ("Archive", IPM_SUBTREE_FOLDER_ID, "IPF.Note", false),
        FREEBUSY_DATA_FOLDER_ID => ("FreeBusy Data", ROOT_FOLDER_ID, "IPF.Note", false),
        CONVERSATION_HISTORY_FOLDER_ID => (
            "Conversation History",
            IPM_SUBTREE_FOLDER_ID,
            "IPF.Note",
            false,
        ),
        REMINDERS_FOLDER_ID => ("Reminders", ROOT_FOLDER_ID, "Outlook.Reminder", false),
        PUBLIC_FOLDERS_ROOT_FOLDER_ID => ("Public Folders", 0, "IPF.Note", true),
        _ => ("Root", 0, "", true),
    }
}

pub(super) fn special_folder_type(folder_id: u64) -> u32 {
    match folder_id {
        ROOT_FOLDER_ID | PUBLIC_FOLDERS_ROOT_FOLDER_ID => FOLDER_ROOT,
        SEARCH_FOLDER_ID
        | CONTACTS_SEARCH_FOLDER_ID
        | REMINDERS_FOLDER_ID
        | TRACKED_MAIL_PROCESSING_FOLDER_ID
        | TODO_SEARCH_FOLDER_ID => FOLDER_SEARCH,
        _ => FOLDER_GENERIC,
    }
}

pub(in crate::mapi) fn serialize_root_folder_row(
    _mailboxes: &[JmapMailbox],
    columns: &[u32],
    principal: Option<&AccountPrincipal>,
) -> Vec<u8> {
    let mut row = Vec::new();
    let change_number = mapi_mailstore::change_number_for_store_id(ROOT_FOLDER_ID);
    for column in columns {
        match *column {
            PID_TAG_DISPLAY_NAME_W => write_utf16z(&mut row, "Root"),
            PID_TAG_ENTRY_ID => {
                let mailbox_guid = principal
                    .map(|principal| principal.account_id)
                    .unwrap_or_default();
                let entry_id = crate::mapi::identity::folder_entry_id_from_object_id(
                    mailbox_guid,
                    ROOT_FOLDER_ID,
                )
                .unwrap_or_else(|| {
                    crate::mapi::identity::instance_key_for_object_id(ROOT_FOLDER_ID)
                });
                write_u16_prefixed_bytes(&mut row, &entry_id);
            }
            PID_TAG_INSTANCE_KEY => write_u16_prefixed_bytes(
                &mut row,
                &crate::mapi::identity::instance_key_for_object_id(ROOT_FOLDER_ID),
            ),
            PID_TAG_FOLDER_ID => write_object_id(&mut row, ROOT_FOLDER_ID),
            PID_TAG_PARENT_FOLDER_ID => write_object_id(&mut row, 0),
            PID_TAG_FOLDER_TYPE => write_u32(&mut row, FOLDER_ROOT),
            PID_TAG_ACCESS => write_u32(&mut row, MAPI_FOLDER_ACCESS),
            PID_TAG_CONTENT_COUNT | PID_TAG_CONTENT_UNREAD_COUNT | PID_TAG_DELETED_COUNT_TOTAL => {
                write_u32(&mut row, 0)
            }
            PID_TAG_SUBFOLDERS => row.push(1),
            PID_TAG_CONTAINER_CLASS_W | PID_TAG_MESSAGE_CLASS_W => {
                write_property_default(&mut row, *column)
            }
            PID_TAG_LAST_MODIFICATION_TIME
            | PID_TAG_LOCAL_COMMIT_TIME
            | PID_TAG_LOCAL_COMMIT_TIME_MAX
            | PID_TAG_HIER_REV => write_u64(
                &mut row,
                mapi_mailstore::filetime_from_change_number(change_number),
            ),
            PID_TAG_SERIALIZED_REPLID_GUID_MAP => {
                write_u16_prefixed_bytes(&mut row, &serialized_replid_guid_map())
            }
            PID_TAG_HIERARCHY_CHANGE_NUMBER => {
                write_u32(&mut row, change_number.min(u64::from(u32::MAX)) as u32)
            }
            PID_TAG_SOURCE_KEY => write_u16_prefixed_bytes(
                &mut row,
                &mapi_mailstore::source_key_for_store_id(ROOT_FOLDER_ID),
            ),
            PID_TAG_PARENT_SOURCE_KEY => write_u16_prefixed_bytes(&mut row, &[]),
            PID_TAG_CHANGE_KEY => write_u16_prefixed_bytes(
                &mut row,
                &mapi_mailstore::change_key_for_change_number(change_number),
            ),
            PID_TAG_PREDECESSOR_CHANGE_LIST => write_u16_prefixed_bytes(
                &mut row,
                &mapi_mailstore::predecessor_change_list(change_number),
            ),
            PID_TAG_CHANGE_NUMBER => write_u64(&mut row, change_number),
            _ => match special_folder_identification_property_value(
                principal
                    .map(|principal| principal.account_id)
                    .unwrap_or_default(),
                *column,
            ) {
                Some(value) => write_mapi_value(&mut row, *column, &value),
                None => write_property_default(&mut row, *column),
            },
        }
    }
    row
}

pub(in crate::mapi) fn serialize_ipm_subtree_folder_row(
    _mailboxes: &[JmapMailbox],
    columns: &[u32],
    principal: Option<&AccountPrincipal>,
) -> Vec<u8> {
    let mut row = Vec::new();
    let change_number = mapi_mailstore::change_number_for_store_id(IPM_SUBTREE_FOLDER_ID);
    for column in columns {
        match *column {
            PID_TAG_DISPLAY_NAME_W => write_utf16z(&mut row, "Top of Information Store"),
            PID_TAG_ENTRY_ID => {
                let mailbox_guid = principal
                    .map(|principal| principal.account_id)
                    .unwrap_or_default();
                let entry_id = crate::mapi::identity::folder_entry_id_from_object_id(
                    mailbox_guid,
                    IPM_SUBTREE_FOLDER_ID,
                )
                .unwrap_or_else(|| {
                    crate::mapi::identity::instance_key_for_object_id(IPM_SUBTREE_FOLDER_ID)
                });
                write_u16_prefixed_bytes(&mut row, &entry_id);
            }
            PID_TAG_INSTANCE_KEY => write_u16_prefixed_bytes(
                &mut row,
                &crate::mapi::identity::instance_key_for_object_id(IPM_SUBTREE_FOLDER_ID),
            ),
            PID_TAG_FOLDER_ID => write_object_id(&mut row, IPM_SUBTREE_FOLDER_ID),
            PID_TAG_PARENT_FOLDER_ID => write_object_id(&mut row, ROOT_FOLDER_ID),
            PID_TAG_FOLDER_TYPE => write_u32(&mut row, FOLDER_GENERIC),
            PID_TAG_ACCESS => write_u32(&mut row, MAPI_FOLDER_ACCESS),
            PID_TAG_CONTENT_COUNT | PID_TAG_CONTENT_UNREAD_COUNT | PID_TAG_DELETED_COUNT_TOTAL => {
                write_u32(&mut row, 0)
            }
            PID_TAG_SUBFOLDERS => row.push(1),
            PID_TAG_CONTAINER_CLASS_W => write_utf16z(&mut row, "IPF.Note"),
            PID_TAG_MESSAGE_CLASS_W => write_utf16z(&mut row, "IPF.Note"),
            PID_TAG_DEFAULT_POST_MESSAGE_CLASS_STRING8 => write_ascii_z(&mut row, "IPM.Note"),
            PID_TAG_DEFAULT_POST_MESSAGE_CLASS_W => write_utf16z(&mut row, "IPM.Note"),
            PID_TAG_LAST_MODIFICATION_TIME
            | PID_TAG_LOCAL_COMMIT_TIME
            | PID_TAG_LOCAL_COMMIT_TIME_MAX
            | PID_TAG_HIER_REV => write_u64(
                &mut row,
                mapi_mailstore::filetime_from_change_number(change_number),
            ),
            PID_TAG_SERIALIZED_REPLID_GUID_MAP => {
                write_u16_prefixed_bytes(&mut row, &serialized_replid_guid_map())
            }
            PID_TAG_HIERARCHY_CHANGE_NUMBER => {
                write_u32(&mut row, change_number.min(u64::from(u32::MAX)) as u32)
            }
            PID_TAG_SOURCE_KEY => write_u16_prefixed_bytes(
                &mut row,
                &mapi_mailstore::source_key_for_store_id(IPM_SUBTREE_FOLDER_ID),
            ),
            PID_TAG_PARENT_SOURCE_KEY => write_u16_prefixed_bytes(
                &mut row,
                &mapi_mailstore::source_key_for_store_id(ROOT_FOLDER_ID),
            ),
            PID_TAG_CHANGE_KEY => write_u16_prefixed_bytes(
                &mut row,
                &mapi_mailstore::change_key_for_change_number(change_number),
            ),
            PID_TAG_PREDECESSOR_CHANGE_LIST => write_u16_prefixed_bytes(
                &mut row,
                &mapi_mailstore::predecessor_change_list(change_number),
            ),
            PID_TAG_CHANGE_NUMBER => write_u64(&mut row, change_number),
            PID_TAG_OST_OSTID => write_u16_prefixed_bytes(
                &mut row,
                &principal.map(ipm_subtree_ost_ostid).unwrap_or_default(),
            ),
            _ => write_property_default(&mut row, *column),
        }
    }
    row
}

pub(in crate::mapi) fn write_logon_property_row(
    response: &mut Vec<u8>,
    principal: &AccountPrincipal,
    columns: &[u32],
) {
    if columns
        .iter()
        .all(|column| logon_property_value(principal, *column).is_some())
    {
        write_standard_property_row(response, &serialize_logon_row(principal, columns));
        return;
    }

    response.push(1);
    for column in columns {
        match logon_property_value(principal, *column) {
            Some(value) => {
                response.push(0);
                write_mapi_value(response, *column, &value);
            }
            None => {
                response.push(0x0A);
                write_u32(response, ROP_ERROR_NOT_FOUND);
            }
        }
    }
}

pub(in crate::mapi) fn serialize_logon_row(
    principal: &AccountPrincipal,
    columns: &[u32],
) -> Vec<u8> {
    let mut row = Vec::new();
    for column in columns {
        match logon_property_value(principal, *column) {
            Some(value) => write_mapi_value(&mut row, *column, &value),
            None => write_property_default(&mut row, *column),
        }
    }
    row
}

pub(in crate::mapi) fn serialized_replid_guid_map() -> Vec<u8> {
    let mut value = Vec::with_capacity(18);
    value.extend_from_slice(&(STORE_REPLICA_ID as u16).to_le_bytes());
    value.extend_from_slice(&crate::mapi::identity::STORE_REPLICA_GUID);
    value
}

pub(super) fn mailbox_has_subfolders(mailbox: &JmapMailbox, mailboxes: &[JmapMailbox]) -> bool {
    if mapi_folder_id(mailbox) == SYNC_ISSUES_FOLDER_ID {
        return false;
    }
    !mailboxes.is_empty()
        && mailboxes
            .iter()
            .any(|candidate| candidate.parent_id == Some(mailbox.id))
}

pub(in crate::mapi) fn serialize_folder_row_with_context(
    mailbox: &JmapMailbox,
    mailboxes: &[JmapMailbox],
    columns: &[u32],
    mailbox_guid: Uuid,
) -> Vec<u8> {
    let mut row = Vec::new();
    for column in columns {
        match *column {
            PID_TAG_DISPLAY_NAME_W => write_utf16z(&mut row, &mapi_mailbox_display_name(mailbox)),
            PID_TAG_FOLDER_ID => write_object_id(&mut row, mapi_folder_id(mailbox)),
            PID_TAG_PARENT_FOLDER_ID => write_object_id(&mut row, mapi_parent_folder_id(mailbox)),
            PID_TAG_CONTENT_COUNT => write_u32(&mut row, mailbox.total_emails),
            PID_TAG_CONTENT_UNREAD_COUNT => write_u32(&mut row, mailbox.unread_emails),
            PID_TAG_MESSAGE_SIZE => write_u32(
                &mut row,
                mailbox.size_octets.min(u64::from(u32::MAX)) as u32,
            ),
            PID_TAG_MESSAGE_SIZE_EXTENDED => write_u64(&mut row, mailbox.size_octets),
            PID_TAG_SUBFOLDERS => row.push(mailbox_has_subfolders(mailbox, mailboxes) as u8),
            PID_TAG_FOLDER_TYPE => write_u32(&mut row, folder_type(mailbox)),
            PID_TAG_ACCESS => write_u32(&mut row, MAPI_FOLDER_ACCESS),
            PID_TAG_CONTAINER_CLASS_W => write_utf16z(&mut row, folder_message_class(mailbox)),
            PID_TAG_MESSAGE_CLASS_W => write_utf16z(&mut row, folder_message_class(mailbox)),
            _ => match mailbox_property_value_with_context_for_account(
                mailbox,
                mailboxes,
                *column,
                mailbox_guid,
            ) {
                Some(value) => write_mapi_value(&mut row, *column, &value),
                None => write_property_default(&mut row, *column),
            },
        }
    }
    row
}

pub(in crate::mapi) fn serialize_folder_row_with_context_and_version(
    mailbox: &JmapMailbox,
    mailboxes: &[JmapMailbox],
    columns: &[u32],
    mailbox_guid: Uuid,
    version: Option<&crate::mapi_store::MapiFolderVersion>,
) -> Vec<u8> {
    let mut row = Vec::new();
    for column in columns {
        if let Some(value) =
            version.and_then(|version| folder_version_property_value(version, *column))
        {
            write_mapi_value(&mut row, *column, &value);
        } else {
            row.extend_from_slice(&serialize_folder_row_with_context(
                mailbox,
                mailboxes,
                &[*column],
                mailbox_guid,
            ));
        }
    }
    row
}

pub(in crate::mapi) fn serialize_collaboration_folder_row_with_context(
    folder: &MapiCollaborationFolder,
    columns: &[u32],
    associated_count: u32,
) -> Vec<u8> {
    let mut row = Vec::new();
    for column in columns {
        match *column {
            PID_TAG_DISPLAY_NAME_W => write_utf16z(&mut row, &folder.collection.display_name),
            PID_TAG_FOLDER_ID => write_object_id(&mut row, folder.id),
            PID_TAG_PARENT_FOLDER_ID => write_object_id(&mut row, IPM_SUBTREE_FOLDER_ID),
            PID_TAG_CONTENT_COUNT => write_u32(&mut row, folder.item_count),
            PID_TAG_CONTENT_UNREAD_COUNT => write_u32(&mut row, 0),
            PID_TAG_ASSOCIATED_CONTENT_COUNT => write_u32(&mut row, associated_count),
            PID_TAG_SUBFOLDERS => row.push(0),
            PID_TAG_FOLDER_TYPE => write_u32(&mut row, FOLDER_GENERIC),
            PID_TAG_ACCESS => write_u32(&mut row, MAPI_FOLDER_ACCESS),
            PID_TAG_CONTAINER_CLASS_W => {
                write_utf16z(&mut row, collaboration_folder_message_class(folder.kind))
            }
            PID_TAG_MESSAGE_CLASS_W => {
                write_utf16z(&mut row, collaboration_folder_message_class(folder.kind))
            }
            _ => match collaboration_folder_property_value(folder, *column) {
                Some(value) => write_mapi_value(&mut row, *column, &value),
                None => write_property_default(&mut row, *column),
            },
        }
    }
    row
}

pub(in crate::mapi) fn serialize_collaboration_folder_row_with_context_and_version(
    folder: &MapiCollaborationFolder,
    columns: &[u32],
    associated_count: u32,
    version: Option<&crate::mapi_store::MapiFolderVersion>,
) -> Vec<u8> {
    let mut row = Vec::new();
    for column in columns {
        if let Some(value) =
            version.and_then(|version| folder_version_property_value(version, *column))
        {
            write_mapi_value(&mut row, *column, &value);
        } else {
            row.extend_from_slice(&serialize_collaboration_folder_row_with_context(
                folder,
                &[*column],
                associated_count,
            ));
        }
    }
    row
}

pub(in crate::mapi) fn mapi_message_id(email: &JmapEmail) -> u64 {
    mapi_item_id(&email.id)
}

pub(in crate::mapi) fn mapi_folder_id_for_email(email: &JmapEmail) -> u64 {
    try_mapi_folder_id_for_role(&email.mailbox_role)
        .or_else(|| crate::mapi::identity::mapped_mapi_object_id(&email.mailbox_id))
        .unwrap_or(IPM_SUBTREE_FOLDER_ID)
}

pub(in crate::mapi) fn mapi_item_id(id: &Uuid) -> u64 {
    crate::mapi::identity::mapped_mapi_object_id(id).expect("MAPI item identity mapping missing")
}
