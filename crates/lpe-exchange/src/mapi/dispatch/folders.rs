use super::*;

pub(super) fn private_create_folder_is_existing_response_flag() -> bool {
    false
}

pub(super) fn create_folder_existing_mailbox_satisfies_deleted_advertised_request(
    session: &MapiSession,
    parent_folder_id: u64,
    display_name: &str,
) -> bool {
    advertised_special_folder_id_for_create(parent_folder_id, display_name)
        .map(|folder_id| session.advertised_special_folder_was_deleted(folder_id))
        .unwrap_or(false)
}

pub(super) fn advertised_special_folder_delete_uses_session_tombstone(folder_id: u64) -> bool {
    folder_id == QUICK_STEP_SETTINGS_FOLDER_ID
}

pub(super) fn advertised_special_folder_delete_is_noop(folder_id: u64) -> bool {
    matches!(
        folder_id,
        CONTACTS_FOLDER_ID
            | SUGGESTED_CONTACTS_FOLDER_ID
            | QUICK_CONTACTS_FOLDER_ID
            | IM_CONTACT_LIST_FOLDER_ID
    )
}

pub(super) fn synthetic_folder_allows_create_message(folder_id: u64) -> bool {
    matches!(
        folder_id,
        INBOX_FOLDER_ID
            | DRAFTS_FOLDER_ID
            | SENT_FOLDER_ID
            | TRASH_FOLDER_ID
            | OUTBOX_FOLDER_ID
            | NOTES_FOLDER_ID
            | JOURNAL_FOLDER_ID
            | FREEBUSY_DATA_FOLDER_ID
            | COMMON_VIEWS_FOLDER_ID
            | CONVERSATION_ACTION_SETTINGS_FOLDER_ID
            | QUICK_STEP_SETTINGS_FOLDER_ID
    )
}

pub(super) fn advertised_special_folder_container_class(folder_id: u64) -> Option<&'static str> {
    role_for_folder_id(folder_id)?;
    Some(match folder_id {
        CALENDAR_FOLDER_ID => "IPF.Appointment",
        CONTACTS_FOLDER_ID | SUGGESTED_CONTACTS_FOLDER_ID | CONTACTS_SEARCH_FOLDER_ID => {
            "IPF.Contact"
        }
        QUICK_CONTACTS_FOLDER_ID => "IPF.Contact.MOC.QuickContacts",
        IM_CONTACT_LIST_FOLDER_ID => "IPF.Contact.MOC.ImContactList",
        TASKS_FOLDER_ID | TODO_SEARCH_FOLDER_ID => "IPF.Task",
        NOTES_FOLDER_ID => "IPF.StickyNote",
        JOURNAL_FOLDER_ID => "IPF.Journal",
        RSS_FEEDS_FOLDER_ID => "IPF.Note.OutlookHomepage",
        _ => "IPF.Note",
    })
}

pub(super) fn folder_local_default_named_view_is_supported(
    snapshot: &MapiMailStoreSnapshot,
    folder_id: u64,
    message_id: u64,
) -> bool {
    snapshot
        .default_folder_named_view_message(folder_id, message_id)
        .is_some_and(|_| {
            let container_class = snapshot
                .collaboration_folder_for_id(folder_id)
                .map(|folder| collaboration_folder_message_class(folder.kind))
                .or_else(|| advertised_special_folder_container_class(folder_id));
            container_class.is_some_and(|container_class| {
                default_view_supported_folder(folder_id, container_class)
            })
        })
}
