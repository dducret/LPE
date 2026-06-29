use super::*;

pub(super) fn debug_object_scope_for_id(
    object_id: Option<u64>,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
) -> &'static str {
    let Some(object_id) = object_id else {
        return "unparsed";
    };
    if is_advertised_special_folder(object_id) {
        return "advertised_special_folder";
    }
    if mailboxes
        .iter()
        .any(|mailbox| mapi_item_id_matches(&mailbox.id, object_id))
    {
        return "mailbox";
    }
    if emails
        .iter()
        .any(|email| mapi_item_id_matches(&email.id, object_id))
    {
        return "message";
    }
    if snapshot
        .event_for_id(CALENDAR_FOLDER_ID, object_id)
        .is_some()
        || snapshot
            .event_for_id(REMINDERS_FOLDER_ID, object_id)
            .is_some()
    {
        return "calendar_event";
    }
    if snapshot
        .contact_for_id(CONTACTS_FOLDER_ID, object_id)
        .is_some()
        || snapshot
            .contact_for_id(CONTACTS_SEARCH_FOLDER_ID, object_id)
            .is_some()
    {
        return "contact";
    }
    if snapshot.task_for_id(TASKS_FOLDER_ID, object_id).is_some()
        || snapshot
            .task_for_id(TODO_SEARCH_FOLDER_ID, object_id)
            .is_some()
        || snapshot
            .task_for_id(REMINDERS_FOLDER_ID, object_id)
            .is_some()
    {
        return "task";
    }
    if snapshot.note_for_id(NOTES_FOLDER_ID, object_id).is_some() {
        return "note";
    }
    if snapshot
        .journal_entry_for_id(JOURNAL_FOLDER_ID, object_id)
        .is_some()
    {
        return "journal_entry";
    }
    if snapshot
        .conversation_action_message_for_id(object_id)
        .is_some()
    {
        return "conversation_action";
    }
    "not_loaded"
}

fn long_term_id_from_id_scope_is_loaded(scope: &str) -> bool {
    scope != "unparsed" && scope != "not_loaded"
}

fn long_term_id_from_id_object_is_loaded(object_id: Option<u64>, scope: &str) -> bool {
    if long_term_id_from_id_scope_is_loaded(scope) {
        return true;
    }
    object_id
        .and_then(crate::mapi::identity::global_counter_from_store_id)
        .is_some_and(|counter| counter >= crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER)
}

pub(super) fn rop_long_term_id_from_id_response_for_scope(
    request: &RopRequest,
    object_id: Option<u64>,
    scope: &str,
) -> Vec<u8> {
    if long_term_id_from_id_object_is_loaded(object_id, scope) {
        rop_long_term_id_from_id_response(request)
    } else {
        rop_error_response(
            RopId::LongTermIdFromId as u8,
            request.response_handle_index(),
            0x8004_010F,
        )
    }
}
