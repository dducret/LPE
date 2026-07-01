use super::*;

pub(super) fn is_object_id_conversion_rop(rop_id: RopId) -> bool {
    matches!(rop_id, RopId::LongTermIdFromId | RopId::IdFromLongTermId)
}

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

pub(super) fn append_long_term_id_from_id_response(
    principal: &AccountPrincipal,
    request: &RopRequest,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    responses: &mut Vec<u8>,
) {
    let source_id_bytes = request
        .long_term_source_id_bytes()
        .map(bytes_to_hex)
        .unwrap_or_default();
    let decoded_object_id = request.long_term_source_object_id();
    let decoded_object_scope =
        debug_object_scope_for_id(decoded_object_id, mailboxes, emails, snapshot);
    let response = rop_long_term_id_from_id_response_for_scope(
        request,
        decoded_object_id,
        decoded_object_scope,
    );
    let response_status = if response.len() > 6 {
        "ok"
    } else {
        "ecNotFound"
    };
    tracing::info!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = "emsmdb",
        mailbox = %principal.email,
        request_type = "Execute",
        request_rop_id = "0x43",
        source_id_bytes = %source_id_bytes,
        decoded_object_id = decoded_object_id
            .map(|object_id| format!("{object_id:#018x}"))
            .unwrap_or_default(),
        decoded_advertised_special_folder = decoded_object_id
            .map(is_advertised_special_folder)
            .unwrap_or(false),
        decoded_object_scope,
        response_status,
        message = "rca debug mapi long term id from id",
    );
    responses.extend_from_slice(&response)
}

pub(super) fn append_id_from_long_term_id_response(
    principal: &AccountPrincipal,
    request: &RopRequest,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    responses: &mut Vec<u8>,
) {
    let replica_guid_aliases = [
        *principal.account_id.as_bytes(),
        principal.account_id.to_bytes_le(),
    ];
    let long_term_id = request.long_term_id();
    let decoded_object_id =
        long_term_id.and_then(crate::mapi::identity::object_id_from_folder_identifier_bytes);
    let decoded_object_scope =
        debug_object_scope_for_id(decoded_object_id, mailboxes, emails, snapshot);
    let response = rop_id_from_long_term_id_response(request, &replica_guid_aliases);
    let response_status = if response.len() > 6 {
        "ok"
    } else {
        "ecNotFound"
    };
    tracing::info!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = "emsmdb",
        mailbox = %principal.email,
        request_type = "Execute",
        request_rop_id = "0x44",
        microsoft_special_folder_open_rule =
            "MS-OXOSFLD special-folder EntryIDs can be converted to FIDs by RopIdFromLongTermId before RopOpenFolder",
        long_term_id_bytes = long_term_id.map(|bytes| bytes.len()).unwrap_or_default(),
        long_term_id_preview = %long_term_id
            .map(|bytes| hex_preview(bytes, 24))
            .unwrap_or_default(),
        decoded_object_id = decoded_object_id
            .map(|object_id| format!("{object_id:#018x}"))
            .unwrap_or_default(),
        decoded_object_is_calendar = decoded_object_id == Some(CALENDAR_FOLDER_ID),
        decoded_advertised_special_folder = decoded_object_id
            .map(is_advertised_special_folder)
            .unwrap_or(false),
        decoded_object_scope,
        response_status,
        message = "rca debug mapi id from long term id",
    );
    responses.extend_from_slice(&response)
}

pub(super) fn append_object_id_conversion_response(
    principal: &AccountPrincipal,
    request: &RopRequest,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    responses: &mut Vec<u8>,
) {
    match RopId::from_u8(request.rop_id) {
        Some(RopId::LongTermIdFromId) => append_long_term_id_from_id_response(
            principal, request, mailboxes, emails, snapshot, responses,
        ),
        Some(RopId::IdFromLongTermId) => append_id_from_long_term_id_response(
            principal, request, mailboxes, emails, snapshot, responses,
        ),
        _ => {}
    }
}
