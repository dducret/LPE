use super::{
    format_bytes_hex, format_property_value_shapes_for_debug, hex_preview_for_debug,
    mapi_object_debug_fields,
};
use crate::mapi::properties::*;
use crate::mapi::rop::{
    canonical_property_storage_tag, special_folder_identification_property_value, AccountPrincipal,
    JmapEmail, JmapMailbox, MapiMailStoreSnapshot, MapiObject, CALENDAR_FOLDER_ID,
    COMMON_VIEWS_FOLDER_ID, CONTACTS_FOLDER_ID, DRAFTS_FOLDER_ID, INBOX_FOLDER_ID,
    IPM_SUBTREE_FOLDER_ID, JOURNAL_FOLDER_ID, NOTES_FOLDER_ID, OUTBOX_FOLDER_ID,
    REMINDERS_FOLDER_ID, ROOT_FOLDER_ID, SEARCH_FOLDER_ID, SENT_FOLDER_ID, TASKS_FOLDER_ID,
    TRASH_FOLDER_ID, VIEWS_FOLDER_ID,
};
use crate::mapi::sync::ARCHIVE_FOLDER_ID;
use crate::mapi_mailstore;

pub(in crate::mapi) fn log_calendar_default_folder_lookup_debug(
    object: Option<&MapiObject>,
    principal: &AccountPrincipal,
    columns: &[u32],
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    unsupported_tags: &[u32],
) {
    if !columns
        .iter()
        .any(|tag| canonical_property_storage_tag(*tag) == PID_TAG_IPM_APPOINTMENT_ENTRY_ID)
    {
        return;
    }
    let (object_kind, folder_id, _item_id) = mapi_object_debug_fields(object);
    let lookup_location = match object {
        Some(MapiObject::Folder {
            folder_id: INBOX_FOLDER_ID,
            ..
        }) => "inbox_primary",
        Some(MapiObject::Folder {
            folder_id: ROOT_FOLDER_ID,
            ..
        }) => "root_fallback",
        Some(MapiObject::Logon) => "store_logon",
        Some(MapiObject::PublicFolderLogon) => "public_folder_logon",
        Some(MapiObject::Folder { .. }) => "other_folder",
        _ => "other_object",
    };
    let unsupported = unsupported_tags
        .iter()
        .any(|tag| canonical_property_storage_tag(*tag) == PID_TAG_IPM_APPOINTMENT_ENTRY_ID);
    let entry_id = special_folder_identification_property_value(
        principal.account_id,
        PID_TAG_IPM_APPOINTMENT_ENTRY_ID,
    )
    .and_then(|value| match value {
        MapiValue::Binary(bytes) => Some(bytes),
        _ => None,
    })
    .unwrap_or_default();
    let inbox_entry_id = entry_id.clone();
    let root_fallback_entry_id = special_folder_identification_property_value(
        principal.account_id,
        PID_TAG_IPM_APPOINTMENT_ENTRY_ID,
    )
    .and_then(|value| match value {
        MapiValue::Binary(bytes) => Some(bytes),
        _ => None,
    })
    .unwrap_or_default();
    let decoded_folder_id = crate::mapi::identity::object_id_from_folder_entry_id(&entry_id);
    let root_fallback_decoded_folder_id =
        crate::mapi::identity::object_id_from_folder_entry_id(&root_fallback_entry_id);
    let calendar_collection = snapshot.collaboration_folder_for_id(CALENDAR_FOLDER_ID);
    let returned_value_shape = format_property_value_shapes_for_debug(
        object,
        principal,
        &[PID_TAG_IPM_APPOINTMENT_ENTRY_ID],
        mailboxes,
        emails,
        snapshot,
        unsupported_tags,
    );
    tracing::info!(
        rca_debug = true,
        adapter = "mapi",
        endpoint = "emsmdb",
        mailbox = %principal.email,
        request_type = "Execute",
        request_rop_id = "0x07",
        object_kind,
        folder_id = %folder_id,
        microsoft_documented_lookup_order = "GetReceiveFolder(Inbox), Inbox.GetProps(PR_IPM_APPOINTMENT_ENTRYID), root fallback",
        lookup_location,
        lookup_asked_inbox =
            matches!(object, Some(MapiObject::Folder { folder_id: INBOX_FOLDER_ID, .. })),
        lookup_asked_root =
            matches!(object, Some(MapiObject::Folder { folder_id: ROOT_FOLDER_ID, .. })),
        property_tag = "0x36d00102",
        property_name = "PidTagIpmAppointmentEntryId",
        property_returned = !unsupported,
        entry_id_bytes = entry_id.len(),
        entry_id_preview = %hex_preview_for_debug(&entry_id, 24),
        inbox_entry_id_bytes = inbox_entry_id.len(),
        inbox_entry_id_preview = %hex_preview_for_debug(&inbox_entry_id, 24),
        root_fallback_entry_id_bytes = root_fallback_entry_id.len(),
        root_fallback_entry_id_preview = %hex_preview_for_debug(&root_fallback_entry_id, 24),
        root_fallback_matches_inbox = root_fallback_entry_id == inbox_entry_id,
        decoded_folder_id = %decoded_folder_id
            .map(|folder_id| format!("0x{folder_id:016x}"))
            .unwrap_or_default(),
        root_fallback_decoded_folder_id = %root_fallback_decoded_folder_id
            .map(|folder_id| format!("0x{folder_id:016x}"))
            .unwrap_or_default(),
        decoded_folder_is_calendar = decoded_folder_id == Some(CALENDAR_FOLDER_ID),
        root_fallback_decoded_folder_is_calendar =
            root_fallback_decoded_folder_id == Some(CALENDAR_FOLDER_ID),
        expected_calendar_folder_id = "0x0000000000100001",
        calendar_folder_projected = calendar_collection.is_some(),
        calendar_collection_id =
            calendar_collection.map(|folder| folder.collection.id.as_str()).unwrap_or(""),
        calendar_collection_name =
            calendar_collection.map(|folder| folder.collection.display_name.as_str()).unwrap_or(""),
        returned_property_value_shape = %returned_value_shape,
        message = "rca debug mapi calendar default folder lookup"
    );
}

pub(in crate::mapi) fn default_folder_property_mappings_for_debug(tags: &[u32]) -> Vec<String> {
    tags.iter()
        .filter_map(|tag| default_folder_property_mapping_for_debug(*tag))
        .collect()
}

fn default_folder_property_mapping_for_debug(tag: u32) -> Option<String> {
    let (name, folder_id) = match canonical_property_storage_tag(tag) {
        PID_TAG_IPM_SUBTREE_ENTRY_ID => ("IPM Subtree", IPM_SUBTREE_FOLDER_ID),
        PID_TAG_IPM_OUTBOX_ENTRY_ID => ("Outbox", OUTBOX_FOLDER_ID),
        PID_TAG_IPM_WASTEBASKET_ENTRY_ID => ("Deleted Items", TRASH_FOLDER_ID),
        PID_TAG_IPM_SENTMAIL_ENTRY_ID => ("Sent Items", SENT_FOLDER_ID),
        PID_TAG_VIEWS_ENTRY_ID => ("Personal Views", VIEWS_FOLDER_ID),
        PID_TAG_COMMON_VIEWS_ENTRY_ID => ("Common Views", COMMON_VIEWS_FOLDER_ID),
        PID_TAG_FINDER_ENTRY_ID => ("Finder", SEARCH_FOLDER_ID),
        PID_TAG_IPM_ARCHIVE_ENTRY_ID => ("Archive", ARCHIVE_FOLDER_ID),
        PID_TAG_IPM_APPOINTMENT_ENTRY_ID => ("Calendar", CALENDAR_FOLDER_ID),
        PID_TAG_IPM_CONTACT_ENTRY_ID => ("Contacts", CONTACTS_FOLDER_ID),
        PID_TAG_IPM_JOURNAL_ENTRY_ID => ("Journal", JOURNAL_FOLDER_ID),
        PID_TAG_IPM_NOTE_ENTRY_ID => ("Notes", NOTES_FOLDER_ID),
        PID_TAG_IPM_TASK_ENTRY_ID => ("Tasks", TASKS_FOLDER_ID),
        PID_TAG_REM_ONLINE_ENTRY_ID => ("Reminders", REMINDERS_FOLDER_ID),
        PID_TAG_REM_OFFLINE_ENTRY_ID => ("Reminders", REMINDERS_FOLDER_ID),
        PID_TAG_IPM_DRAFTS_ENTRY_ID => ("Drafts", DRAFTS_FOLDER_ID),
        _ => return None,
    };
    Some(format!(
        "{tag:#010x}:{name}:folder_id={folder_id:#018x}:source_key={}",
        format_bytes_hex(&mapi_mailstore::source_key_for_store_id(folder_id))
    ))
}
