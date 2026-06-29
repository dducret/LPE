use super::super::*;

pub(in crate::mapi::dispatch) fn debug_open_folder_property_shapes(
    properties: &HashMap<u32, MapiValue>,
) -> String {
    let mut tags = [
        PID_TAG_DISPLAY_NAME_W,
        PID_TAG_FOLDER_TYPE,
        PID_TAG_CONTAINER_CLASS_W,
        PID_TAG_CONTENT_COUNT,
        PID_TAG_CONTENT_UNREAD_COUNT,
        PID_TAG_SUBFOLDERS,
        PID_TAG_ENTRY_ID,
        PID_TAG_RECORD_KEY,
        PID_TAG_SOURCE_KEY,
        PID_TAG_PARENT_SOURCE_KEY,
    ]
    .into_iter()
    .filter_map(|tag| {
        properties
            .get(&tag)
            .map(|value| format!("{tag:#010x}:{}", mapi_value_debug_shape(value)))
    })
    .collect::<Vec<_>>();
    tags.sort();
    tags.join(",")
}

pub(in crate::mapi::dispatch) fn debug_open_folder_metadata(
    folder_id: u64,
    mailboxes: &[JmapMailbox],
) -> (String, String, String) {
    if let Some(mailbox) = folder_row_for_id(folder_id, mailboxes) {
        return (
            mapi_mailbox_display_name(mailbox),
            mailbox.role.clone(),
            folder_message_class(mailbox).to_string(),
        );
    }
    (
        post_hierarchy_probe_folder_name(folder_id).to_string(),
        debug_role_for_folder_id(folder_id).to_string(),
        debug_container_class_for_folder_id(folder_id).to_string(),
    )
}
