use super::super::*;

pub(in crate::mapi::dispatch) fn sync_checkpoint_scope(
    folder_id: u64,
    checkpoint_mailbox_id: Option<Uuid>,
    special_objects: &[mapi_mailstore::SpecialMessageSyncFact],
) -> &'static str {
    let virtual_scope_id =
        mapi_mailstore::virtual_special_mailbox(folder_id).map(|mailbox| mailbox.id);
    if checkpoint_mailbox_id.is_some() && checkpoint_mailbox_id == virtual_scope_id {
        return "virtual_special_folder";
    }
    if checkpoint_mailbox_id.is_some() {
        "canonical_mailbox"
    } else if !special_objects.is_empty() {
        "virtual_special_folder"
    } else {
        "virtual_or_system_folder"
    }
}

pub(in crate::mapi::dispatch) fn uploaded_state_marker_summary(marker_mask: u8) -> String {
    let mut markers = Vec::new();
    if marker_mask & 0x01 != 0 {
        markers.push("MetaTagIdsetGiven");
    }
    if marker_mask & 0x02 != 0 {
        markers.push("MetaTagCnsetSeen");
    }
    if marker_mask & 0x04 != 0 {
        markers.push("MetaTagCnsetSeenFAI");
    }
    if marker_mask & 0x08 != 0 {
        markers.push("MetaTagCnsetRead");
    }
    markers.join(",")
}
