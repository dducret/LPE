use super::*;

pub(super) fn restriction_matches_email_in_snapshot(
    restriction: Option<&MapiRestriction>,
    email: &JmapEmail,
    folder_id: u64,
    snapshot: &MapiMailStoreSnapshot,
) -> bool {
    restriction_matches_email_with_attachments(
        restriction,
        email,
        snapshot
            .attachments_for_message(folder_id, mapi_message_id(email))
            .unwrap_or_default(),
    )
}

pub(super) fn restriction_matches_conversation_member_in_snapshot(
    restriction: Option<&MapiRestriction>,
    email: &JmapEmail,
    snapshot: &MapiMailStoreSnapshot,
) -> bool {
    restriction_matches_email_in_snapshot(
        restriction,
        email,
        mapi_folder_id_for_email(email),
        snapshot,
    )
}

pub(super) fn is_top_level_count_restriction(restriction: Option<&MapiRestriction>) -> bool {
    matches!(restriction, Some(MapiRestriction::Count { .. }))
}

pub(super) fn property_tag_id_matches(left: u32, right: u32) -> bool {
    (left & 0xFFFF_0000) == (right & 0xFFFF_0000)
}

pub(in crate::mapi) fn is_unrestricted_common_views_navigation_projection(
    columns: &[u32],
    restriction: &Option<MapiRestriction>,
) -> bool {
    restriction.is_none()
        && columns.iter().any(|tag| {
            [
                PID_TAG_WLINK_SAVE_STAMP,
                PID_TAG_WLINK_TYPE,
                PID_TAG_WLINK_FLAGS,
                PID_TAG_WLINK_ORDINAL,
                PID_TAG_WLINK_ENTRY_ID,
                PID_TAG_WLINK_RECORD_KEY,
                PID_TAG_WLINK_STORE_ENTRY_ID,
                PID_TAG_WLINK_FOLDER_TYPE,
                PID_TAG_WLINK_GROUP_HEADER_ID,
                PID_TAG_WLINK_GROUP_CLSID,
                PID_TAG_WLINK_GROUP_NAME_W,
                PID_TAG_WLINK_SECTION,
                PID_TAG_WLINK_CALENDAR_COLOR,
                PID_TAG_WLINK_ADDRESS_BOOK_EID,
                PID_TAG_WLINK_ADDRESS_BOOK_STORE_EID,
                PID_TAG_WLINK_CLIENT_ID,
                PID_TAG_WLINK_RO_GROUP_TYPE,
                PID_NAME_SHARING_CALENDAR_GROUP_ENTRY_ASSOCIATED_LOCAL_FOLDER_ID_TAG,
            ]
            .iter()
            .any(|wlink_tag| property_tag_id_matches(*tag, *wlink_tag))
        })
        && !columns.iter().any(|tag| {
            [
                PID_TAG_VIEW_DESCRIPTOR_CLSID,
                PID_TAG_VIEW_DESCRIPTOR_FLAGS,
                OUTLOOK_COMMON_VIEW_DESCRIPTOR_BINARY_6835,
                OUTLOOK_COMMON_VIEW_DESCRIPTOR_STRINGS_683C,
                PID_TAG_VIEW_DESCRIPTOR_VERSION,
                PID_TAG_VIEW_DESCRIPTOR_FOLDER_TYPE,
                PID_TAG_VIEW_DESCRIPTOR_VIEW_MODE,
                PID_TAG_VIEW_DESCRIPTOR_BINARY,
                PID_TAG_VIEW_DESCRIPTOR_STRINGS_W,
                PID_TAG_VIEW_DESCRIPTOR_NAME_W,
                PID_TAG_VIEW_DESCRIPTOR_VERSION_CANONICAL,
            ]
            .iter()
            .any(|view_tag| property_tag_id_matches(*tag, *view_tag))
        })
}

pub(super) fn retain_rows_by_restriction<T>(
    rows: &mut Vec<T>,
    restriction: Option<&MapiRestriction>,
    mut matches_restriction: impl FnMut(&T, Option<&MapiRestriction>) -> bool,
) {
    if let Some(MapiRestriction::Count { count, child }) = restriction {
        let mut remaining = *count as usize;
        rows.retain(|row| {
            if remaining == 0 || !matches_restriction(row, Some(child)) {
                return false;
            }
            remaining -= 1;
            true
        });
    } else {
        rows.retain(|row| matches_restriction(row, restriction));
    }
}
