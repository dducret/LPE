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
