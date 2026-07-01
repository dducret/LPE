use super::*;

pub(in crate::mapi) const COLLAPSE_STATE_MAGIC: &[u8; 6] = b"LPECS1";

pub(in crate::mapi) fn rop_expand_row_response(
    request: &RopRequest,
    object: Option<&mut MapiObject>,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
) -> Vec<u8> {
    let Some(category_id) = request.category_id() else {
        return rop_error_response(0x59, request.response_handle_index(), 0x8004_0102);
    };
    let Some(MapiObject::ContentsTable {
        folder_id,
        associated,
        columns,
        columns_set,
        sort_orders,
        category_count,
        expanded_count,
        collapsed_categories,
        restriction,
        ..
    }) = object
    else {
        return rop_error_response(0x59, request.response_handle_index(), 0x8004_0102);
    };
    if !*columns_set && columns.is_empty() {
        return rop_error_response(0x59, request.response_handle_index(), 0x0000_04B9);
    }
    if *associated || *category_count == 0 || sort_orders.is_empty() {
        return rop_error_response(0x59, request.response_handle_index(), 0x8004_0102);
    }

    let columns = columns.clone();
    let mut source_rows = emails_for_folder(*folder_id, mailboxes, emails);
    source_rows.retain(|email| {
        restriction_matches_email_in_snapshot(restriction.as_ref(), email, *folder_id, snapshot)
    });
    sort_emails(&mut source_rows, sort_orders);
    let rows = categorized_email_rows(
        *folder_id,
        source_rows,
        &columns,
        sort_orders,
        1,
        &HashSet::new(),
    );
    let leaf_rows = rows
        .into_iter()
        .filter(|row| row.leaf && row.category_id == category_id)
        .map(|row| row.row)
        .collect::<Vec<_>>();
    if leaf_rows.is_empty() {
        return rop_error_response(0x59, request.response_handle_index(), 0x8004_010F);
    }
    if *expanded_count != 0 && !collapsed_categories.contains(&category_id) {
        return rop_error_response(0x59, request.response_handle_index(), 0x0000_04F8);
    }

    collapsed_categories.remove(&category_id);
    *expanded_count = (*expanded_count).max(1);
    let max_rows = request.expand_max_row_count();
    let selected = if max_rows == 0 {
        Vec::new()
    } else {
        leaf_rows.iter().take(max_rows).cloned().collect()
    };
    rop_expand_row_success_response(request, leaf_rows.len(), selected)
}

pub(in crate::mapi) fn rop_collapse_row_response(
    request: &RopRequest,
    object: Option<&mut MapiObject>,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
) -> Vec<u8> {
    let Some(category_id) = request.category_id() else {
        return rop_error_response(0x5A, request.response_handle_index(), 0x8004_0102);
    };
    let Some(MapiObject::ContentsTable {
        folder_id,
        associated,
        columns,
        columns_set,
        sort_orders,
        category_count,
        expanded_count,
        collapsed_categories,
        restriction,
        ..
    }) = object
    else {
        return rop_error_response(0x5A, request.response_handle_index(), 0x8004_0102);
    };
    if *associated || *category_count == 0 || sort_orders.is_empty() {
        return rop_error_response(0x5A, request.response_handle_index(), 0x8004_0102);
    }
    if !*columns_set && columns.is_empty() {
        return rop_error_response(0x5A, request.response_handle_index(), 0x0000_04B9);
    }

    let columns = columns.clone();
    let mut source_rows = emails_for_folder(*folder_id, mailboxes, emails);
    source_rows.retain(|email| {
        restriction_matches_email_in_snapshot(restriction.as_ref(), email, *folder_id, snapshot)
    });
    sort_emails(&mut source_rows, sort_orders);
    let rows = categorized_email_rows(
        *folder_id,
        source_rows,
        &columns,
        sort_orders,
        1,
        &HashSet::new(),
    );
    let collapsed_count = rows
        .iter()
        .find(|row| !row.leaf && row.category_id == category_id)
        .map(|row| row.leaf_count)
        .unwrap_or(0);
    if collapsed_count == 0 {
        return rop_error_response(0x5A, request.response_handle_index(), 0x8004_010F);
    }
    if *expanded_count == 0 || collapsed_categories.contains(&category_id) {
        return rop_error_response(0x5A, request.response_handle_index(), 0x0000_04F7);
    }
    collapsed_categories.insert(category_id);
    rop_collapse_row_success_response(request, collapsed_count)
}

pub(in crate::mapi) fn rop_get_collapse_state_response(
    request: &RopRequest,
    object: Option<&MapiObject>,
) -> Vec<u8> {
    let Some(MapiObject::ContentsTable {
        folder_id,
        columns,
        columns_set,
        category_count,
        expanded_count,
        collapsed_categories,
        position,
        ..
    }) = object
    else {
        return rop_error_response(0x6B, request.response_handle_index(), 0x8004_0102);
    };
    if *category_count == 0 {
        return rop_error_response(0x6B, request.response_handle_index(), 0x8004_0102);
    }
    if !*columns_set && columns.is_empty() {
        return rop_error_response(0x6B, request.response_handle_index(), 0x0000_04B9);
    }
    let mut state = Vec::new();
    state.extend_from_slice(COLLAPSE_STATE_MAGIC);
    write_u64(&mut state, *folder_id);
    write_u64(
        &mut state,
        request.collapse_state_row_id().unwrap_or_default(),
    );
    write_u32(&mut state, request.collapse_state_row_instance_number());
    write_u32(&mut state, (*position).min(u32::MAX as usize) as u32);
    write_u16(&mut state, *category_count);
    write_u16(&mut state, *expanded_count);
    write_u16(
        &mut state,
        collapsed_categories.len().min(u16::MAX as usize) as u16,
    );
    for category_id in collapsed_categories.iter().take(u16::MAX as usize) {
        write_u64(&mut state, *category_id);
    }
    rop_get_collapse_state_success_response(request, &state)
}

pub(in crate::mapi) fn rop_set_collapse_state_response(
    request: &RopRequest,
    object: Option<&mut MapiObject>,
) -> Vec<u8> {
    let Some(object) = object else {
        return rop_error_response(0x6C, request.response_handle_index(), 0x8004_0102);
    };
    if !table_columns_are_available(object) {
        return rop_error_response(0x6C, request.response_handle_index(), 0x0000_04B9);
    }
    let state = request.collapse_state();
    if state.len() < 30 || state.get(..6) != Some(COLLAPSE_STATE_MAGIC.as_slice()) {
        return rop_error_response(0x6C, request.response_handle_index(), 0x8004_0102);
    }
    let mut offset = 6;
    let folder_id = read_u64_from(state, &mut offset).unwrap_or_default();
    let _row_id = read_u64_from(state, &mut offset).unwrap_or_default();
    let _row_instance = read_u32_from(state, &mut offset).unwrap_or_default();
    let position = read_u32_from(state, &mut offset).unwrap_or_default() as usize;
    let category_count = read_u16_from(state, &mut offset).unwrap_or_default();
    let expanded_count = read_u16_from(state, &mut offset).unwrap_or_default();
    let collapsed_count = read_u16_from(state, &mut offset).unwrap_or_default() as usize;
    let mut collapsed = HashSet::new();
    for _ in 0..collapsed_count.min(256) {
        if let Some(category_id) = read_u64_from(state, &mut offset) {
            collapsed.insert(category_id);
        }
    }

    let MapiObject::ContentsTable {
        folder_id: table_folder_id,
        category_count: table_category_count,
        expanded_count: table_expanded_count,
        collapsed_categories,
        position: table_position,
        bookmarks,
        next_bookmark,
        ..
    } = object
    else {
        return rop_error_response(0x6C, request.response_handle_index(), 0x8004_0102);
    };
    if *table_folder_id != folder_id || category_count == 0 {
        return rop_error_response(0x6C, request.response_handle_index(), 0x8004_0102);
    }
    *table_category_count = category_count;
    *table_expanded_count = expanded_count;
    *collapsed_categories = collapsed;
    *table_position = position;

    let bookmark_id = *next_bookmark;
    *next_bookmark = next_bookmark.saturating_add(1);
    let bookmark = bookmark_id.to_le_bytes().to_vec();
    bookmarks.insert(
        bookmark.clone(),
        TableBookmark {
            position,
            row_key: None,
        },
    );
    rop_set_collapse_state_success_response(request, &bookmark)
}

fn read_u16_from(bytes: &[u8], offset: &mut usize) -> Option<u16> {
    let value = u16::from_le_bytes(bytes.get(*offset..*offset + 2)?.try_into().ok()?);
    *offset += 2;
    Some(value)
}

fn read_u32_from(bytes: &[u8], offset: &mut usize) -> Option<u32> {
    let value = u32::from_le_bytes(bytes.get(*offset..*offset + 4)?.try_into().ok()?);
    *offset += 4;
    Some(value)
}

fn read_u64_from(bytes: &[u8], offset: &mut usize) -> Option<u64> {
    let value = u64::from_le_bytes(bytes.get(*offset..*offset + 8)?.try_into().ok()?);
    *offset += 8;
    Some(value)
}
