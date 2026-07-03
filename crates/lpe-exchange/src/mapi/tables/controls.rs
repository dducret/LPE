use super::*;

pub(in crate::mapi) fn rop_query_columns_all_response(
    request: &RopRequest,
    object: Option<&MapiObject>,
    snapshot: &MapiMailStoreSnapshot,
) -> Vec<u8> {
    let columns = match object {
        Some(MapiObject::HierarchyTable { .. }) => default_folder_property_tags(),
        Some(MapiObject::ContentsTable {
            folder_id,
            associated,
            ..
        }) => {
            if *associated && *folder_id == COMMON_VIEWS_FOLDER_ID {
                default_navigation_shortcut_property_tags()
            } else if *associated && *folder_id == FREEBUSY_DATA_FOLDER_ID {
                default_message_property_tags()
            } else if *associated
                && snapshot
                    .collaboration_folder_for_id(*folder_id)
                    .is_some_and(|folder| folder.kind == MapiCollaborationFolderKind::Calendar)
            {
                default_calendar_configuration_property_tags()
            } else if *associated && should_use_associated_config_table(*folder_id, snapshot, None)
            {
                default_associated_config_columns()
            } else {
                match snapshot
                    .collaboration_folder_for_id(*folder_id)
                    .map(|folder| folder.kind)
                {
                    Some(MapiCollaborationFolderKind::Contacts) => default_contact_property_tags(),
                    Some(MapiCollaborationFolderKind::Calendar) => default_event_property_tags(),
                    Some(MapiCollaborationFolderKind::Task) => default_task_property_tags(),
                    None if *folder_id == CONTACTS_SEARCH_FOLDER_ID => {
                        default_contact_property_tags()
                    }
                    None if *folder_id == TODO_SEARCH_FOLDER_ID => default_task_property_tags(),
                    None if *folder_id == TRACKED_MAIL_PROCESSING_FOLDER_ID => {
                        default_message_property_tags()
                    }
                    None if *folder_id == REMINDERS_FOLDER_ID => default_reminder_property_tags(),
                    None if *folder_id == NOTES_FOLDER_ID => default_note_property_tags(),
                    None if *folder_id == JOURNAL_FOLDER_ID => {
                        default_journal_entry_property_tags()
                    }
                    None => default_message_property_tags(),
                }
            }
        }
        Some(MapiObject::AttachmentTable { .. }) => default_attachment_columns(),
        Some(MapiObject::PermissionTable { .. }) => default_permission_columns(),
        Some(MapiObject::RuleTable { .. }) => default_rule_columns(),
        _ => return rop_error_response(0x37, request.response_handle_index(), 0x8004_0102),
    };

    let mut response = vec![0x37, request.response_handle_index()];
    write_u32(&mut response, 0);
    response.extend_from_slice(&(columns.len() as u16).to_le_bytes());
    for column in columns {
        response.extend_from_slice(&column.to_le_bytes());
    }
    response
}

pub(in crate::mapi) fn rop_get_status_response(
    request: &RopRequest,
    object: Option<&MapiObject>,
) -> Vec<u8> {
    if !object.is_some_and(is_table_object) {
        return rop_error_response(0x16, request.response_handle_index(), 0x8004_0102);
    }

    let mut response = vec![0x16, request.response_handle_index()];
    write_u32(&mut response, 0);
    response.push(0);
    response
}

pub(in crate::mapi) fn rop_query_position_response(
    request: &RopRequest,
    object: Option<&MapiObject>,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    mailbox_guid: Uuid,
) -> Vec<u8> {
    if !object.is_some_and(is_table_object) {
        return rop_error_response(0x17, request.response_handle_index(), 0x8004_0102);
    }

    let (position, row_count) =
        table_position_and_count(object, mailboxes, emails, snapshot, mailbox_guid);
    let mut response = vec![0x17, request.response_handle_index()];
    write_u32(&mut response, 0);
    write_u32(&mut response, position as u32);
    write_u32(&mut response, row_count as u32);
    response
}

pub(in crate::mapi) fn rop_seek_row_response(
    request: &RopRequest,
    object: Option<&mut MapiObject>,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    mailbox_guid: Uuid,
) -> Vec<u8> {
    if !seek_row_request_is_valid(request) {
        return rop_error_response(0x18, request.response_handle_index(), 0x8007_0057);
    }
    let Some(object) = object else {
        return rop_error_response(0x18, request.response_handle_index(), 0x8004_0102);
    };
    let (current_position, total_rows) =
        table_position_and_count(Some(object), mailboxes, emails, snapshot, mailbox_guid);
    let Some(position) = table_position_mut(object) else {
        return rop_error_response(0x18, request.response_handle_index(), 0x8004_0102);
    };

    let requested_rows = request.seek_row_count().unwrap_or(0);
    let base_position = match request.seek_origin().unwrap_or(1) {
        0 => 0isize,
        2 => total_rows as isize,
        _ => current_position as isize,
    };
    let requested_position = base_position.saturating_add(requested_rows as isize);
    let new_position = requested_position.clamp(0, total_rows as isize);
    let rows_sought = (new_position - base_position) as i32;
    *position = new_position as usize;

    let mut response = vec![0x18, request.response_handle_index()];
    write_u32(&mut response, 0);
    let want_row_moved_count = request.want_row_moved_count();
    response.push((want_row_moved_count && rows_sought != requested_rows) as u8);
    response.extend_from_slice(&if want_row_moved_count { rows_sought } else { 0 }.to_le_bytes());
    response
}

fn seek_row_request_is_valid(request: &RopRequest) -> bool {
    matches!(request.payload.first().copied(), Some(0x00..=0x02))
        && matches!(request.payload.get(5).copied(), Some(0x00 | 0x01))
        && request.payload.get(1..5).is_some()
}

pub(in crate::mapi) fn rop_create_bookmark_response(
    request: &RopRequest,
    object: Option<&mut MapiObject>,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    mailbox_guid: Uuid,
) -> Vec<u8> {
    let Some(object) = object else {
        return rop_error_response(0x1B, request.response_handle_index(), 0x8004_0102);
    };
    let row_key = table_row_keys(object, mailboxes, emails, snapshot, mailbox_guid)
        .get(table_position(object).unwrap_or(0))
        .copied()
        .filter(|row_key| *row_key != 0);
    let Some((position, bookmarks, next_bookmark)) = table_bookmark_state_mut(object) else {
        return rop_error_response(0x1B, request.response_handle_index(), 0x8004_0102);
    };
    let bookmark = next_bookmark.to_le_bytes().to_vec();
    bookmarks.insert(
        bookmark.clone(),
        TableBookmark {
            position: *position,
            row_key,
        },
    );
    *next_bookmark = next_bookmark.saturating_add(1).max(1);

    let mut response = vec![0x1B, request.response_handle_index()];
    write_u32(&mut response, 0);
    response.extend_from_slice(&(bookmark.len() as u16).to_le_bytes());
    response.extend_from_slice(&bookmark);
    response
}

pub(in crate::mapi) fn rop_seek_row_bookmark_response(
    request: &RopRequest,
    object: Option<&mut MapiObject>,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    mailbox_guid: Uuid,
) -> Vec<u8> {
    if !seek_row_bookmark_request_is_valid(request) {
        return rop_error_response(0x19, request.response_handle_index(), 0x8007_0057);
    }
    let Some(object) = object else {
        return rop_error_response(0x19, request.response_handle_index(), 0x8004_0102);
    };
    if !table_columns_are_available(object) {
        return rop_error_response(0x19, request.response_handle_index(), 0x0000_04B9);
    }
    let row_keys = table_row_keys(object, mailboxes, emails, snapshot, mailbox_guid);
    let total_rows = row_keys.len();
    let Some((position, bookmarks, _next_bookmark)) = table_bookmark_state_mut(object) else {
        return rop_error_response(0x19, request.response_handle_index(), 0x8004_0102);
    };
    let Some(bookmark) = bookmarks.get(request.bookmark()).cloned() else {
        return rop_error_response(0x19, request.response_handle_index(), 0x8004_0405);
    };
    let mut row_no_longer_visible = false;
    let base_position = match bookmark.row_key {
        Some(row_key) => row_keys
            .iter()
            .position(|key| *key == row_key)
            .unwrap_or_else(|| {
                if !row_keys.contains(&0) {
                    row_no_longer_visible = true;
                }
                bookmark.position.min(total_rows)
            }),
        None => bookmark.position.min(total_rows),
    };

    let requested_rows = request.bookmark_row_count().unwrap_or(0);
    let requested_position = (base_position as isize).saturating_add(requested_rows as isize);
    let new_position = requested_position.clamp(0, total_rows as isize);
    let rows_sought = (new_position - base_position as isize) as i32;
    *position = new_position as usize;

    let mut response = vec![0x19, request.response_handle_index()];
    write_u32(&mut response, 0);
    response.push(row_no_longer_visible as u8);
    response.push((request.bookmark_want_row_moved_count() && rows_sought != requested_rows) as u8);
    response.extend_from_slice(
        &if request.bookmark_want_row_moved_count() {
            rows_sought
        } else {
            0
        }
        .to_le_bytes(),
    );
    response
}

fn seek_row_bookmark_request_is_valid(request: &RopRequest) -> bool {
    let Some(size) = request
        .payload
        .get(..2)
        .and_then(|bytes| bytes.try_into().ok())
        .map(u16::from_le_bytes)
        .map(usize::from)
    else {
        return false;
    };
    request.payload.get(2..2 + size).is_some()
        && request.payload.get(2 + size..6 + size).is_some()
        && matches!(request.payload.get(6 + size).copied(), Some(0x00 | 0x01))
}

pub(in crate::mapi) fn rop_seek_row_fractional_response(
    request: &RopRequest,
    object: Option<&mut MapiObject>,
    mailboxes: &[JmapMailbox],
    emails: &[JmapEmail],
    snapshot: &MapiMailStoreSnapshot,
    mailbox_guid: Uuid,
) -> Vec<u8> {
    if !seek_row_fractional_request_is_valid(request) {
        return rop_error_response(0x1A, request.response_handle_index(), 0x8007_0057);
    }
    let Some(object) = object else {
        return rop_error_response(0x1A, request.response_handle_index(), 0x8004_0102);
    };
    let total_rows =
        table_position_and_count(Some(object), mailboxes, emails, snapshot, mailbox_guid).1;
    let Some(position) = table_position_mut(object) else {
        return rop_error_response(0x1A, request.response_handle_index(), 0x8004_0102);
    };
    let (numerator, denominator) = request.fractional_position().unwrap_or((0, 1));
    let new_position = (total_rows as u128)
        .saturating_mul(numerator as u128)
        .checked_div(denominator as u128)
        .unwrap_or(0)
        .min(total_rows as u128) as usize;
    *position = new_position;

    let mut response = vec![0x1A, request.response_handle_index()];
    write_u32(&mut response, 0);
    response
}

fn seek_row_fractional_request_is_valid(request: &RopRequest) -> bool {
    request
        .fractional_position()
        .is_some_and(|(_numerator, denominator)| denominator != 0)
}

pub(in crate::mapi) fn rop_free_bookmark_response(
    request: &RopRequest,
    object: Option<&mut MapiObject>,
) -> Vec<u8> {
    let Some(object) = object else {
        return rop_error_response(0x89, request.response_handle_index(), 0x8004_0102);
    };
    if !table_columns_are_available(object) {
        return rop_error_response(0x89, request.response_handle_index(), 0x0000_04B9);
    }
    let Some((_position, bookmarks, _next_bookmark)) = table_bookmark_state_mut(object) else {
        return rop_error_response(0x89, request.response_handle_index(), 0x8004_0102);
    };
    bookmarks.remove(request.bookmark());

    let mut response = vec![0x89, request.response_handle_index()];
    write_u32(&mut response, 0);
    response
}
