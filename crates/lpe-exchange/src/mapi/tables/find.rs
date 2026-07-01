use super::*;

pub(super) fn rop_find_row_no_match_response(request: &RopRequest) -> Vec<u8> {
    rop_error_response(0x4F, request.response_handle_index(), 0x8004_010F)
}

pub(super) fn find_row_request_is_valid(request: &RopRequest) -> bool {
    request
        .payload
        .first()
        .is_some_and(|flags| flags & !0x01 == 0)
}

pub(in crate::mapi) fn find_row<'a, T>(
    rows: &'a [&'a T],
    current_position: usize,
    request: &RopRequest,
    matches: impl Fn(&T) -> bool,
) -> Option<(usize, &'a T)> {
    if rows.is_empty() {
        return None;
    }
    let start = match request.find_origin().unwrap_or(1) {
        0 => 0,
        2 => rows.len().saturating_sub(1),
        _ => current_position.min(rows.len()),
    };
    if request.find_backward() {
        let end = start.min(rows.len().saturating_sub(1));
        (0..=end)
            .rev()
            .find_map(|index| matches(rows[index]).then_some((index, rows[index])))
    } else {
        rows.iter()
            .enumerate()
            .skip(start)
            .find_map(|(index, row)| matches(row).then_some((index, *row)))
    }
}

pub(super) fn find_hierarchy_row<'a>(
    rows: &'a [HierarchyRow<'a>],
    mailboxes: &[JmapMailbox],
    current_position: usize,
    request: &RopRequest,
    restriction: Option<&MapiRestriction>,
    mailbox_guid: Uuid,
) -> Option<(usize, HierarchyRow<'a>)> {
    if rows.is_empty() {
        return None;
    }
    let start = match request.find_origin().unwrap_or(1) {
        0 => 0,
        2 => rows.len().saturating_sub(1),
        _ => current_position.min(rows.len()),
    };
    if request.find_backward() {
        let end = start.min(rows.len().saturating_sub(1));
        (0..=end).rev().find_map(|index| {
            hierarchy_row_matches(&rows[index], mailboxes, restriction, mailbox_guid)
                .then_some((index, rows[index]))
        })
    } else {
        rows.iter()
            .enumerate()
            .skip(start)
            .find_map(|(index, row)| {
                hierarchy_row_matches(row, mailboxes, restriction, mailbox_guid)
                    .then_some((index, *row))
            })
    }
}
