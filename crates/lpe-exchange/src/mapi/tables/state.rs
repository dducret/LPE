use super::*;

pub(in crate::mapi) fn is_table_object(object: &MapiObject) -> bool {
    matches!(
        object,
        MapiObject::HierarchyTable { .. }
            | MapiObject::ContentsTable { .. }
            | MapiObject::AttachmentTable { .. }
            | MapiObject::PermissionTable { .. }
            | MapiObject::RuleTable { .. }
    )
}

pub(super) fn table_columns_are_available(object: &MapiObject) -> bool {
    match object {
        MapiObject::HierarchyTable {
            columns,
            columns_set,
            sort_orders,
            restriction,
            ..
        }
        | MapiObject::ContentsTable {
            columns,
            columns_set,
            sort_orders,
            restriction,
            ..
        }
        | MapiObject::AttachmentTable {
            columns,
            columns_set,
            sort_orders,
            restriction,
            ..
        } => {
            (*columns_set || !columns.is_empty())
                && !table_sort_is_invalid(sort_orders)
                && !table_restriction_is_invalid(restriction.as_ref())
        }
        MapiObject::PermissionTable {
            columns,
            columns_set,
            ..
        } => *columns_set || !columns.is_empty(),
        MapiObject::RuleTable {
            columns,
            columns_set,
            ..
        } => *columns_set || !columns.is_empty(),
        _ => false,
    }
}

pub(in crate::mapi) fn invalid_table_sort_orders() -> Vec<MapiSortOrder> {
    vec![MapiSortOrder {
        property_tag: 0,
        order: u8::MAX,
    }]
}

fn table_sort_is_invalid(sort_orders: &[MapiSortOrder]) -> bool {
    sort_orders
        .first()
        .is_some_and(|sort| sort.property_tag == 0 && sort.order == u8::MAX)
}

fn table_restriction_is_invalid(restriction: Option<&MapiRestriction>) -> bool {
    matches!(restriction, Some(MapiRestriction::InvalidTableRestriction))
}

pub(in crate::mapi) fn table_position_mut(object: &mut MapiObject) -> Option<&mut usize> {
    match object {
        MapiObject::HierarchyTable { position, .. }
        | MapiObject::ContentsTable { position, .. }
        | MapiObject::AttachmentTable { position, .. }
        | MapiObject::PermissionTable { position, .. }
        | MapiObject::RuleTable { position, .. } => Some(position),
        _ => None,
    }
}

pub(in crate::mapi) fn table_position(object: &MapiObject) -> Option<usize> {
    match object {
        MapiObject::HierarchyTable { position, .. }
        | MapiObject::ContentsTable { position, .. }
        | MapiObject::AttachmentTable { position, .. }
        | MapiObject::PermissionTable { position, .. }
        | MapiObject::RuleTable { position, .. } => Some(*position),
        _ => None,
    }
}

pub(in crate::mapi) fn table_bookmark_state_mut(
    object: &mut MapiObject,
) -> Option<(&mut usize, &mut HashMap<Vec<u8>, TableBookmark>, &mut u32)> {
    match object {
        MapiObject::HierarchyTable {
            position,
            bookmarks,
            next_bookmark,
            ..
        }
        | MapiObject::ContentsTable {
            position,
            bookmarks,
            next_bookmark,
            ..
        } => Some((position, bookmarks, next_bookmark)),
        _ => None,
    }
}

pub(super) fn selected_row_indexes(
    row_len: usize,
    start_position: usize,
    forward_read: bool,
    requested_row_count: usize,
) -> Vec<usize> {
    let row_count = requested_row_count.min(row_len);
    if forward_read {
        return (start_position.min(row_len)..row_len)
            .take(row_count)
            .collect();
    }
    let end_position = start_position.min(row_len);
    let selected_start = end_position.saturating_sub(row_count);
    (selected_start..end_position).rev().collect()
}
