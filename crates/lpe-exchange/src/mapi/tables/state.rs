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
