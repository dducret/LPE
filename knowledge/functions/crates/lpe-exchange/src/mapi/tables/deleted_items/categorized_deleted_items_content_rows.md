---
type: Rust Function
title: categorized_deleted_items_content_rows
resource: crates/lpe-exchange/src/mapi/tables/deleted_items.rs#L136-L222
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/deleted_items/serialize_deleted_items_content_row
  - functions/crates/lpe-exchange/src/mapi/tables/deleted_items/deleted_items_category_values
  - functions/crates/lpe-exchange/src/mapi/tables/contents/category_id_for_value
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/tables/sorting/apply_sort_direction
  - functions/crates/lpe-exchange/src/mapi/tables/sorting/compare_case_insensitive
  - functions/crates/lpe-exchange/src/mapi/tables/deleted_items/deleted_items_content_row_is_unread
  - functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_category_header_row
  - functions/crates/lpe-exchange/src/mapi/tables/deleted_items/serialize_categorized_deleted_items_content_row
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/collapse/expanded_categorized_rows
  - functions/crates/lpe-exchange/src/mapi/tables/counts/table_position_and_count
  - functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner
---

# Signature

`pub(super) fn categorized_deleted_items_content_rows( rows: Vec<DeletedItemsContentRow<'_>>, snapshot: &MapiMailStoreSnapshot, mailbox_guid: Uuid, columns: &[u32], sort_orders: &[MapiSortOrder], expanded_count: u16, collapsed_categories: &HashSet<u64>, ) -> Vec<CategorizedTableRow>`

# Calls

- [serialize_deleted_items_content_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/deleted_items/serialize_deleted_items_content_row.md)
- [deleted_items_category_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/deleted_items/deleted_items_category_values.md)
- [category_id_for_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/contents/category_id_for_value.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [apply_sort_direction](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/sorting/apply_sort_direction.md)
- [compare_case_insensitive](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/sorting/compare_case_insensitive.md)
- [deleted_items_content_row_is_unread](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/deleted_items/deleted_items_content_row_is_unread.md)
- [serialize_category_header_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_category_header_row.md)
- [serialize_categorized_deleted_items_content_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/deleted_items/serialize_categorized_deleted_items_content_row.md)

# Called by

- [expanded_categorized_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/collapse/expanded_categorized_rows.md)
- [table_position_and_count](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/counts/table_position_and_count.md)
- [rop_query_rows_response_inner](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner.md)