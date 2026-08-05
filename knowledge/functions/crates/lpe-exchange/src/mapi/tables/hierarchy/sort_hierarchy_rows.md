---
type: Rust Function
title: sort_hierarchy_rows
resource: crates/lpe-exchange/src/mapi/tables/hierarchy.rs#L241-L268
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/sorting/compare_case_insensitive
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_row_display_name
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_row_content_count
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_row_unread_count
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_row_id
  - functions/crates/lpe-exchange/src/mapi/tables/sorting/apply_sort_direction
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_rows_excluding_deleted
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_table_rows_excluding_deleted
---

# Signature

`fn sort_hierarchy_rows(rows: &mut [HierarchyRow<'_>], sort_orders: &[MapiSortOrder])`

# Calls

- [compare_case_insensitive](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/sorting/compare_case_insensitive.md)
- [hierarchy_row_display_name](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_row_display_name.md)
- [hierarchy_row_content_count](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_row_content_count.md)
- [hierarchy_row_unread_count](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_row_unread_count.md)
- [hierarchy_row_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_row_id.md)
- [apply_sort_direction](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/sorting/apply_sort_direction.md)

# Called by

- [hierarchy_rows_excluding_deleted](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_rows_excluding_deleted.md)
- [hierarchy_table_rows_excluding_deleted](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_table_rows_excluding_deleted.md)