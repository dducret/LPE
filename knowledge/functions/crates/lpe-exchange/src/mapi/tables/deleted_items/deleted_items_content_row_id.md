---
type: Rust Function
title: deleted_items_content_row_id
resource: crates/lpe-exchange/src/mapi/tables/deleted_items.rs#L84-L91
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/mapped_mapi_object_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/deleted_items/sort_deleted_items_content_rows
  - functions/crates/lpe-exchange/src/mapi/tables/row_keys/table_row_keys
---

# Signature

`pub(super) fn deleted_items_content_row_id(row: &DeletedItemsContentRow<'_>) -> u64`

# Calls

- [mapped_mapi_object_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/mapped_mapi_object_id.md)

# Called by

- [sort_deleted_items_content_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/deleted_items/sort_deleted_items_content_rows.md)
- [table_row_keys](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/row_keys/table_row_keys.md)