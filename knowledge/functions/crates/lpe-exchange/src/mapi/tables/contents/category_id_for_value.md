---
type: Rust Function
title: category_id_for_value
resource: crates/lpe-exchange/src/mapi/tables/contents.rs#L15-L27
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/contents/categorized_email_rows
  - functions/crates/lpe-exchange/src/mapi/tables/deleted_items/categorized_deleted_items_content_rows
  - functions/crates/lpe-exchange/src/mapi/tables/tests/categorized_table_expand_collapse_require_set_columns
  - functions/crates/lpe-exchange/src/mapi/tables/tests/microsoft_categorized_expand_collapse_report_current_state_errors
---

# Signature

`pub(super) fn category_id_for_value(folder_id: u64, property_tag: u32, value: &str) -> u64`

# Called by

- [categorized_email_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/contents/categorized_email_rows.md)
- [categorized_deleted_items_content_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/deleted_items/categorized_deleted_items_content_rows.md)
- [categorized_table_expand_collapse_require_set_columns](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/categorized_table_expand_collapse_require_set_columns.md)
- [microsoft_categorized_expand_collapse_report_current_state_errors](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/microsoft_categorized_expand_collapse_report_current_state_errors.md)