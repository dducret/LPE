---
type: Rust Function
title: should_use_associated_config_table
resource: crates/lpe-exchange/src/mapi/tables/associated_contents.rs#L164-L171
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/has_associated_table_rows
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/is_inbox_exact_rule_organizer_restriction
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response
  - functions/crates/lpe-exchange/src/mapi/tables/controls/rop_query_columns_all_response
  - functions/crates/lpe-exchange/src/mapi/tables/query/query_rows_response_columns
  - functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner
---

# Signature

`pub(super) fn should_use_associated_config_table( folder_id: u64, snapshot: &MapiMailStoreSnapshot, restriction: Option<&MapiRestriction>, ) -> bool`

# Calls

- [has_associated_table_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/has_associated_table_rows.md)
- [is_inbox_exact_rule_organizer_restriction](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/is_inbox_exact_rule_organizer_restriction.md)

# Called by

- [rop_find_row_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response.md)
- [rop_query_columns_all_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/controls/rop_query_columns_all_response.md)
- [query_rows_response_columns](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/query/query_rows_response_columns.md)
- [rop_query_rows_response_inner](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner.md)