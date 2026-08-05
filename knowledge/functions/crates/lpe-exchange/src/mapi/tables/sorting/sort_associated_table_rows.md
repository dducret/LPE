---
type: Rust Function
title: sort_associated_table_rows
resource: crates/lpe-exchange/src/mapi/tables/sorting.rs#L137-L159
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_table_row_property_value
  - functions/crates/lpe-exchange/src/mapi/tables/sorting/compare_optional_mapi_values
  - functions/crates/lpe-exchange/src/mapi/tables/sorting/apply_sort_direction
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_table_row_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response
  - functions/crates/lpe-exchange/src/mapi/tables/diagnostics/outlook_bootstrap_row_invariant_summaries
  - functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner
  - functions/crates/lpe-exchange/src/mapi/tables/tests/microsoft_oxocfg_associated_config_sort_uses_persisted_last_modification_time
---

# Signature

`pub(super) fn sort_associated_table_rows( rows: &mut [AssociatedTableRow], sort_orders: &[MapiSortOrder], mailbox_guid: Uuid, )`

# Calls

- [associated_table_row_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_table_row_property_value.md)
- [compare_optional_mapi_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/sorting/compare_optional_mapi_values.md)
- [apply_sort_direction](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/sorting/apply_sort_direction.md)
- [associated_table_row_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_table_row_id.md)

# Called by

- [rop_find_row_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response.md)
- [outlook_bootstrap_row_invariant_summaries](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/diagnostics/outlook_bootstrap_row_invariant_summaries.md)
- [rop_query_rows_response_inner](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner.md)
- [microsoft_oxocfg_associated_config_sort_uses_persisted_last_modification_time](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/microsoft_oxocfg_associated_config_sort_uses_persisted_last_modification_time.md)