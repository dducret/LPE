---
type: Rust Function
title: sort_associated_config_messages_for_debug
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/associated_config.rs#L131-L157
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/sorting/compare_case_insensitive
  - functions/crates/lpe-exchange/src/mapi/tables/sorting/apply_sort_direction
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/format_inbox_associated_prefix_find_summary
---

# Signature

`pub(in crate::mapi::dispatch) fn sort_associated_config_messages_for_debug( rows: &mut [crate::mapi_store::MapiAssociatedConfigMessage], sort_orders: &[MapiSortOrder], )`

# Calls

- [compare_case_insensitive](../../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/sorting/compare_case_insensitive.md)
- [apply_sort_direction](../../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/sorting/apply_sort_direction.md)

# Called by

- [format_inbox_associated_prefix_find_summary](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/format_inbox_associated_prefix_find_summary.md)