---
type: Rust Function
title: format_debug_sort_orders
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics.rs#L1069-L1075
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/calendar_contract/format_calendar_view_contract_fingerprint
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_open/append_open_table_response
---

# Signature

`pub(super) fn format_debug_sort_orders(sort_orders: &[MapiSortOrder]) -> String`

# Called by

- [format_calendar_view_contract_fingerprint](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/calendar_contract/format_calendar_view_contract_fingerprint.md)
- [append_open_table_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_open/append_open_table_response.md)