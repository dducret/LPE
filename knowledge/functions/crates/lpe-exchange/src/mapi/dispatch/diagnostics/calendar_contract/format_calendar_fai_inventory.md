---
type: Rust Function
title: format_calendar_fai_inventory
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/calendar_contract.rs#L193-L230
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/debug_associated_table_rows
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/calendar_contract/format_calendar_view_contract_fingerprint
---

# Signature

`fn format_calendar_fai_inventory(snapshot: &MapiMailStoreSnapshot, account_id: Uuid) -> String`

# Calls

- [debug_associated_table_rows](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/debug_associated_table_rows.md)

# Called by

- [format_calendar_view_contract_fingerprint](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/calendar_contract/format_calendar_view_contract_fingerprint.md)