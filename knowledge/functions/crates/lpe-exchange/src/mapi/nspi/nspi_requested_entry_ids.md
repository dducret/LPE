---
type: Rust Function
title: nspi_requested_entry_ids
resource: crates/lpe-exchange/src/mapi/nspi.rs#L1348-L1360
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_stat_current_rec
  - functions/crates/lpe-exchange/src/mapi/nspi/push_unique_nspi_entry_id
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_direct_entry_id
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_query_rows_explicit_entry_ids
  called_by:
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_props_response
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_requested_entry
  - functions/crates/lpe-exchange/src/mapi/nspi/diagnostics/log_nspi_get_props_debug
  - functions/crates/lpe-exchange/src/mapi/nspi/diagnostics/log_nspi_rowset_debug
---

# Signature

`pub(in crate::mapi) fn nspi_requested_entry_ids(request: &[u8]) -> Vec<u32>`

# Calls

- [nspi_stat_current_rec](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_stat_current_rec.md)
- [push_unique_nspi_entry_id](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/push_unique_nspi_entry_id.md)
- [nspi_direct_entry_id](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_direct_entry_id.md)
- [nspi_query_rows_explicit_entry_ids](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_query_rows_explicit_entry_ids.md)

# Called by

- [nspi_props_response](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_props_response.md)
- [nspi_requested_entry](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_requested_entry.md)
- [log_nspi_get_props_debug](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/diagnostics/log_nspi_get_props_debug.md)
- [log_nspi_rowset_debug](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/diagnostics/log_nspi_rowset_debug.md)