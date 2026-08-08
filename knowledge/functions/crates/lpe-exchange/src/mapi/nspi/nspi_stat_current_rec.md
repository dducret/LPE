---
type: Rust Function
title: nspi_stat_current_rec
resource: crates/lpe-exchange/src/mapi/nspi.rs#L1368-L1381
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_word_looks_like_entry_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_props_response
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_request_has_entry_selector
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_requested_entry_ids
  - functions/crates/lpe-exchange/src/mapi/nspi/diagnostics/log_nspi_get_props_debug
  - functions/crates/lpe-exchange/src/mapi/nspi/diagnostics/log_nspi_rowset_debug
---

# Signature

`fn nspi_stat_current_rec(request: &[u8]) -> Option<u32>`

# Calls

- [nspi_word_looks_like_entry_id](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_word_looks_like_entry_id.md)

# Called by

- [nspi_props_response](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_props_response.md)
- [nspi_request_has_entry_selector](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_request_has_entry_selector.md)
- [nspi_requested_entry_ids](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_requested_entry_ids.md)
- [log_nspi_get_props_debug](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/diagnostics/log_nspi_get_props_debug.md)
- [log_nspi_rowset_debug](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/diagnostics/log_nspi_rowset_debug.md)