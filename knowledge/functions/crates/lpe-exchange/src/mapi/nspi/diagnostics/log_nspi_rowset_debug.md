---
type: Rust Function
title: log_nspi_rowset_debug
resource: crates/lpe-exchange/src/mapi/nspi/diagnostics.rs#L167-L223
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_requested_entry_ids
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_stat_current_rec
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_query_rows_count_details
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_query_rows_explicit_entry_ids
  - functions/crates/lpe-exchange/src/mapi/nspi/diagnostics/format_nspi_duplicate_entry_keys_for_debug
  called_by:
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_rowset_response
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_matches_response
---

# Signature

`pub(super) fn log_nspi_rowset_debug( principal: &AccountPrincipal, request: &[u8], request_type: &str, available_entry_count: usize, lookup_values: &[String], tags: &[u32], entries: &[ExchangeAddressBookEntry], row_limit: Option<usize>, )`

# Calls

- [nspi_requested_entry_ids](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_requested_entry_ids.md)
- [nspi_stat_current_rec](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_stat_current_rec.md)
- [nspi_query_rows_count_details](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_query_rows_count_details.md)
- [nspi_query_rows_explicit_entry_ids](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_query_rows_explicit_entry_ids.md)
- [format_nspi_duplicate_entry_keys_for_debug](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/diagnostics/format_nspi_duplicate_entry_keys_for_debug.md)

# Called by

- [nspi_rowset_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_rowset_response.md)
- [nspi_matches_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_matches_response.md)