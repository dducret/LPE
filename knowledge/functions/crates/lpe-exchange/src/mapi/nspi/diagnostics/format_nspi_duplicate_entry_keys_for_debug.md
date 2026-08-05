---
type: Rust Function
title: format_nspi_duplicate_entry_keys_for_debug
resource: crates/lpe-exchange/src/mapi/nspi/diagnostics.rs#L329-L353
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/state/entry
  called_by:
  - functions/crates/lpe-exchange/src/mapi/nspi/diagnostics/log_nspi_rowset_debug
  - functions/crates/lpe-exchange/src/mapi/nspi/tests/nspi_duplicate_debug_groups_rows_by_kind_email_and_name
---

# Signature

`pub(super) fn format_nspi_duplicate_entry_keys_for_debug( entries: &[ExchangeAddressBookEntry], ) -> (usize, String)`

# Calls

- [entry](../../../../../../../functions/crates/lpe-jmap/src/state/entry.md)

# Called by

- [log_nspi_rowset_debug](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/diagnostics/log_nspi_rowset_debug.md)
- [nspi_duplicate_debug_groups_rows_by_kind_email_and_name](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/tests/nspi_duplicate_debug_groups_rows_by_kind_email_and_name.md)