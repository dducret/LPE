---
type: Rust Function
title: scan_address_book_lookup_values
resource: crates/lpe-exchange/src/mapi/nspi.rs#L1419-L1426
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/nspi/scan_ascii_lookup_values
  - functions/crates/lpe-exchange/src/mapi/nspi/scan_utf16_lookup_values
  called_by:
  - functions/crates/lpe-exchange/src/mapi/nspi/resolve_names_requested_values
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_rowset_response
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_matches_response
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_requested_entry
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_request_has_entry_selector
---

# Signature

`pub(in crate::mapi) fn scan_address_book_lookup_values(request: &[u8]) -> Vec<String>`

# Calls

- [scan_ascii_lookup_values](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/scan_ascii_lookup_values.md)
- [scan_utf16_lookup_values](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/scan_utf16_lookup_values.md)

# Called by

- [resolve_names_requested_values](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/resolve_names_requested_values.md)
- [nspi_rowset_response](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_rowset_response.md)
- [nspi_matches_response](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_matches_response.md)
- [nspi_requested_entry](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_requested_entry.md)
- [nspi_request_has_entry_selector](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_request_has_entry_selector.md)