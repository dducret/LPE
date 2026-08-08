---
type: Rust Function
title: resolve_names_requested_values
resource: crates/lpe-exchange/src/mapi/nspi.rs#L325-L329
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/nspi/parse_resolve_names_values
  - functions/crates/lpe-exchange/src/mapi/nspi/scan_address_book_lookup_values
  called_by:
  - functions/crates/lpe-exchange/src/mapi/nspi/resolve_names_response
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_props_response
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_filter_entries_for_request
---

# Signature

`pub(in crate::mapi) fn resolve_names_requested_values(request: &[u8]) -> Vec<String>`

# Calls

- [parse_resolve_names_values](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/parse_resolve_names_values.md)
- [scan_address_book_lookup_values](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/scan_address_book_lookup_values.md)

# Called by

- [resolve_names_response](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/resolve_names_response.md)
- [nspi_props_response](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_props_response.md)
- [nspi_filter_entries_for_request](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_filter_entries_for_request.md)