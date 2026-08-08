---
type: Rust Function
title: nspi_request_has_entry_selector
resource: crates/lpe-exchange/src/mapi/nspi.rs#L1204-L1208
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_stat_current_rec
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_direct_entry_id
  - functions/crates/lpe-exchange/src/mapi/nspi/scan_address_book_lookup_values
  called_by:
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_props_response
---

# Signature

`pub(in crate::mapi) fn nspi_request_has_entry_selector(request: &[u8]) -> bool`

# Calls

- [nspi_stat_current_rec](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_stat_current_rec.md)
- [nspi_direct_entry_id](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_direct_entry_id.md)
- [scan_address_book_lookup_values](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/scan_address_book_lookup_values.md)

# Called by

- [nspi_props_response](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_props_response.md)