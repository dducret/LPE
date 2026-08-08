---
type: Rust Function
title: scan_ascii_lookup_values
resource: crates/lpe-exchange/src/mapi/nspi.rs#L1431-L1443
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/nspi/normalize_nspi_lookup_value
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_lookup_value_is_plausible
  called_by:
  - functions/crates/lpe-exchange/src/mapi/nspi/scan_address_book_lookup_values
---

# Signature

`pub(in crate::mapi) fn scan_ascii_lookup_values(request: &[u8]) -> Vec<String>`

# Calls

- [normalize_nspi_lookup_value](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/normalize_nspi_lookup_value.md)
- [nspi_lookup_value_is_plausible](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_lookup_value_is_plausible.md)

# Called by

- [scan_address_book_lookup_values](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/scan_address_book_lookup_values.md)