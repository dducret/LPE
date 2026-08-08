---
type: Rust Function
title: scan_utf16_lookup_values
resource: crates/lpe-exchange/src/mapi/nspi.rs#L1445-L1478
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/nspi/is_utf16_lookup_start
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/nspi/normalize_nspi_lookup_value
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_lookup_value_is_plausible
  called_by:
  - functions/crates/lpe-exchange/src/mapi/nspi/scan_address_book_lookup_values
---

# Signature

`pub(in crate::mapi) fn scan_utf16_lookup_values(request: &[u8]) -> Vec<String>`

# Calls

- [is_utf16_lookup_start](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/is_utf16_lookup_start.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [normalize_nspi_lookup_value](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/normalize_nspi_lookup_value.md)
- [nspi_lookup_value_is_plausible](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_lookup_value_is_plausible.md)

# Called by

- [scan_address_book_lookup_values](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/scan_address_book_lookup_values.md)