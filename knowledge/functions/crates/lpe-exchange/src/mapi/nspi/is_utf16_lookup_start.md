---
type: Rust Function
title: is_utf16_lookup_start
resource: crates/lpe-exchange/src/mapi/nspi.rs#L1480-L1486
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/nspi/scan_utf16_lookup_values
---

# Signature

`pub(in crate::mapi) fn is_utf16_lookup_start(request: &[u8], start: usize) -> bool`

# Called by

- [scan_utf16_lookup_values](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/scan_utf16_lookup_values.md)