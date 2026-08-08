---
type: Rust Function
title: nspi_lookup_value_is_plausible
resource: crates/lpe-exchange/src/mapi/nspi.rs#L1504-L1552
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/Parser/next
  called_by:
  - functions/crates/lpe-exchange/src/mapi/nspi/scan_ascii_lookup_values
  - functions/crates/lpe-exchange/src/mapi/nspi/scan_utf16_lookup_values
---

# Signature

`fn nspi_lookup_value_is_plausible(value: &str) -> bool`

# Calls

- [next](../../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)

# Called by

- [scan_ascii_lookup_values](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/scan_ascii_lookup_values.md)
- [scan_utf16_lookup_values](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/scan_utf16_lookup_values.md)