---
type: Rust Function
title: decode_fast_transfer_utf16
resource: crates/lpe-exchange/src/mapi/dispatch/sync_import.rs#L681-L693
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/read_fast_transfer_property_value
---

# Signature

`fn decode_fast_transfer_utf16(bytes: &[u8]) -> Result<String>`

# Called by

- [read_fast_transfer_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/read_fast_transfer_property_value.md)