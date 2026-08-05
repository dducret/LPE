---
type: Rust Function
title: read_fast_transfer_variable_bytes
resource: crates/lpe-exchange/src/mapi/dispatch/sync_import.rs#L671-L674
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_bytes
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/read_fast_transfer_property_value
---

# Signature

`fn read_fast_transfer_variable_bytes(cursor: &mut Cursor<'_>) -> Result<Vec<u8>>`

# Calls

- [read_bytes](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_bytes.md)

# Called by

- [read_fast_transfer_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/read_fast_transfer_property_value.md)