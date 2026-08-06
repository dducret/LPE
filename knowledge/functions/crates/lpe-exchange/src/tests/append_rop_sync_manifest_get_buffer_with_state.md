---
type: Rust Function
title: append_rop_sync_manifest_get_buffer_with_state
resource: crates/lpe-exchange/src/tests/mod.rs#L15210-L15225
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/append_rop_sync_manifest_get_buffer_with_state_and_flags
  called_by:
  - functions/crates/lpe-exchange/src/tests/append_rop_sync_manifest_get_buffer
---

# Signature

`fn append_rop_sync_manifest_get_buffer_with_state( rops: &mut Vec<u8>, input: u8, output: u8, buffer_size: u16, state: &[u8], )`

# Calls

- [append_rop_sync_manifest_get_buffer_with_state_and_flags](../../../../../functions/crates/lpe-exchange/src/tests/append_rop_sync_manifest_get_buffer_with_state_and_flags.md)

# Called by

- [append_rop_sync_manifest_get_buffer](../../../../../functions/crates/lpe-exchange/src/tests/append_rop_sync_manifest_get_buffer.md)