---
type: Rust Function
title: append_rop_sync_manifest_get_buffer_with_state_and_flags
resource: crates/lpe-exchange/src/tests/mod.rs#L15159-L15210
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/tests/append_rop_sync_manifest_get_buffer_with_state
  - functions/crates/lpe-exchange/src/tests/content_sync_response_rops_for_store_with_flags
---

# Signature

`fn append_rop_sync_manifest_get_buffer_with_state_and_flags( rops: &mut Vec<u8>, input: u8, output: u8, buffer_size: u16, state: &[u8], synchronization_flags: u16, )`

# Called by

- [append_rop_sync_manifest_get_buffer_with_state](../../../../../functions/crates/lpe-exchange/src/tests/append_rop_sync_manifest_get_buffer_with_state.md)
- [content_sync_response_rops_for_store_with_flags](../../../../../functions/crates/lpe-exchange/src/tests/content_sync_response_rops_for_store_with_flags.md)