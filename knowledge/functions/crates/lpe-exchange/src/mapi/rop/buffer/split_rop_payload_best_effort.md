---
type: Rust Function
title: split_rop_payload_best_effort
resource: crates/lpe-exchange/src/mapi/rop/buffer.rs#L164-L179
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/split_rop_payload_spec
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/split_rop_payload_legacy
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/split_rop_buffer
---

# Signature

`fn split_rop_payload_best_effort(buffer: &[u8]) -> Option<(&[u8], &[u8])>`

# Calls

- [split_rop_payload_spec](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/split_rop_payload_spec.md)
- [split_rop_payload_legacy](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/split_rop_payload_legacy.md)

# Called by

- [split_rop_buffer](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/split_rop_buffer.md)