---
type: Rust Function
title: split_rop_payload_legacy
resource: crates/lpe-exchange/src/mapi/rop/buffer.rs#L192-L201
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/split_rop_payload_best_effort
---

# Signature

`pub(in crate::mapi) fn split_rop_payload_legacy(buffer: &[u8]) -> Option<(&[u8], &[u8])>`

# Called by

- [split_rop_payload_best_effort](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/split_rop_payload_best_effort.md)