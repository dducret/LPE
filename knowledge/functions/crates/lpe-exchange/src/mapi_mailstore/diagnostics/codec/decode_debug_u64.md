---
type: Rust Function
title: decode_debug_u64
resource: crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec.rs#L787-L789
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_hierarchy_transfer_debug_summary
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/fast_transfer_value_shape
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_debug_object_id
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_debug_change_number
---

# Signature

`pub(super) fn decode_debug_u64(bytes: &[u8]) -> Option<u64>`

# Called by

- [decode_hierarchy_transfer_debug_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_hierarchy_transfer_debug_summary.md)
- [fast_transfer_value_shape](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/fast_transfer_value_shape.md)
- [decode_debug_object_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_debug_object_id.md)
- [decode_debug_change_number](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_debug_change_number.md)