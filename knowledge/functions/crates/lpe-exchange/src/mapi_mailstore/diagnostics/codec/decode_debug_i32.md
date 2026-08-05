---
type: Rust Function
title: decode_debug_i32
resource: crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec.rs#L779-L781
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_hierarchy_transfer_debug_summary
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_content_transfer_fai_debug_summary
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/fast_transfer_value_shape
---

# Signature

`pub(super) fn decode_debug_i32(bytes: &[u8]) -> Option<i32>`

# Called by

- [decode_hierarchy_transfer_debug_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_hierarchy_transfer_debug_summary.md)
- [decode_content_transfer_fai_debug_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_content_transfer_fai_debug_summary.md)
- [fast_transfer_value_shape](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/fast_transfer_value_shape.md)