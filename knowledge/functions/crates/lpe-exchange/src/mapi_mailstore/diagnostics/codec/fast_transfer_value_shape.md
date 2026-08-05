---
type: Rust Function
title: fast_transfer_value_shape
resource: crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec.rs#L495-L527
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_debug_i16
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_debug_i32
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_debug_bool
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_debug_u64
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_debug_string8z
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_debug_utf16z
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_content_transfer_fai_debug_summary
---

# Signature

`pub(super) fn fast_transfer_value_shape(tag: u32, value: &[u8]) -> String`

# Calls

- [decode_debug_i16](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_debug_i16.md)
- [decode_debug_i32](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_debug_i32.md)
- [decode_debug_bool](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_debug_bool.md)
- [decode_debug_u64](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_debug_u64.md)
- [decode_debug_string8z](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_debug_string8z.md)
- [decode_debug_utf16z](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_debug_utf16z.md)

# Called by

- [decode_content_transfer_fai_debug_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_content_transfer_fai_debug_summary.md)