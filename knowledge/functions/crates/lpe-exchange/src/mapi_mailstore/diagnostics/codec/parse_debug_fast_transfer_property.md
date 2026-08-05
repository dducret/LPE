---
type: Rust Function
title: parse_debug_fast_transfer_property
resource: crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec.rs#L981-L1024
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/read_debug_u32
  - functions/crates/lpe-exchange/src/mapi_mailstore/fast_transfer_property_value_start
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/read_debug_slice
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_hierarchy_transfer_debug_summary
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_content_transfer_fai_debug_summary
---

# Signature

`pub(super) fn parse_debug_fast_transfer_property( bytes: &[u8], offset: usize, ) -> Result<FastTransferDebugProperty, String>`

# Calls

- [read_debug_u32](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/read_debug_u32.md)
- [fast_transfer_property_value_start](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/fast_transfer_property_value_start.md)
- [read_debug_slice](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/read_debug_slice.md)

# Called by

- [decode_hierarchy_transfer_debug_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_hierarchy_transfer_debug_summary.md)
- [decode_content_transfer_fai_debug_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_content_transfer_fai_debug_summary.md)