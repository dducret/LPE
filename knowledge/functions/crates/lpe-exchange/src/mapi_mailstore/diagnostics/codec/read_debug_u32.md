---
type: Rust Function
title: read_debug_u32
resource: crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec.rs#L1026-L1029
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/read_debug_slice
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_hierarchy_transfer_debug_summary
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_content_transfer_fai_debug_summary
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/parse_debug_fast_transfer_property
---

# Signature

`pub(super) fn read_debug_u32(bytes: &[u8], offset: usize) -> Result<u32, String>`

# Calls

- [read_debug_slice](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/read_debug_slice.md)

# Called by

- [decode_hierarchy_transfer_debug_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_hierarchy_transfer_debug_summary.md)
- [decode_content_transfer_fai_debug_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_content_transfer_fai_debug_summary.md)
- [parse_debug_fast_transfer_property](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/parse_debug_fast_transfer_property.md)