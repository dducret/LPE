---
type: Rust Function
title: read_strict_u32
resource: crates/lpe-exchange/src/tests/mod.rs#L13698-L13701
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/read_strict_slice
  called_by:
  - functions/crates/lpe-exchange/src/tests/strict_decode_hierarchy_sync_stream
  - functions/crates/lpe-exchange/src/tests/strict_parse_fast_transfer_property
  - functions/crates/lpe-exchange/src/tests/strict_decode_content_sync_stream
---

# Signature

`fn read_strict_u32(bytes: &[u8], offset: usize) -> Result<u32, String>`

# Calls

- [read_strict_slice](../../../../../functions/crates/lpe-exchange/src/tests/read_strict_slice.md)

# Called by

- [strict_decode_hierarchy_sync_stream](../../../../../functions/crates/lpe-exchange/src/tests/strict_decode_hierarchy_sync_stream.md)
- [strict_parse_fast_transfer_property](../../../../../functions/crates/lpe-exchange/src/tests/strict_parse_fast_transfer_property.md)
- [strict_decode_content_sync_stream](../../../../../functions/crates/lpe-exchange/src/tests/strict_decode_content_sync_stream.md)