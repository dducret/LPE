---
type: Rust Function
title: strict_decode_i32_property
resource: crates/lpe-exchange/src/tests/mod.rs#L13163-L13173
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/tests/strict_decode_content_sync_stream
  - functions/crates/lpe-exchange/src/tests/strict_record_content_header_property
  - functions/crates/lpe-exchange/src/tests/strict_record_content_body_property
---

# Signature

`fn strict_decode_i32_property(property: &StrictFastTransferProperty) -> Result<i32, String>`

# Called by

- [strict_decode_content_sync_stream](../../../../../functions/crates/lpe-exchange/src/tests/strict_decode_content_sync_stream.md)
- [strict_record_content_header_property](../../../../../functions/crates/lpe-exchange/src/tests/strict_record_content_header_property.md)
- [strict_record_content_body_property](../../../../../functions/crates/lpe-exchange/src/tests/strict_record_content_body_property.md)