---
type: Rust Function
title: strict_decode_u64_property
resource: crates/lpe-exchange/src/tests/mod.rs#L13442-L13452
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/tests/strict_record_folder_property
  - functions/crates/lpe-exchange/src/tests/strict_record_content_header_property
---

# Signature

`fn strict_decode_u64_property(property: &StrictFastTransferProperty) -> Result<u64, String>`

# Called by

- [strict_record_folder_property](../../../../../functions/crates/lpe-exchange/src/tests/strict_record_folder_property.md)
- [strict_record_content_header_property](../../../../../functions/crates/lpe-exchange/src/tests/strict_record_content_header_property.md)