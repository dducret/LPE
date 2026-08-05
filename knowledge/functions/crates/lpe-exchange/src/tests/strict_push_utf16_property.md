---
type: Rust Function
title: strict_push_utf16_property
resource: crates/lpe-exchange/src/tests/mod.rs#L14436-L14441
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/tests/strict_push_folder_change
  - functions/crates/lpe-exchange/src/tests/strict_content_decoder_accepts_imported_change_key_with_server_change_number
  - functions/crates/lpe-exchange/src/tests/strict_hierarchy_decoder_rejects_duplicate_folder_property
---

# Signature

`fn strict_push_utf16_property(bytes: &mut Vec<u8>, tag: u32, value: &str)`

# Called by

- [strict_push_folder_change](../../../../../functions/crates/lpe-exchange/src/tests/strict_push_folder_change.md)
- [strict_content_decoder_accepts_imported_change_key_with_server_change_number](../../../../../functions/crates/lpe-exchange/src/tests/strict_content_decoder_accepts_imported_change_key_with_server_change_number.md)
- [strict_hierarchy_decoder_rejects_duplicate_folder_property](../../../../../functions/crates/lpe-exchange/src/tests/strict_hierarchy_decoder_rejects_duplicate_folder_property.md)