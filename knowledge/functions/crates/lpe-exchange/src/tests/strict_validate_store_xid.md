---
type: Rust Function
title: strict_validate_store_xid
resource: crates/lpe-exchange/src/tests/mod.rs#L13757-L13765
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/tests/strict_decode_hierarchy_sync_stream
  - functions/crates/lpe-exchange/src/tests/strict_finish_folder_change
  - functions/crates/lpe-exchange/src/tests/strict_decode_content_sync_stream
---

# Signature

`fn strict_validate_store_xid(value: &[u8]) -> Result<(), String>`

# Called by

- [strict_decode_hierarchy_sync_stream](../../../../../functions/crates/lpe-exchange/src/tests/strict_decode_hierarchy_sync_stream.md)
- [strict_finish_folder_change](../../../../../functions/crates/lpe-exchange/src/tests/strict_finish_folder_change.md)
- [strict_decode_content_sync_stream](../../../../../functions/crates/lpe-exchange/src/tests/strict_decode_content_sync_stream.md)