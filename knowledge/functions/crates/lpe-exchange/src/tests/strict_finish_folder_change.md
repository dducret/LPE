---
type: Rust Function
title: strict_finish_folder_change
resource: crates/lpe-exchange/src/tests/mod.rs#L13215-L13316
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/position
  - functions/crates/lpe-exchange/src/tests/strict_validate_store_xid
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/tests/strict_decode_hierarchy_sync_stream
---

# Signature

`fn strict_finish_folder_change( folder: StrictHierarchyFolderBuilder, seen_source_keys: &mut Vec<Vec<u8>>, folder_changes: &mut Vec<StrictHierarchyFolderChange>, ) -> Result<(), String>`

# Calls

- [position](../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/position.md)
- [strict_validate_store_xid](../../../../../functions/crates/lpe-exchange/src/tests/strict_validate_store_xid.md)
- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [strict_decode_hierarchy_sync_stream](../../../../../functions/crates/lpe-exchange/src/tests/strict_decode_hierarchy_sync_stream.md)