---
type: Rust Function
title: strict_record_folder_property
resource: crates/lpe-exchange/src/tests/mod.rs#L13218-L13274
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/tests/strict_decode_utf16z
  - functions/crates/lpe-exchange/src/tests/strict_decode_object_id_property
  - functions/crates/lpe-exchange/src/tests/strict_decode_u32_property
  - functions/crates/lpe-exchange/src/tests/strict_decode_u64_property
  called_by:
  - functions/crates/lpe-exchange/src/tests/strict_decode_hierarchy_sync_stream
---

# Signature

`fn strict_record_folder_property( folder: &mut StrictHierarchyFolderBuilder, property: StrictFastTransferProperty, ) -> Result<(), String>`

# Calls

- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [strict_decode_utf16z](../../../../../functions/crates/lpe-exchange/src/tests/strict_decode_utf16z.md)
- [strict_decode_object_id_property](../../../../../functions/crates/lpe-exchange/src/tests/strict_decode_object_id_property.md)
- [strict_decode_u32_property](../../../../../functions/crates/lpe-exchange/src/tests/strict_decode_u32_property.md)
- [strict_decode_u64_property](../../../../../functions/crates/lpe-exchange/src/tests/strict_decode_u64_property.md)

# Called by

- [strict_decode_hierarchy_sync_stream](../../../../../functions/crates/lpe-exchange/src/tests/strict_decode_hierarchy_sync_stream.md)