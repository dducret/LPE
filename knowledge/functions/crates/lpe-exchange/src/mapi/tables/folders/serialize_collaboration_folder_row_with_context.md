---
type: Rust Function
title: serialize_collaboration_folder_row_with_context
resource: crates/lpe-exchange/src/mapi/tables/folders.rs#L870-L900
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/write_utf16z
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/tables/folders/collaboration_folder_message_class
  - functions/crates/lpe-exchange/src/mapi/properties/collaboration_folder_property_value
  - functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value
  - functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_property_default
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_collaboration_folder_row_with_context_and_version
---

# Signature

`pub(in crate::mapi) fn serialize_collaboration_folder_row_with_context( folder: &MapiCollaborationFolder, columns: &[u32], associated_count: u32, ) -> Vec<u8>`

# Calls

- [write_utf16z](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/write_utf16z.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [collaboration_folder_message_class](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/collaboration_folder_message_class.md)
- [collaboration_folder_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/collaboration_folder_property_value.md)
- [write_mapi_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value.md)
- [write_property_default](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_property_default.md)

# Called by

- [serialize_collaboration_folder_row_with_context_and_version](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_collaboration_folder_row_with_context_and_version.md)