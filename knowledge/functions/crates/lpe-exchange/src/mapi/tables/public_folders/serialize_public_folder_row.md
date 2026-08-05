---
type: Rust Function
title: serialize_public_folder_row
resource: crates/lpe-exchange/src/mapi/tables/public_folders.rs#L4-L34
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/mapped_mapi_object_id
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/write_utf16z
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/properties/public_folder_property_value
  - functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value
  - functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_property_default
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/serialize_session_folder_row
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/serialize_hierarchy_row_from_backing_object
---

# Signature

`pub(in crate::mapi) fn serialize_public_folder_row( folder: &MapiPublicFolder, columns: &[u32], ) -> Vec<u8>`

# Calls

- [mapped_mapi_object_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/mapped_mapi_object_id.md)
- [write_utf16z](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/write_utf16z.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [public_folder_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/public_folder_property_value.md)
- [write_mapi_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value.md)
- [write_property_default](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_property_default.md)

# Called by

- [serialize_session_folder_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/serialize_session_folder_row.md)
- [serialize_hierarchy_row_from_backing_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/serialize_hierarchy_row_from_backing_object.md)