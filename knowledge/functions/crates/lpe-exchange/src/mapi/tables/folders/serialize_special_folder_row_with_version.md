---
type: Rust Function
title: serialize_special_folder_row_with_version
resource: crates/lpe-exchange/src/mapi/tables/folders.rs#L263-L286
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/folder_version_property_value
  - functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value
  - functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_special_folder_row
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_specific_response_with_custom
  - functions/crates/lpe-exchange/src/mapi/rop/serialize_object_property
  - functions/crates/lpe-exchange/src/mapi/rop/serialize_session_folder_row
---

# Signature

`pub(in crate::mapi) fn serialize_special_folder_row_with_version( folder_id: u64, mailboxes: &[JmapMailbox], columns: &[u32], principal: Option<&AccountPrincipal>, version: Option<&crate::mapi_store::MapiFolderVersion>, ) -> Vec<u8>`

# Calls

- [folder_version_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/folder_version_property_value.md)
- [write_mapi_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value.md)
- [serialize_special_folder_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_special_folder_row.md)

# Called by

- [rop_get_properties_specific_response_with_custom](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_specific_response_with_custom.md)
- [serialize_object_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/serialize_object_property.md)
- [serialize_session_folder_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/serialize_session_folder_row.md)