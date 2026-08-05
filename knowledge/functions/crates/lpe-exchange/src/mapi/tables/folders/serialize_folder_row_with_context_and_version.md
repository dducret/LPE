---
type: Rust Function
title: serialize_folder_row_with_context_and_version
resource: crates/lpe-exchange/src/mapi/tables/folders.rs#L845-L868
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/folder_version_property_value
  - functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value
  - functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_folder_row_with_context
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_specific_response_with_custom
  - functions/crates/lpe-exchange/src/mapi/rop/serialize_object_property
  - functions/crates/lpe-exchange/src/mapi/rop/serialize_session_folder_row
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/serialize_hierarchy_row_from_backing_object
---

# Signature

`pub(in crate::mapi) fn serialize_folder_row_with_context_and_version( mailbox: &JmapMailbox, mailboxes: &[JmapMailbox], columns: &[u32], mailbox_guid: Uuid, version: Option<&crate::mapi_store::MapiFolderVersion>, ) -> Vec<u8>`

# Calls

- [folder_version_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/folder_version_property_value.md)
- [write_mapi_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value.md)
- [serialize_folder_row_with_context](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_folder_row_with_context.md)

# Called by

- [rop_get_properties_specific_response_with_custom](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_specific_response_with_custom.md)
- [serialize_object_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/serialize_object_property.md)
- [serialize_session_folder_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/serialize_session_folder_row.md)
- [serialize_hierarchy_row_from_backing_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/serialize_hierarchy_row_from_backing_object.md)