---
type: Rust Function
title: serialize_advertised_special_folder_row_with_counts_and_version
resource: crates/lpe-exchange/src/mapi/tables/folders.rs#L437-L470
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/change_number_for_store_id
  - functions/crates/lpe-exchange/src/mapi/properties/folder_version_property_value
  - functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value
  - functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_advertised_special_folder_row_with_counts_and_change_number
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/serialize_hierarchy_row_from_backing_object
---

# Signature

`pub(super) fn serialize_advertised_special_folder_row_with_counts_and_version( folder_id: u64, columns: &[u32], mailbox_guid: Uuid, content_count: u32, unread_count: u32, deleted_count: u32, version: Option<&crate::mapi_store::MapiFolderVersion>, ) -> Vec<u8>`

# Calls

- [change_number_for_store_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/change_number_for_store_id.md)
- [folder_version_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/folder_version_property_value.md)
- [write_mapi_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value.md)
- [serialize_advertised_special_folder_row_with_counts_and_change_number](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_advertised_special_folder_row_with_counts_and_change_number.md)

# Called by

- [serialize_hierarchy_row_from_backing_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/serialize_hierarchy_row_from_backing_object.md)