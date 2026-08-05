---
type: Rust Function
title: serialize_advertised_special_folder_row_with_counts_and_change_number
resource: crates/lpe-exchange/src/mapi/tables/folders.rs#L329-L435
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/folders/special_folder_metadata
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/write_utf16z
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/write_u16_prefixed_bytes
  - functions/crates/lpe-exchange/src/mapi/tables/folders/special_folder_type
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_property_default
  - functions/crates/lpe-exchange/src/mapi/properties/default_post_message_class_for_container_class
  - functions/crates/lpe-exchange/src/mapi/properties/values/write_ascii_z
  - functions/crates/lpe-exchange/src/mapi/properties/views/default_view_supported_folder
  - functions/crates/lpe-exchange/src/mapi/properties/views/default_folder_view_entry_id
  - functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/write_u64
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_change_number
  - functions/crates/lpe-exchange/src/mapi/tables/folders/serialized_replid_guid_map
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_store_id
  - functions/crates/lpe-exchange/src/mapi/properties/folder/special_folder_identification_property_value
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_advertised_special_folder_row_with_counts
  - functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_advertised_special_folder_row_with_counts_and_version
---

# Signature

`pub(super) fn serialize_advertised_special_folder_row_with_counts_and_change_number( folder_id: u64, columns: &[u32], mailbox_guid: Uuid, content_count: u32, unread_count: u32, deleted_count: u32, change_number: u64, ) -> Vec<u8>`

# Calls

- [special_folder_metadata](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/special_folder_metadata.md)
- [write_utf16z](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/write_utf16z.md)
- [write_u16_prefixed_bytes](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/write_u16_prefixed_bytes.md)
- [special_folder_type](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/special_folder_type.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [write_property_default](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_property_default.md)
- [default_post_message_class_for_container_class](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/default_post_message_class_for_container_class.md)
- [write_ascii_z](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/write_ascii_z.md)
- [default_view_supported_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/views/default_view_supported_folder.md)
- [default_folder_view_entry_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/views/default_folder_view_entry_id.md)
- [write_mapi_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value.md)
- [write_u64](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/write_u64.md)
- [filetime_from_change_number](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_change_number.md)
- [serialized_replid_guid_map](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/serialized_replid_guid_map.md)
- [source_key_for_store_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_store_id.md)
- [special_folder_identification_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/folder/special_folder_identification_property_value.md)

# Called by

- [serialize_advertised_special_folder_row_with_counts](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_advertised_special_folder_row_with_counts.md)
- [serialize_advertised_special_folder_row_with_counts_and_version](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_advertised_special_folder_row_with_counts_and_version.md)