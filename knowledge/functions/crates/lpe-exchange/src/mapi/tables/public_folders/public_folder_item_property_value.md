---
type: Rust Function
title: public_folder_item_property_value
resource: crates/lpe-exchange/src/mapi/tables/public_folders.rs#L113-L158
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/change_number_for_store_id
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  - functions/crates/lpe-exchange/src/mapi/properties/message/mapi_message_size_value
  - functions/crates/lpe-exchange/src/mapi/tables/public_folders/public_folder_item_size
  - functions/crates/lpe-exchange/src/mapi/properties/message/mapi_message_size_extended_value
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_store_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/public_folders/restriction_matches_public_folder_item
---

# Signature

`pub(super) fn public_folder_item_property_value( item: &MapiPublicFolderItem, property_tag: u32, ) -> Option<MapiValue>`

# Calls

- [change_number_for_store_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/change_number_for_store_id.md)
- [canonical_property_storage_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)
- [mapi_message_size_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/mapi_message_size_value.md)
- [public_folder_item_size](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/public_folders/public_folder_item_size.md)
- [mapi_message_size_extended_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/mapi_message_size_extended_value.md)
- [source_key_for_store_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_store_id.md)

# Called by

- [restriction_matches_public_folder_item](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/public_folders/restriction_matches_public_folder_item.md)