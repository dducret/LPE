---
type: Rust Function
title: serialize_search_folder_definition_row_with_mailbox_guid
resource: crates/lpe-exchange/src/mapi/tables/associated_contents.rs#L62-L75
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/search_folders/search_folder_definition_message_property_value
  - functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value
  - functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_property_default
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_specific_response_with_custom
---

# Signature

`pub(in crate::mapi) fn serialize_search_folder_definition_row_with_mailbox_guid( message: &SearchFolderDefinition, mailbox_guid: Uuid, columns: &[u32], ) -> Vec<u8>`

# Calls

- [search_folder_definition_message_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/search_folders/search_folder_definition_message_property_value.md)
- [write_mapi_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value.md)
- [write_property_default](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_property_default.md)

# Called by

- [rop_get_properties_specific_response_with_custom](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_specific_response_with_custom.md)