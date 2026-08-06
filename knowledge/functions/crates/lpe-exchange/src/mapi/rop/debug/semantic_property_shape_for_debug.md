---
type: Rust Function
title: semantic_property_shape_for_debug
resource: crates/lpe-exchange/src/mapi/rop/debug.rs#L818-L855
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/folder/logon_property_value
  - functions/crates/lpe-exchange/src/mapi/rop/debug/shapes/mapi_value_shape_for_debug
  - functions/crates/lpe-exchange/src/mapi/properties/folder/special_folder_identification_property_value
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/associated_config_message_for_id
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_config_property_value_with_mailbox_guid
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/debug/format_property_value_shapes_for_debug
---

# Signature

`fn semantic_property_shape_for_debug( object: Option<&MapiObject>, principal: &AccountPrincipal, snapshot: &MapiMailStoreSnapshot, tag: u32, ) -> Option<String>`

# Calls

- [logon_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/folder/logon_property_value.md)
- [mapi_value_shape_for_debug](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/shapes/mapi_value_shape_for_debug.md)
- [special_folder_identification_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/folder/special_folder_identification_property_value.md)
- [associated_config_message_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/associated_config_message_for_id.md)
- [associated_config_property_value_with_mailbox_guid](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_config_property_value_with_mailbox_guid.md)

# Called by

- [format_property_value_shapes_for_debug](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/format_property_value_shapes_for_debug.md)