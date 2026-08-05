---
type: Rust Function
title: apply_mapi_property_values_to_map
resource: crates/lpe-exchange/src/mapi/dispatch/custom_properties.rs#L21-L30
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/set_associated_config_properties
  - functions/crates/lpe-exchange/src/mapi/dispatch/conversation_actions/stage_virtual_conversation_action_property_values
  - functions/crates/lpe-exchange/src/mapi/dispatch/messages/stage_message_property_values
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/apply_supported_object_property_values
  - functions/crates/lpe-exchange/src/mapi/dispatch/public_folders/apply_canonical_public_folder_item_property_values
---

# Signature

`pub(super) fn apply_mapi_property_values_to_map( properties: &mut HashMap<u32, MapiValue>, values: Vec<(u32, MapiValue)>, )`

# Calls

- [canonical_property_storage_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)

# Called by

- [set_associated_config_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/set_associated_config_properties.md)
- [stage_virtual_conversation_action_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/conversation_actions/stage_virtual_conversation_action_property_values.md)
- [stage_message_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/messages/stage_message_property_values.md)
- [apply_supported_object_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/apply_supported_object_property_values.md)
- [apply_canonical_public_folder_item_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/public_folders/apply_canonical_public_folder_item_property_values.md)