---
type: Rust Function
title: mapi_properties_to_json
resource: crates/lpe-exchange/src/mapi/properties/values.rs#L26-L34
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/values/mapi_value_to_json
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/delete_associated_config_properties
  - functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/set_associated_config_properties
  - functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/persist_associated_config_message
  - functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/associated_config_message_with_identity
  - functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/append_existing_associated_config_save_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/apply_supported_object_property_values
  - functions/crates/lpe-exchange/src/mapi/properties/streams/sync_stream_target
---

# Signature

`pub(in crate::mapi) fn mapi_properties_to_json( properties: &HashMap<u32, MapiValue>, ) -> serde_json::Value`

# Calls

- [mapi_value_to_json](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/mapi_value_to_json.md)

# Called by

- [delete_associated_config_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/delete_associated_config_properties.md)
- [set_associated_config_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/set_associated_config_properties.md)
- [persist_associated_config_message](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/persist_associated_config_message.md)
- [associated_config_message_with_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/associated_config_message_with_identity.md)
- [append_existing_associated_config_save_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/append_existing_associated_config_save_response.md)
- [apply_supported_object_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/apply_supported_object_property_values.md)
- [sync_stream_target](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/sync_stream_target.md)