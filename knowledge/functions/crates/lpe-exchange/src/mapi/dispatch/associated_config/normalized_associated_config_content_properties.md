---
type: Rust Function
title: normalized_associated_config_content_properties
resource: crates/lpe-exchange/src/mapi/dispatch/associated_config.rs#L601-L612
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/normalized_associated_config_persisted_properties
  - functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/remove_associated_config_server_owned_properties
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/append_existing_associated_config_save_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/apply_supported_object_property_values
---

# Signature

`pub(super) fn normalized_associated_config_content_properties( message_class: &str, properties: &HashMap<u32, MapiValue>, ) -> HashMap<u32, MapiValue>`

# Calls

- [normalized_associated_config_persisted_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/normalized_associated_config_persisted_properties.md)
- [remove_associated_config_server_owned_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/remove_associated_config_server_owned_properties.md)

# Called by

- [append_existing_associated_config_save_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/append_existing_associated_config_save_response.md)
- [apply_supported_object_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/apply_supported_object_property_values.md)