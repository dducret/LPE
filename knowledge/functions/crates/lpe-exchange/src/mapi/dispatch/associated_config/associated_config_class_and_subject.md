---
type: Rust Function
title: associated_config_class_and_subject
resource: crates/lpe-exchange/src/mapi/dispatch/associated_config.rs#L632-L650
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/delete_associated_config_properties
  - functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/set_associated_config_properties
  - functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/persist_associated_config_message
  - functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/append_existing_associated_config_save_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/apply_supported_object_property_values
---

# Signature

`pub(super) fn associated_config_class_and_subject( properties: &HashMap<u32, MapiValue>, ) -> (String, String)`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [delete_associated_config_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/delete_associated_config_properties.md)
- [set_associated_config_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/set_associated_config_properties.md)
- [persist_associated_config_message](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/persist_associated_config_message.md)
- [append_existing_associated_config_save_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/append_existing_associated_config_save_response.md)
- [apply_supported_object_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/apply_supported_object_property_values.md)