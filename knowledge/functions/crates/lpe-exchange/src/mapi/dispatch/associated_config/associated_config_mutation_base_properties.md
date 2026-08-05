---
type: Rust Function
title: associated_config_mutation_base_properties
resource: crates/lpe-exchange/src/mapi/dispatch/associated_config.rs#L175-L191
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/values/mapi_properties_from_json
  - functions/crates/lpe-jmap/src/state/entry
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_config_property_value
  - functions/crates/lpe-core/src/sieve/Parser/expect
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/delete_associated_config_properties
  - functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/set_associated_config_properties
  - functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/append_existing_associated_config_save_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/apply_supported_object_property_values
---

# Signature

`pub(super) fn associated_config_mutation_base_properties( message: &crate::mapi_store::MapiAssociatedConfigMessage, ) -> HashMap<u32, MapiValue>`

# Calls

- [mapi_properties_from_json](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/mapi_properties_from_json.md)
- [entry](../../../../../../../functions/crates/lpe-jmap/src/state/entry.md)
- [associated_config_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_config_property_value.md)
- [expect](../../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)

# Called by

- [delete_associated_config_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/delete_associated_config_properties.md)
- [set_associated_config_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/set_associated_config_properties.md)
- [append_existing_associated_config_save_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/append_existing_associated_config_save_response.md)
- [apply_supported_object_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/apply_supported_object_property_values.md)