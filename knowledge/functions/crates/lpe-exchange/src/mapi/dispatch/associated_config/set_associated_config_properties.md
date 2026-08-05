---
type: Rust Function
title: set_associated_config_properties
resource: crates/lpe-exchange/src/mapi/dispatch/associated_config.rs#L71-L102
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/associated_config_mutation_base_properties
  - functions/crates/lpe-exchange/src/mapi_store/associated_config/is_associated_config_read_only_property_tag
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  - functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/apply_mapi_property_values_to_map
  - functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/associated_config_class_and_subject
  - functions/crates/lpe-exchange/src/mapi/properties/values/mapi_properties_to_json
  - functions/crates/lpe-exchange/src/mapi_store/associated_config/copy_associated_config_server_metadata
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/append_set_properties_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/imported_associated_config_search_key_cannot_change_after_first_save
---

# Signature

`pub(super) fn set_associated_config_properties( existing: &crate::mapi_store::MapiAssociatedConfigMessage, values: Vec<(u32, MapiValue)>, ) -> Result<crate::mapi_store::MapiAssociatedConfigMessage>`

# Calls

- [associated_config_mutation_base_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/associated_config_mutation_base_properties.md)
- [is_associated_config_read_only_property_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/is_associated_config_read_only_property_tag.md)
- [canonical_property_storage_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)
- [apply_mapi_property_values_to_map](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/apply_mapi_property_values_to_map.md)
- [associated_config_class_and_subject](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/associated_config_class_and_subject.md)
- [mapi_properties_to_json](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/mapi_properties_to_json.md)
- [copy_associated_config_server_metadata](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/copy_associated_config_server_metadata.md)

# Called by

- [append_set_properties_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/append_set_properties_response.md)
- [imported_associated_config_search_key_cannot_change_after_first_save](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/imported_associated_config_search_key_cannot_change_after_first_save.md)