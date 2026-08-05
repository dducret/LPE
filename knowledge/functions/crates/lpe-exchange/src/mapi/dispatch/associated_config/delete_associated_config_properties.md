---
type: Rust Function
title: delete_associated_config_properties
resource: crates/lpe-exchange/src/mapi/dispatch/associated_config.rs#L3-L55
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/associated_config_message_for_mutation
  - functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/associated_config_mutation_base_properties
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  - functions/LPE-CT/web/app/smoke/test/MockClassList/remove
  - functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/associated_config_class_and_subject
  - functions/crates/lpe-exchange/src/mapi/properties/values/mapi_properties_to_json
  - functions/crates/lpe-exchange/src/mapi_store/associated_config/copy_associated_config_server_metadata
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/append_delete_properties_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/imported_associated_config_search_key_cannot_change_after_first_save
---

# Signature

`pub(super) fn delete_associated_config_properties( folder_id: u64, config_id: u64, snapshot: &MapiMailStoreSnapshot, saved_message: Option<&crate::mapi_store::MapiAssociatedConfigMessage>, property_tags: &[u32], ) -> Result<(usize, crate::mapi_store::MapiAssociatedConfigMessage)>`

# Calls

- [associated_config_message_for_mutation](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/associated_config_message_for_mutation.md)
- [associated_config_mutation_base_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/associated_config_mutation_base_properties.md)
- [canonical_property_storage_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)
- [remove](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockClassList/remove.md)
- [associated_config_class_and_subject](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/associated_config_class_and_subject.md)
- [mapi_properties_to_json](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/mapi_properties_to_json.md)
- [copy_associated_config_server_metadata](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/copy_associated_config_server_metadata.md)

# Called by

- [append_delete_properties_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/append_delete_properties_response.md)
- [imported_associated_config_search_key_cannot_change_after_first_save](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/imported_associated_config_search_key_cannot_change_after_first_save.md)