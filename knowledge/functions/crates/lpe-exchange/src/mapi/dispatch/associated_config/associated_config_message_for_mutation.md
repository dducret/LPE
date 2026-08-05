---
type: Rust Function
title: associated_config_message_for_mutation
resource: crates/lpe-exchange/src/mapi/dispatch/associated_config.rs#L57-L69
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/associated_config_message_for_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/delete_associated_config_properties
  - functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/append_existing_associated_config_save_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/apply_supported_object_property_values
  - functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/append_set_properties_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/associated_config_mutation_uses_saved_handle_when_snapshot_misses_row
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/associated_config_mutation_prefers_cumulative_saved_handle_over_stale_snapshot
---

# Signature

`pub(super) fn associated_config_message_for_mutation( snapshot: &MapiMailStoreSnapshot, folder_id: u64, config_id: u64, saved_message: Option<&crate::mapi_store::MapiAssociatedConfigMessage>, ) -> Option<crate::mapi_store::MapiAssociatedConfigMessage>`

# Calls

- [associated_config_message_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/associated_config_message_for_id.md)

# Called by

- [delete_associated_config_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/delete_associated_config_properties.md)
- [append_existing_associated_config_save_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/append_existing_associated_config_save_response.md)
- [apply_supported_object_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/apply_supported_object_property_values.md)
- [append_set_properties_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/append_set_properties_response.md)
- [associated_config_mutation_uses_saved_handle_when_snapshot_misses_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/associated_config_mutation_uses_saved_handle_when_snapshot_misses_row.md)
- [associated_config_mutation_prefers_cumulative_saved_handle_over_stale_snapshot](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/associated_config_mutation_prefers_cumulative_saved_handle_over_stale_snapshot.md)