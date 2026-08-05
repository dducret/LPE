---
type: Rust Method
title: associated_config_message_for_id
resource: crates/lpe-exchange/src/mapi_store/snapshot.rs#L1369-L1385
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/associated_config/outlook_inbox_associated_config_sync_defaults
  - functions/crates/lpe-exchange/src/mapi_store/associated_config/outlook_inbox_exact_virtual_associated_config_for_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/associated_config_message_for_mutation
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/associated_config/associated_config_debug_fields
  - functions/crates/lpe-exchange/src/mapi/dispatch/message_open/append_open_message_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_delete_messages_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_deletes/append_synchronization_import_deletes_response
  - functions/crates/lpe-exchange/src/mapi/properties/streams/property_stream_data
  - functions/crates/lpe-exchange/src/mapi/properties/streams/message_body_stream_data
  - functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_specific_response_with_custom
  - functions/crates/lpe-exchange/src/mapi/rop/associated_config_modeled_property
  - functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_all_response
  - functions/crates/lpe-exchange/src/mapi/rop/serialize_object_property
  - functions/crates/lpe-exchange/src/mapi/rop/debug/semantic_property_shape_for_debug
  - functions/crates/lpe-exchange/src/mapi/rop/debug/format_ipm_configuration_getprops_contract
  - functions/crates/lpe-exchange/src/mapi/sync/fast_transfer_manifest_for_object
  - functions/crates/lpe-exchange/src/mapi/tables/tests/virtual_rule_organizer_projects_exchange_stream_property
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/associated_config_identity_matches_folder
  - functions/crates/lpe-exchange/src/mapi_store/tests/inbox_associated_configs_do_not_emit_unpersisted_defaults
  - functions/crates/lpe-exchange/src/mapi_store/tests/empty_persisted_umolk_placeholder_does_not_shadow_exact_modeled_row
  - functions/crates/lpe-exchange/src/mapi_store/tests/stale_persisted_umolk_xml_placeholder_does_not_shadow_exact_modeled_row
---

# Signature

`pub(crate) fn associated_config_message_for_id( &self, item_id: u64, ) -> Option<MapiAssociatedConfigMessage>`

# Calls

- [outlook_inbox_associated_config_sync_defaults](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/outlook_inbox_associated_config_sync_defaults.md)
- [outlook_inbox_exact_virtual_associated_config_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/outlook_inbox_exact_virtual_associated_config_for_id.md)

# Called by

- [associated_config_message_for_mutation](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/associated_config_message_for_mutation.md)
- [associated_config_debug_fields](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/associated_config/associated_config_debug_fields.md)
- [append_open_message_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/message_open/append_open_message_response.md)
- [append_delete_messages_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_delete_messages_response.md)
- [append_synchronization_import_deletes_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_deletes/append_synchronization_import_deletes_response.md)
- [property_stream_data](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/property_stream_data.md)
- [message_body_stream_data](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/message_body_stream_data.md)
- [rop_get_properties_specific_response_with_custom](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_specific_response_with_custom.md)
- [associated_config_modeled_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/associated_config_modeled_property.md)
- [rop_get_properties_all_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_all_response.md)
- [serialize_object_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/serialize_object_property.md)
- [semantic_property_shape_for_debug](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/semantic_property_shape_for_debug.md)
- [format_ipm_configuration_getprops_contract](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/format_ipm_configuration_getprops_contract.md)
- [fast_transfer_manifest_for_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/fast_transfer_manifest_for_object.md)
- [virtual_rule_organizer_projects_exchange_stream_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/virtual_rule_organizer_projects_exchange_stream_property.md)
- [associated_config_identity_matches_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/associated_config_identity_matches_folder.md)
- [inbox_associated_configs_do_not_emit_unpersisted_defaults](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/inbox_associated_configs_do_not_emit_unpersisted_defaults.md)
- [empty_persisted_umolk_placeholder_does_not_shadow_exact_modeled_row](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/empty_persisted_umolk_placeholder_does_not_shadow_exact_modeled_row.md)
- [stale_persisted_umolk_xml_placeholder_does_not_shadow_exact_modeled_row](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/stale_persisted_umolk_xml_placeholder_does_not_shadow_exact_modeled_row.md)