---
type: Rust Function
title: mapi_properties_from_json
resource: crates/lpe-exchange/src/mapi/properties/values.rs#L36-L51
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/values/mapi_value_from_json
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/associated_config_mutation_base_properties
  - functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/persist_associated_config_message
  - functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/associated_config_message_with_identity
  - functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/append_set_properties_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message/current_common_views_fai_identity
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/imported_associated_config_search_key_cannot_change_after_first_save
  - functions/crates/lpe-exchange/src/mapi/properties/streams/sync_stream_target
  - functions/crates/lpe-exchange/src/mapi/rop/debug/format_associated_config_0e0b_debug
  - functions/crates/lpe-exchange/src/mapi/sync/associated_config/associated_config_sync_object
  - functions/crates/lpe-exchange/src/mapi/sync/associated_config/associated_config_text_property
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_config_property_value_with_mailbox_guid
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_config_named_property_tags
---

# Signature

`pub(in crate::mapi) fn mapi_properties_from_json( properties: &serde_json::Value, ) -> HashMap<u32, MapiValue>`

# Calls

- [mapi_value_from_json](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/mapi_value_from_json.md)

# Called by

- [associated_config_mutation_base_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/associated_config_mutation_base_properties.md)
- [persist_associated_config_message](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/persist_associated_config_message.md)
- [associated_config_message_with_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/associated_config_message_with_identity.md)
- [append_set_properties_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/append_set_properties_response.md)
- [current_common_views_fai_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message/current_common_views_fai_identity.md)
- [imported_associated_config_search_key_cannot_change_after_first_save](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/imported_associated_config_search_key_cannot_change_after_first_save.md)
- [sync_stream_target](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/sync_stream_target.md)
- [format_associated_config_0e0b_debug](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/format_associated_config_0e0b_debug.md)
- [associated_config_sync_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/associated_config/associated_config_sync_object.md)
- [associated_config_text_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/associated_config/associated_config_text_property.md)
- [associated_config_property_value_with_mailbox_guid](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_config_property_value_with_mailbox_guid.md)
- [associated_config_named_property_tags](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_config_named_property_tags.md)