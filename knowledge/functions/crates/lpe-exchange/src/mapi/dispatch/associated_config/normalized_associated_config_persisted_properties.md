---
type: Rust Function
title: normalized_associated_config_persisted_properties
resource: crates/lpe-exchange/src/mapi/dispatch/associated_config.rs#L557-L599
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/minimal_roaming_dictionary_stream
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/persist_associated_config_message
  - functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/normalized_associated_config_content_properties
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/empty_inbox_message_list_settings_save_preserves_client_stream_mask
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/associated_config_persist_normalizes_stale_configuration_roaming_dictionary
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/associated_config_persist_normalizes_stale_umolk_minimal_dictionary
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/associated_config_persist_leaves_non_configuration_roaming_dictionary_unchanged
---

# Signature

`pub(super) fn normalized_associated_config_persisted_properties( message_class: &str, properties: &HashMap<u32, MapiValue>, ) -> HashMap<u32, MapiValue>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [minimal_roaming_dictionary_stream](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/minimal_roaming_dictionary_stream.md)

# Called by

- [persist_associated_config_message](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/persist_associated_config_message.md)
- [normalized_associated_config_content_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/normalized_associated_config_content_properties.md)
- [empty_inbox_message_list_settings_save_preserves_client_stream_mask](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/empty_inbox_message_list_settings_save_preserves_client_stream_mask.md)
- [associated_config_persist_normalizes_stale_configuration_roaming_dictionary](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/associated_config_persist_normalizes_stale_configuration_roaming_dictionary.md)
- [associated_config_persist_normalizes_stale_umolk_minimal_dictionary](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/associated_config_persist_normalizes_stale_umolk_minimal_dictionary.md)
- [associated_config_persist_leaves_non_configuration_roaming_dictionary_unchanged](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/associated_config_persist_leaves_non_configuration_roaming_dictionary_unchanged.md)