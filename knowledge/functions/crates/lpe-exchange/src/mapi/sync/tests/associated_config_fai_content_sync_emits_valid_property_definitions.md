---
type: Rust Function
title: associated_config_fai_content_sync_emits_valid_property_definitions
resource: crates/lpe-exchange/src/mapi/sync/tests.rs#L532-L660
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/empty
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_named_property_mappings
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_associated_configs
  - functions/crates/lpe-exchange/src/mapi/sync/special_sync_objects_for
  - functions/crates/lpe-exchange/src/mapi/sync/tests/sync_principal
  - functions/crates/lpe-exchange/src/mapi/sync/tests/associated_content_sync_buffer
  - functions/crates/lpe-exchange/src/mapi_mailstore/special_message/fast_transfer_message_content_buffer_with_special_object
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_content_transfer_fai_debug_summary
  - functions/crates/lpe-exchange/src/mapi/identity/generated_message_search_key
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
---

# Signature

`fn associated_config_fai_content_sync_emits_valid_property_definitions()`

# Calls

- [remember_mapi_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity.md)
- [empty](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/empty.md)
- [with_named_property_mappings](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_named_property_mappings.md)
- [with_associated_configs](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_associated_configs.md)
- [special_sync_objects_for](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/special_sync_objects_for.md)
- [sync_principal](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/sync_principal.md)
- [associated_content_sync_buffer](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/associated_content_sync_buffer.md)
- [fast_transfer_message_content_buffer_with_special_object](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/special_message/fast_transfer_message_content_buffer_with_special_object.md)
- [decode_content_transfer_fai_debug_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_content_transfer_fai_debug_summary.md)
- [generated_message_search_key](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/generated_message_search_key.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)