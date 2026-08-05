---
type: Rust Function
title: normal_message_no_foreign_identifiers_uses_local_source_key_for_selection
resource: crates/lpe-exchange/src/mapi_mailstore/tests.rs#L3245-L3323
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity_with_source_key
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_store_id
  - functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_store_id
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_attachments
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/download_change_facts
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/select_download_manifest_for_client_state
  - functions/crates/lpe-exchange/src/mapi_mailstore/initial_sync_state_stream
  - functions/crates/lpe-core/src/sieve/Parser/expect
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/assert_variable_property
---

# Signature

`fn normal_message_no_foreign_identifiers_uses_local_source_key_for_selection()`

# Calls

- [remember_mapi_identity_with_source_key](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity_with_source_key.md)
- [source_key_for_store_id](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_store_id.md)
- [global_counter_from_store_id](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/global_counter_from_store_id.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [sync_manifest_buffer_with_attachments](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_attachments.md)
- [download_change_facts](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/download_change_facts.md)
- [select_download_manifest_for_client_state](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/select_download_manifest_for_client_state.md)
- [initial_sync_state_stream](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/initial_sync_state_stream.md)
- [expect](../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)
- [assert_variable_property](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/assert_variable_property.md)