---
type: Rust Function
title: sync_manifest_serializes_content_message_header_in_fixed_order
resource: crates/lpe-exchange/src/mapi_mailstore/tests.rs#L507-L648
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_store_id
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/assert_tag_order
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/assert_bool_property
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/assert_i32_property
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/assert_change_number_property
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/position
  - functions/crates/lpe-core/src/sieve/Parser/expect
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/assert_i64_property
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/assert_variable_property
  - functions/crates/lpe-exchange/src/mapi_mailstore/replguid_idset_from_counters
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/download_change_facts_with_normal_message_sync_facts
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/select_download_manifest_for_client_state
  - functions/crates/lpe-exchange/src/mapi_mailstore/initial_sync_state_stream
---

# Signature

`fn sync_manifest_serializes_content_message_header_in_fixed_order()`

# Calls

- [remember_mapi_identity](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity.md)
- [source_key_for_store_id](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_store_id.md)
- [sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts.md)
- [assert_tag_order](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/assert_tag_order.md)
- [assert_bool_property](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/assert_bool_property.md)
- [assert_i32_property](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/assert_i32_property.md)
- [assert_change_number_property](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/assert_change_number_property.md)
- [position](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/position.md)
- [expect](../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)
- [assert_i64_property](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/assert_i64_property.md)
- [assert_variable_property](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/assert_variable_property.md)
- [replguid_idset_from_counters](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/replguid_idset_from_counters.md)
- [download_change_facts_with_normal_message_sync_facts](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/download_change_facts_with_normal_message_sync_facts.md)
- [select_download_manifest_for_client_state](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/select_download_manifest_for_client_state.md)
- [initial_sync_state_stream](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/initial_sync_state_stream.md)