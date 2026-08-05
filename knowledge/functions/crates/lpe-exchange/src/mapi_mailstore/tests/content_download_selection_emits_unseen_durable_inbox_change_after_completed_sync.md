---
type: Rust Function
title: content_download_selection_emits_unseen_durable_inbox_change_after_completed_sync
resource: crates/lpe-exchange/src/mapi_mailstore/tests.rs#L651-L777
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_store_id
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_change_number
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/download_change_facts_with_normal_message_sync_facts
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/select_download_manifest_for_client_state
  - functions/crates/lpe-exchange/src/mapi_mailstore/initial_sync_state_stream
  - functions/crates/lpe-core/src/sieve/Parser/expect
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/assert_change_number_property
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/assert_variable_property
  - functions/crates/lpe-exchange/src/mapi_mailstore/replguid_idset_from_counters
---

# Signature

`fn content_download_selection_emits_unseen_durable_inbox_change_after_completed_sync()`

# Calls

- [source_key_for_store_id](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_store_id.md)
- [filetime_from_change_number](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_change_number.md)
- [sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts.md)
- [download_change_facts_with_normal_message_sync_facts](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/download_change_facts_with_normal_message_sync_facts.md)
- [select_download_manifest_for_client_state](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/select_download_manifest_for_client_state.md)
- [initial_sync_state_stream](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/initial_sync_state_stream.md)
- [expect](../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)
- [assert_change_number_property](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/assert_change_number_property.md)
- [assert_variable_property](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/assert_variable_property.md)
- [replguid_idset_from_counters](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/replguid_idset_from_counters.md)