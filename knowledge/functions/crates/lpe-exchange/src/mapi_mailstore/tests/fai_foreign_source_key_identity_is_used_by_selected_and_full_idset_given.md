---
type: Rust Function
title: fai_foreign_source_key_identity_is_used_by_selected_and_full_idset_given
resource: crates/lpe-exchange/src/mapi_mailstore/tests.rs#L3160-L3242
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_rfc3339_utc
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_special_objects_and_final_state
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/download_change_facts
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/select_download_manifest_for_client_state
  - functions/crates/lpe-exchange/src/mapi_mailstore/initial_sync_state_stream
  - functions/crates/lpe-core/src/sieve/Parser/expect
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/assert_variable_property
---

# Signature

`fn fai_foreign_source_key_identity_is_used_by_selected_and_full_idset_given()`

# Calls

- [remember_mapi_identity](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity.md)
- [filetime_from_rfc3339_utc](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_rfc3339_utc.md)
- [sync_manifest_buffer_with_special_objects_and_final_state](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_special_objects_and_final_state.md)
- [download_change_facts](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/download_change_facts.md)
- [select_download_manifest_for_client_state](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/select_download_manifest_for_client_state.md)
- [initial_sync_state_stream](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/initial_sync_state_stream.md)
- [expect](../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [assert_variable_property](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/assert_variable_property.md)