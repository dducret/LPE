---
type: Rust Function
title: hierarchy_download_no_deletions_keeps_missing_id_without_tombstone
resource: crates/lpe-exchange/src/mapi_mailstore/tests.rs#L2193-L2232
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/replguid_idset_from_counters
  - functions/crates/lpe-exchange/src/mapi_mailstore/sync_state_stream_with_uploaded_property
  - functions/crates/lpe-exchange/src/mapi_mailstore/initial_sync_state_stream
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_attachments
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/select_download_manifest_for_client_state
  - functions/crates/lpe-core/src/sieve/Parser/expect
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/assert_absent_property
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/assert_variable_property
---

# Signature

`fn hierarchy_download_no_deletions_keeps_missing_id_without_tombstone()`

# Calls

- [replguid_idset_from_counters](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/replguid_idset_from_counters.md)
- [sync_state_stream_with_uploaded_property](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/sync_state_stream_with_uploaded_property.md)
- [initial_sync_state_stream](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/initial_sync_state_stream.md)
- [sync_manifest_buffer_with_attachments](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_attachments.md)
- [select_download_manifest_for_client_state](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/select_download_manifest_for_client_state.md)
- [expect](../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)
- [assert_absent_property](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/assert_absent_property.md)
- [assert_variable_property](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/assert_variable_property.md)