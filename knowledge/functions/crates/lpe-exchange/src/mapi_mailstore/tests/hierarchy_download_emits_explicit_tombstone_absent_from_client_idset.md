---
type: Rust Function
title: hierarchy_download_emits_explicit_tombstone_absent_from_client_idset
resource: crates/lpe-exchange/src/mapi_mailstore/tests.rs#L2235-L2280
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/replid_idset_from_object_ids
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_attachments
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/select_download_manifest_for_client_state
  - functions/crates/lpe-exchange/src/mapi_mailstore/initial_sync_state_stream
  - functions/crates/lpe-core/src/sieve/Parser/expect
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/position
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/assert_variable_property_present
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/assert_variable_property
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_hierarchy_transfer_debug_summary
---

# Signature

`fn hierarchy_download_emits_explicit_tombstone_absent_from_client_idset()`

# Calls

- [replid_idset_from_object_ids](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/replid_idset_from_object_ids.md)
- [sync_manifest_buffer_with_attachments](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_attachments.md)
- [select_download_manifest_for_client_state](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/select_download_manifest_for_client_state.md)
- [initial_sync_state_stream](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/initial_sync_state_stream.md)
- [expect](../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)
- [position](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/position.md)
- [assert_variable_property_present](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/assert_variable_property_present.md)
- [assert_variable_property](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/assert_variable_property.md)
- [decode_hierarchy_transfer_debug_summary](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_hierarchy_transfer_debug_summary.md)