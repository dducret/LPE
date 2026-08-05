---
type: Rust Function
title: hierarchy_download_selection_uses_uploaded_empty_client_state
resource: crates/lpe-exchange/src/mapi_mailstore/tests.rs#L2034-L2084
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/virtual_special_mailbox
  - functions/crates/lpe-core/src/sieve/Parser/expect
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_attachments
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/select_download_manifest_for_client_state
  - functions/crates/lpe-exchange/src/mapi_mailstore/initial_sync_state_stream
  - functions/crates/lpe-exchange/src/mapi_mailstore/canonical_hierarchy_change_number
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_store_id
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_hierarchy_transfer_debug_summary
---

# Signature

`fn hierarchy_download_selection_uses_uploaded_empty_client_state()`

# Calls

- [virtual_special_mailbox](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/virtual_special_mailbox.md)
- [expect](../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)
- [sync_manifest_buffer_with_attachments](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_attachments.md)
- [select_download_manifest_for_client_state](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/select_download_manifest_for_client_state.md)
- [initial_sync_state_stream](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/initial_sync_state_stream.md)
- [canonical_hierarchy_change_number](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/canonical_hierarchy_change_number.md)
- [source_key_for_store_id](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_store_id.md)
- [decode_hierarchy_transfer_debug_summary](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/decode_hierarchy_transfer_debug_summary.md)