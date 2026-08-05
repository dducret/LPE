---
type: Rust Function
title: append_rop_outlook_hierarchy_sync_manifest_get_buffer_with_state
resource: crates/lpe-exchange/src/tests/mod.rs#L15354-L15408
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_hierarchy_sync_uses_baseline_for_stale_root_checkpoint_with_client_state
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_hierarchy_sync_client_state_resumes_after_completed_download
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_import_hierarchy_change_keeps_hidden_system_folder_alias_in_cnset
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_imported_junk_email_alias_is_reconciled_without_cnset_and_deleted_when_canonical_is_emitted
  - functions/crates/lpe-exchange/src/tests/append_rop_outlook_hierarchy_sync_manifest_get_buffer
---

# Signature

`fn append_rop_outlook_hierarchy_sync_manifest_get_buffer_with_state( rops: &mut Vec<u8>, input: u8, output: u8, buffer_size: u16, idset_given: &[u8], cnset_seen: &[u8], )`

# Called by

- [mapi_over_http_hierarchy_sync_uses_baseline_for_stale_root_checkpoint_with_client_state](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_hierarchy_sync_uses_baseline_for_stale_root_checkpoint_with_client_state.md)
- [mapi_over_http_hierarchy_sync_client_state_resumes_after_completed_download](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_hierarchy_sync_client_state_resumes_after_completed_download.md)
- [mapi_over_http_sync_import_hierarchy_change_keeps_hidden_system_folder_alias_in_cnset](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_import_hierarchy_change_keeps_hidden_system_folder_alias_in_cnset.md)
- [mapi_over_http_sync_imported_junk_email_alias_is_reconciled_without_cnset_and_deleted_when_canonical_is_emitted](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_imported_junk_email_alias_is_reconciled_without_cnset_and_deleted_when_canonical_is_emitted.md)
- [append_rop_outlook_hierarchy_sync_manifest_get_buffer](../../../../../functions/crates/lpe-exchange/src/tests/append_rop_outlook_hierarchy_sync_manifest_get_buffer.md)