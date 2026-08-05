---
type: Rust Function
title: strict_test_replguid_globset
resource: crates/lpe-exchange/src/tests/mod.rs#L13549-L13561
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/public_folders/mapi_over_http_public_folder_content_sync_exports_canonical_read_state
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_microsoft_oxcfxics_4_1_1_hierarchy_upload_returns_transfer_state
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_import_hierarchy_change_keeps_hidden_system_folder_alias_in_cnset
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_imported_junk_email_alias_is_reconciled_without_cnset_and_deleted_when_canonical_is_emitted
  - functions/crates/lpe-exchange/src/tests/mapi_message_cnset_property
  - functions/crates/lpe-exchange/src/tests/strict_push_final_hierarchy_state
  - functions/crates/lpe-exchange/src/tests/strict_content_decoder_accepts_imported_change_key_with_server_change_number
  - functions/crates/lpe-exchange/src/tests/strict_hierarchy_decoder_rejects_missing_final_cnset
  - functions/crates/lpe-exchange/src/tests/strict_hierarchy_decoder_rejects_folder_change_after_final_state
  - functions/crates/lpe-exchange/src/tests/strict_hierarchy_decoder_rejects_non_replguid_final_state
---

# Signature

`fn strict_test_replguid_globset(counters: &[u64]) -> Vec<u8>`

# Calls

- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [mapi_over_http_public_folder_content_sync_exports_canonical_read_state](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/public_folders/mapi_over_http_public_folder_content_sync_exports_canonical_read_state.md)
- [mapi_over_http_microsoft_oxcfxics_4_1_1_hierarchy_upload_returns_transfer_state](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_microsoft_oxcfxics_4_1_1_hierarchy_upload_returns_transfer_state.md)
- [mapi_over_http_sync_import_hierarchy_change_keeps_hidden_system_folder_alias_in_cnset](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_import_hierarchy_change_keeps_hidden_system_folder_alias_in_cnset.md)
- [mapi_over_http_sync_imported_junk_email_alias_is_reconciled_without_cnset_and_deleted_when_canonical_is_emitted](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_sync_imported_junk_email_alias_is_reconciled_without_cnset_and_deleted_when_canonical_is_emitted.md)
- [mapi_message_cnset_property](../../../../../functions/crates/lpe-exchange/src/tests/mapi_message_cnset_property.md)
- [strict_push_final_hierarchy_state](../../../../../functions/crates/lpe-exchange/src/tests/strict_push_final_hierarchy_state.md)
- [strict_content_decoder_accepts_imported_change_key_with_server_change_number](../../../../../functions/crates/lpe-exchange/src/tests/strict_content_decoder_accepts_imported_change_key_with_server_change_number.md)
- [strict_hierarchy_decoder_rejects_missing_final_cnset](../../../../../functions/crates/lpe-exchange/src/tests/strict_hierarchy_decoder_rejects_missing_final_cnset.md)
- [strict_hierarchy_decoder_rejects_folder_change_after_final_state](../../../../../functions/crates/lpe-exchange/src/tests/strict_hierarchy_decoder_rejects_folder_change_after_final_state.md)
- [strict_hierarchy_decoder_rejects_non_replguid_final_state](../../../../../functions/crates/lpe-exchange/src/tests/strict_hierarchy_decoder_rejects_non_replguid_final_state.md)