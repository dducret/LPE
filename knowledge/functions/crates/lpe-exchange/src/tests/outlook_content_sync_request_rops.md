---
type: Rust Function
title: outlook_content_sync_request_rops
resource: crates/lpe-exchange/src/tests/mod.rs#L15591-L15605
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/append_rop_open_folder
  - functions/crates/lpe-exchange/src/tests/append_rop_outlook_content_sync_manifest_get_buffer_with_state
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_replays_outlook_contact_sync_import_then_save
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_content_sync_incremental_after_client_state_exports_delta
  - functions/crates/lpe-exchange/src/tests/outlook_content_sync_response_rops_for_store
---

# Signature

`fn outlook_content_sync_request_rops( folder_id: u64, state_properties: &[(u32, Vec<u8>)], ) -> Vec<u8>`

# Calls

- [append_rop_open_folder](../../../../../functions/crates/lpe-exchange/src/tests/append_rop_open_folder.md)
- [append_rop_outlook_content_sync_manifest_get_buffer_with_state](../../../../../functions/crates/lpe-exchange/src/tests/append_rop_outlook_content_sync_manifest_get_buffer_with_state.md)

# Called by

- [mapi_over_http_replays_outlook_contact_sync_import_then_save](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_replays_outlook_contact_sync_import_then_save.md)
- [mapi_over_http_content_sync_incremental_after_client_state_exports_delta](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_content_sync_incremental_after_client_state_exports_delta.md)
- [outlook_content_sync_response_rops_for_store](../../../../../functions/crates/lpe-exchange/src/tests/outlook_content_sync_response_rops_for_store.md)