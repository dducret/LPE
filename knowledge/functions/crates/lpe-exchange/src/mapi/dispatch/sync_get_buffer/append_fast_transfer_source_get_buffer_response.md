---
type: Rust Function
title: append_fast_transfer_source_get_buffer_response
resource: crates/lpe-exchange/src/mapi/dispatch/sync_get_buffer.rs#L123-L565
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/input_object_mut
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/select_download_manifest_for_client_state
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_get_buffer/fast_transfer_source_get_buffer_transfer_size
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/uploaded_state_has_delta_anchor
  - functions/crates/lpe-exchange/src/mapi/sync/responses/rop_fast_transfer_source_get_buffer_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/fast_transfer/summarize_fast_transfer_get_buffer_response
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/hierarchy_transfer_close_summary
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/default_folder_hierarchy_membership_summary
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/log_hierarchy_get_buffer_payload_summary
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_completed_sync_checkpoint
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/debug_role_for_folder_id
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/debug_container_class_for_folder_id
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/store_mapi_sync_checkpoint
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_get_buffer/packed_fast_transfer_source_get_buffer_response_payloads
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_transfer/append_sync_transfer_dispatch_response
---

# Signature

`pub(super) async fn append_fast_transfer_source_get_buffer_response<S: ExchangeStore>( store: &S, principal: &AccountPrincipal, request_id: &str, session: &mut MapiSession, handle_slots: &[u32], request: &RopRequest, residual_rop_out_size: usize, responses: &mut Vec<u8>, ) -> Option<(u64, String, String)>`

# Calls

- [input_object_mut](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_object_mut.md)
- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [response_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index.md)
- [select_download_manifest_for_client_state](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/select_download_manifest_for_client_state.md)
- [fast_transfer_source_get_buffer_transfer_size](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_get_buffer/fast_transfer_source_get_buffer_transfer_size.md)
- [uploaded_state_has_delta_anchor](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/uploaded_state_has_delta_anchor.md)
- [rop_fast_transfer_source_get_buffer_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/responses/rop_fast_transfer_source_get_buffer_response.md)
- [summarize_fast_transfer_get_buffer_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/fast_transfer/summarize_fast_transfer_get_buffer_response.md)
- [hierarchy_transfer_close_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/hierarchy_transfer_close_summary.md)
- [default_folder_hierarchy_membership_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/default_folder_hierarchy_membership_summary.md)
- [log_hierarchy_get_buffer_payload_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/log_hierarchy_get_buffer_payload_summary.md)
- [record_completed_sync_checkpoint](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_completed_sync_checkpoint.md)
- [debug_role_for_folder_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/debug_role_for_folder_id.md)
- [debug_container_class_for_folder_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/debug_container_class_for_folder_id.md)
- [store_mapi_sync_checkpoint](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/store_mapi_sync_checkpoint.md)

# Called by

- [packed_fast_transfer_source_get_buffer_response_payloads](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_get_buffer/packed_fast_transfer_source_get_buffer_response_payloads.md)
- [append_sync_transfer_dispatch_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_transfer/append_sync_transfer_dispatch_response.md)