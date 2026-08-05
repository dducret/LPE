---
type: Rust Function
title: append_sync_transfer_dispatch_response
resource: crates/lpe-exchange/src/mapi/dispatch/sync_transfer.rs#L25-L207
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_fast_transfer_source_copy_messages_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_fast_transfer_destination_configure_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_fast_transfer_destination_put_buffer_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_fast_transfer_source_copy_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute/available_execute_rop_response_size
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_get_buffer/append_fast_transfer_source_get_buffer_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_tell_version_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_upload_state/append_upload_state_stream_begin_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_upload_state/append_upload_state_stream_continue_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_upload_state/append_upload_state_stream_end_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_synchronization_open_collector_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_synchronization_get_transfer_state_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops
---

# Signature

`pub(super) async fn append_sync_transfer_dispatch_response<S>( store: &S, principal: &AccountPrincipal, request_id: &str, session: &mut MapiSession, handle_slots: &mut Vec<u32>, request: &RopRequest, mailboxes: &[JmapMailbox], emails: &[JmapEmail], snapshot: &MapiMailStoreSnapshot, max_rop_out: u32, extended: bool, chain_fast_transfer_get_buffer: bool, responses: &mut Vec<u8>, output_handles: &mut Vec<u32>, completed_hierarchy_sync: &mut Option<(u64, String, String)>, content_sync_configure_observed: &mut bool, ) -> bool where S: ExchangeStore,`

# Calls

- [append_fast_transfer_source_copy_messages_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_fast_transfer_source_copy_messages_response.md)
- [append_fast_transfer_destination_configure_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_fast_transfer_destination_configure_response.md)
- [append_fast_transfer_destination_put_buffer_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_fast_transfer_destination_put_buffer_response.md)
- [append_fast_transfer_source_copy_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_fast_transfer_source_copy_response.md)
- [available_execute_rop_response_size](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute/available_execute_rop_response_size.md)
- [append_fast_transfer_source_get_buffer_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_get_buffer/append_fast_transfer_source_get_buffer_response.md)
- [append_tell_version_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_tell_version_response.md)
- [append_upload_state_stream_begin_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_upload_state/append_upload_state_stream_begin_response.md)
- [append_upload_state_stream_continue_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_upload_state/append_upload_state_stream_continue_response.md)
- [append_upload_state_stream_end_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_upload_state/append_upload_state_stream_end_response.md)
- [append_synchronization_open_collector_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_synchronization_open_collector_response.md)
- [append_synchronization_get_transfer_state_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_synchronization_get_transfer_state_response.md)

# Called by

- [execute_rops](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops.md)