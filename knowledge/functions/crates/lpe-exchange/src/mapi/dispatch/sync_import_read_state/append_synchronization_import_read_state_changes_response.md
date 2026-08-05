---
type: Rust Function
title: append_synchronization_import_read_state_changes_response
resource: crates/lpe-exchange/src/mapi/dispatch/sync_import_read_state.rs#L3-L64
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/input_object
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/import_read_state_changes
  - functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/transient_client_local_message_id
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/record_sync_upload_content_change
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/canonical_message_change_number
  - functions/crates/lpe-exchange/src/mapi/rop/responses/rop_partial_completion_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_sync_import_dispatch_response
---

# Signature

`pub(super) async fn append_synchronization_import_read_state_changes_response<S: ExchangeStore>( store: &S, principal: &AccountPrincipal, session: &mut MapiSession, handle_slots: &[u32], request: &RopRequest, mailboxes: &[JmapMailbox], emails: &[JmapEmail], responses: &mut Vec<u8>, )`

# Calls

- [input_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_object.md)
- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [response_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index.md)
- [import_read_state_changes](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/import_read_state_changes.md)
- [transient_client_local_message_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/transient_client_local_message_id.md)
- [record_sync_upload_content_change](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/record_sync_upload_content_change.md)
- [canonical_message_change_number](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/canonical_message_change_number.md)
- [rop_partial_completion_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_partial_completion_response.md)

# Called by

- [append_sync_import_dispatch_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_sync_import_dispatch_response.md)