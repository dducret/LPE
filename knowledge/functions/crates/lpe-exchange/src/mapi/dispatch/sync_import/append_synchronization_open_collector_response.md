---
type: Rust Function
title: append_synchronization_open_collector_response
resource: crates/lpe-exchange/src/mapi/dispatch/sync_import.rs#L499-L539
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/input_object
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index
  - functions/crates/lpe-exchange/src/mapi/rop/parse/sync_request_options/RopRequest/collector_sync_type
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/allocate_output_handle
  - functions/crates/lpe-exchange/src/mapi/sync/sync_checkpoint_mailbox_id
  - functions/crates/lpe-exchange/src/mapi/sync/sync_checkpoint_kind
  - functions/crates/lpe-exchange/src/mapi/session/set_handle_slot
  - functions/crates/lpe-exchange/src/mapi/rop/responses/rop_simple_success_response
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_transfer/append_sync_transfer_dispatch_response
---

# Signature

`pub(super) fn append_synchronization_open_collector_response( session: &mut MapiSession, handle_slots: &mut Vec<u32>, request: &RopRequest, mailboxes: &[JmapMailbox], responses: &mut Vec<u8>, output_handles: &mut Vec<u32>, )`

# Calls

- [input_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_object.md)
- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [response_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index.md)
- [collector_sync_type](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/sync_request_options/RopRequest/collector_sync_type.md)
- [allocate_output_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/allocate_output_handle.md)
- [sync_checkpoint_mailbox_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/sync_checkpoint_mailbox_id.md)
- [sync_checkpoint_kind](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/sync_checkpoint_kind.md)
- [set_handle_slot](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/set_handle_slot.md)
- [rop_simple_success_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_simple_success_response.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [append_sync_transfer_dispatch_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_transfer/append_sync_transfer_dispatch_response.md)