---
type: Rust Function
title: append_fast_transfer_source_copy_response
resource: crates/lpe-exchange/src/mapi/dispatch/sync_import.rs#L267-L340
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/input_object
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index
  - functions/crates/lpe-exchange/src/mapi/rop/parse/sync_request_options/RopRequest/fast_transfer_source_property_tags
  - functions/crates/lpe-exchange/src/mapi/sync/fast_transfer_manifest_for_object
  - functions/crates/lpe-exchange/src/mapi/rop/parse/sync_request_options/RopRequest/fast_transfer_source_send_options
  - functions/crates/lpe-exchange/src/mapi/rop/parse/sync_request_options/RopRequest/fast_transfer_source_level
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/allocate_output_handle
  - functions/crates/lpe-exchange/src/mapi/session/set_handle_slot
  - functions/crates/lpe-exchange/src/mapi/sync/responses/rop_fast_transfer_source_copy_response
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_transfer/append_sync_transfer_dispatch_response
---

# Signature

`pub(super) fn append_fast_transfer_source_copy_response( session: &mut MapiSession, handle_slots: &mut Vec<u32>, request: &RopRequest, principal: &AccountPrincipal, mailboxes: &[JmapMailbox], emails: &[JmapEmail], snapshot: &MapiMailStoreSnapshot, responses: &mut Vec<u8>, output_handles: &mut Vec<u32>, )`

# Calls

- [input_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_object.md)
- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [response_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index.md)
- [fast_transfer_source_property_tags](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/sync_request_options/RopRequest/fast_transfer_source_property_tags.md)
- [fast_transfer_manifest_for_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/fast_transfer_manifest_for_object.md)
- [fast_transfer_source_send_options](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/sync_request_options/RopRequest/fast_transfer_source_send_options.md)
- [fast_transfer_source_level](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/sync_request_options/RopRequest/fast_transfer_source_level.md)
- [allocate_output_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/allocate_output_handle.md)
- [set_handle_slot](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/set_handle_slot.md)
- [rop_fast_transfer_source_copy_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/responses/rop_fast_transfer_source_copy_response.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [append_sync_transfer_dispatch_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_transfer/append_sync_transfer_dispatch_response.md)