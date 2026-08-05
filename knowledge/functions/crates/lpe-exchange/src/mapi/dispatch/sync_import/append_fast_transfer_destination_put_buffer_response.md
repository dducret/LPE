---
type: Rust Function
title: append_fast_transfer_destination_put_buffer_response
resource: crates/lpe-exchange/src/mapi/dispatch/sync_import.rs#L541-L594
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/first_fast_transfer_marker
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/fast_transfer_upload_data
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/staged_fast_transfer_destination_buffer
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/fast_transfer_property_values
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/apply_fast_transfer_destination_properties
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/commit_fast_transfer_destination_buffer
  - functions/crates/lpe-exchange/src/mapi/rop/responses/rop_fast_transfer_put_buffer_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_transfer/append_sync_transfer_dispatch_response
---

# Signature

`pub(super) fn append_fast_transfer_destination_put_buffer_response( session: &mut MapiSession, handle_slots: &[u32], request: &RopRequest, responses: &mut Vec<u8>, ) -> bool`

# Calls

- [first_fast_transfer_marker](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/first_fast_transfer_marker.md)
- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [response_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index.md)
- [fast_transfer_upload_data](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/fast_transfer_upload_data.md)
- [staged_fast_transfer_destination_buffer](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/staged_fast_transfer_destination_buffer.md)
- [fast_transfer_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/fast_transfer_property_values.md)
- [apply_fast_transfer_destination_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/apply_fast_transfer_destination_properties.md)
- [commit_fast_transfer_destination_buffer](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/commit_fast_transfer_destination_buffer.md)
- [rop_fast_transfer_put_buffer_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_fast_transfer_put_buffer_response.md)

# Called by

- [append_sync_transfer_dispatch_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_transfer/append_sync_transfer_dispatch_response.md)