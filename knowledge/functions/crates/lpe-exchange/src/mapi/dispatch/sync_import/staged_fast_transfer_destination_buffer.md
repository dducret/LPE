---
type: Rust Function
title: staged_fast_transfer_destination_buffer
resource: crates/lpe-exchange/src/mapi/dispatch/sync_import.rs#L29-L46
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/input_object
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/fast_transfer_upload_data
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_fast_transfer_destination_put_buffer_response
---

# Signature

`pub(super) fn staged_fast_transfer_destination_buffer( session: &MapiSession, handle_slots: &[u32], request: &RopRequest, ) -> Option<(u32, Vec<u8>)>`

# Calls

- [input_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_object.md)
- [fast_transfer_upload_data](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/fast_transfer_upload_data.md)

# Called by

- [append_fast_transfer_destination_put_buffer_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_fast_transfer_destination_put_buffer_response.md)