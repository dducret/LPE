---
type: Rust Function
title: commit_fast_transfer_destination_buffer
resource: crates/lpe-exchange/src/mapi/dispatch/sync_import.rs#L48-L59
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/input_object_mut
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_fast_transfer_destination_put_buffer_response
---

# Signature

`pub(super) fn commit_fast_transfer_destination_buffer( session: &mut MapiSession, handle_slots: &[u32], request: &RopRequest, full_buffer: Vec<u8>, )`

# Calls

- [input_object_mut](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_object_mut.md)

# Called by

- [append_fast_transfer_destination_put_buffer_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_fast_transfer_destination_put_buffer_response.md)