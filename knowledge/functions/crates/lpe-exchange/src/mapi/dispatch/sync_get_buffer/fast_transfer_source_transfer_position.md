---
type: Rust Function
title: fast_transfer_source_transfer_position
resource: crates/lpe-exchange/src/mapi/dispatch/sync_get_buffer.rs#L32-L43
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/input_object
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_get_buffer/packed_fast_transfer_source_get_buffer_response_payloads
---

# Signature

`fn fast_transfer_source_transfer_position( session: &MapiSession, handle_slots: &[u32], request: &RopRequest, ) -> Option<usize>`

# Calls

- [input_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_object.md)

# Called by

- [packed_fast_transfer_source_get_buffer_response_payloads](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_get_buffer/packed_fast_transfer_source_get_buffer_response_payloads.md)