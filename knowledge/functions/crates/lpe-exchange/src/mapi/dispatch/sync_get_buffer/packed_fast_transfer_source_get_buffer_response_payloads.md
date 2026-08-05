---
type: Rust Function
title: packed_fast_transfer_source_get_buffer_response_payloads
resource: crates/lpe-exchange/src/mapi/dispatch/sync_get_buffer.rs#L45-L121
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_get_buffer/fast_transfer_source_transfer_position
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_get_buffer/append_fast_transfer_source_get_buffer_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_get_buffer/fast_transfer_source_get_buffer_response_is_partial
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_buffer_with_response_spec
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops
---

# Signature

`pub(super) async fn packed_fast_transfer_source_get_buffer_response_payloads<S: ExchangeStore>( store: &S, principal: &AccountPrincipal, request_id: &str, session: &mut MapiSession, handle_slots: &[u32], request: &RopRequest, initial_rop_buffer_size: usize, max_rop_out: u32, response_handles: &[u32], ) -> (Vec<Vec<u8>>, Option<(u64, String, String)>)`

# Calls

- [fast_transfer_source_transfer_position](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_get_buffer/fast_transfer_source_transfer_position.md)
- [append_fast_transfer_source_get_buffer_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_get_buffer/append_fast_transfer_source_get_buffer_response.md)
- [fast_transfer_source_get_buffer_response_is_partial](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_get_buffer/fast_transfer_source_get_buffer_response_is_partial.md)
- [rop_buffer_with_response_spec](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_buffer_with_response_spec.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [execute_rops](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops.md)