---
type: Rust Function
title: rop_fast_transfer_source_get_buffer_response
resource: crates/lpe-exchange/src/mapi/sync/responses.rs#L15-L59
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_get_buffer/append_fast_transfer_source_get_buffer_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/automatic_fast_transfer_buffer_uses_execute_residual_output_budget
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/chained_fast_transfer_get_buffer_repeats_handles_until_done
  - functions/crates/lpe-exchange/src/mapi/sync/tests/fast_transfer_get_transfer_state_one_buffer_matches_exchange_progress_metadata
  - functions/crates/lpe-exchange/src/mapi/sync/tests/fast_transfer_configure_one_buffer_keeps_exchange_ics_progress_metadata
---

# Signature

`pub(in crate::mapi) fn rop_fast_transfer_source_get_buffer_response( request: &RopRequest, transfer_buffer: &[u8], transfer_position: &mut usize, transfer_buffer_size: usize, transfer_state_source: bool, ) -> Vec<u8>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [append_fast_transfer_source_get_buffer_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_get_buffer/append_fast_transfer_source_get_buffer_response.md)
- [automatic_fast_transfer_buffer_uses_execute_residual_output_budget](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/automatic_fast_transfer_buffer_uses_execute_residual_output_budget.md)
- [chained_fast_transfer_get_buffer_repeats_handles_until_done](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/chained_fast_transfer_get_buffer_repeats_handles_until_done.md)
- [fast_transfer_get_transfer_state_one_buffer_matches_exchange_progress_metadata](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/fast_transfer_get_transfer_state_one_buffer_matches_exchange_progress_metadata.md)
- [fast_transfer_configure_one_buffer_keeps_exchange_ics_progress_metadata](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/fast_transfer_configure_one_buffer_keeps_exchange_ics_progress_metadata.md)