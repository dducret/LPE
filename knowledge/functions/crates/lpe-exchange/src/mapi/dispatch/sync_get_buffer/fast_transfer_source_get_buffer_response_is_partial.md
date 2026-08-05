---
type: Rust Function
title: fast_transfer_source_get_buffer_response_is_partial
resource: crates/lpe-exchange/src/mapi/dispatch/sync_get_buffer.rs#L25-L30
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_get_buffer/packed_fast_transfer_source_get_buffer_response_payloads
---

# Signature

`pub(super) fn fast_transfer_source_get_buffer_response_is_partial(response: &[u8]) -> bool`

# Called by

- [execute_rops](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops.md)
- [packed_fast_transfer_source_get_buffer_response_payloads](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_get_buffer/packed_fast_transfer_source_get_buffer_response_payloads.md)