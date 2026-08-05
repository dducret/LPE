---
type: Rust Function
title: rop_buffer_is_store_independent_release_only
resource: crates/lpe-exchange/src/mapi/dispatch/execute.rs#L45-L61
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/remaining
  - functions/crates/lpe-exchange/src/mapi/rop/request_reader/read_rop_request
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute/execute_can_skip_identity_scope
---

# Signature

`pub(super) fn rop_buffer_is_store_independent_release_only(rop_buffer: &[u8]) -> bool`

# Calls

- [remaining](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/remaining.md)
- [read_rop_request](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/request_reader/read_rop_request.md)

# Called by

- [execute_can_skip_identity_scope](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute/execute_can_skip_identity_scope.md)