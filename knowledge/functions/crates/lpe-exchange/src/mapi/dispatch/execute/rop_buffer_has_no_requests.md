---
type: Rust Function
title: rop_buffer_has_no_requests
resource: crates/lpe-exchange/src/mapi/dispatch/execute.rs#L155-L159
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute/execute_can_skip_identity_scope
---

# Signature

`pub(super) fn rop_buffer_has_no_requests(rop_buffer: &[u8]) -> bool`

# Called by

- [execute_can_skip_identity_scope](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute/execute_can_skip_identity_scope.md)