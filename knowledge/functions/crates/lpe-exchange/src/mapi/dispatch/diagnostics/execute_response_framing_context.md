---
type: Rust Function
title: execute_response_framing_context
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics.rs#L932-L1018
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/execute/log_execute_rop_debug
---

# Signature

`pub(super) fn execute_response_framing_context(request_rop_ids: &[u8]) -> Option<&'static str>`

# Called by

- [log_execute_rop_debug](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/execute/log_execute_rop_debug.md)