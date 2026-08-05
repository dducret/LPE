---
type: Rust Function
title: partial_scope_checkpoint_not_stored_count
resource: crates/lpe-exchange/src/mapi/transport/diagnostics.rs#L662-L670
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/transport/diagnostics/log_mapi_session_disconnect
---

# Signature

`pub(in crate::mapi) fn partial_scope_checkpoint_not_stored_count( actions: &PostHierarchyActionState, ) -> usize`

# Called by

- [log_mapi_session_disconnect](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/diagnostics/log_mapi_session_disconnect.md)