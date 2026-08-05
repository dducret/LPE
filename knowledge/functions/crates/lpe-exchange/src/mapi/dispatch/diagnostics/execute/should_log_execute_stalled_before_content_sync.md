---
type: Rust Function
title: should_log_execute_stalled_before_content_sync
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/execute.rs#L603-L616
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/execute/log_execute_rop_debug
---

# Signature

`pub(in crate::mapi::dispatch) fn should_log_execute_stalled_before_content_sync( endpoint: &str, last_completed_hierarchy_sync_root: &str, content_sync_configure_observed: bool, post_hierarchy_close_kind: &str, ) -> bool`

# Called by

- [log_execute_rop_debug](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/execute/log_execute_rop_debug.md)