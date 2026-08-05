---
type: Rust Function
title: record_mapi_outlook_view_bootstrap_progress
resource: crates/lpe-exchange/src/mapi.rs#L229-L244
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/transport/diagnostics/post_hierarchy_action_summary
---

# Signature

`pub(crate) fn record_mapi_outlook_view_bootstrap_progress( phase: u64, stall_code: u64, inbox_open_probe_count: usize, inbox_folder_type_getprops_probe_count: usize, )`

# Called by

- [post_hierarchy_action_summary](../../../../../functions/crates/lpe-exchange/src/mapi/transport/diagnostics/post_hierarchy_action_summary.md)