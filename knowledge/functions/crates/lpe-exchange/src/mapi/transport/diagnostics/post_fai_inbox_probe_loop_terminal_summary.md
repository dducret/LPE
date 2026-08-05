---
type: Rust Function
title: post_fai_inbox_probe_loop_terminal_summary
resource: crates/lpe-exchange/src/mapi/transport/diagnostics.rs#L672-L700
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/transport/diagnostics/log_mapi_session_disconnect
  - functions/crates/lpe-exchange/src/mapi/transport/tests/post_fai_inbox_probe_loop_terminal_summary_requires_no_normal_or_inbox_ics_contents
---

# Signature

`pub(in crate::mapi) fn post_fai_inbox_probe_loop_terminal_summary( actions: &PostHierarchyActionState, ) -> Option<String>`

# Called by

- [log_mapi_session_disconnect](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/diagnostics/log_mapi_session_disconnect.md)
- [post_fai_inbox_probe_loop_terminal_summary_requires_no_normal_or_inbox_ics_contents](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/post_fai_inbox_probe_loop_terminal_summary_requires_no_normal_or_inbox_ics_contents.md)