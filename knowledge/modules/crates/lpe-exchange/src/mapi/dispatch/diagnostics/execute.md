---
type: Rust Module
title: execute
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/execute.rs#L1-L713
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/super
  - external/crate-mapi-outlook-startup-normal-inbox-visible-row-missing-reason-normal-inbox-visible-row-release-request-shape-normalized-rop-sequence-signature-outlook-startup-gate-summary
  - external/crate-mapi-session-posthierarchyexecuteobservation
  - external/crate-mapi-transport-post-hierarchy-action-summary
  member_of:
  - packages/crates/lpe-exchange
---

# Contains

- [log_execute_rop_debug](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/execute/log_execute_rop_debug.md)
- [should_log_execute_stalled_before_content_sync](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/execute/should_log_execute_stalled_before_content_sync.md)
- [log_execute_dispatch_start_debug](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/execute/log_execute_dispatch_start_debug.md)
- [log_execute_parse_failure_debug](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/execute/log_execute_parse_failure_debug.md)
- [read_le_u32_at](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/execute/read_le_u32_at.md)
- [format_hex_u32](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/execute/format_hex_u32.md)

# Imports

- `super::*`
- `crate::mapi::outlook_startup::{
    normal_inbox_visible_row_missing_reason, normal_inbox_visible_row_release_request_shape,
    normalized_rop_sequence_signature, outlook_startup_gate_summary,
}`
- `crate::mapi::session::PostHierarchyExecuteObservation`
- `crate::mapi::transport::post_hierarchy_action_summary`

# Member of

- [lpe-exchange](../../../../../../../packages/crates/lpe-exchange.md)