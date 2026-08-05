---
type: Rust Function
title: normal_inbox_visible_row_missing_reason
resource: crates/lpe-exchange/src/mapi/outlook_startup.rs#L116-L141
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/log_post_common_views_handoff_execute_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/execute/log_execute_rop_debug
  - functions/crates/lpe-exchange/src/mapi/transport/diagnostics/log_mapi_session_disconnect
---

# Signature

`pub(in crate::mapi) fn normal_inbox_visible_row_missing_reason( session: &MapiSession, ) -> &'static str`

# Called by

- [log_post_common_views_handoff_execute_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/log_post_common_views_handoff_execute_response.md)
- [log_execute_rop_debug](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/execute/log_execute_rop_debug.md)
- [log_mapi_session_disconnect](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/diagnostics/log_mapi_session_disconnect.md)