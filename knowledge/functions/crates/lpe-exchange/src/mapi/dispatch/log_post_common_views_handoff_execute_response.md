---
type: Rust Function
title: log_post_common_views_handoff_execute_response
resource: crates/lpe-exchange/src/mapi/dispatch.rs#L705-L934
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/transport/cookies/request_cookie_transport_debug
  - functions/crates/lpe-exchange/src/mapi/transport/cookies/cookie_value_debug
  - functions/crates/lpe-exchange/src/mapi/transport/cookies/request_sequence_cookie_matches
  - functions/crates/lpe-exchange/src/mapi/outlook_startup/outlook_startup_gate_summary
  - functions/crates/lpe-exchange/src/mapi/outlook_startup/normal_inbox_visible_row_missing_reason
  - functions/crates/lpe-exchange/src/mapi/outlook_startup/normal_inbox_visible_row_release_request_shape
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/advertised_default_view_pending_open
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/default_view_advertisement_state
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/default_view_advertisement_summary
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/post_hierarchy/format_inbox_post_fai_handoff_context
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/format_live_handle_debug_summary
  - functions/crates/lpe-core/src/outlook_trace/write_outlook_trace
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute_response
---

# Signature

`fn log_post_common_views_handoff_execute_response( endpoint: MapiEndpoint, principal: &AccountPrincipal, headers: &HeaderMap, session_id: &str, request_id: &str, session: &MapiSession, request: &RopRequestDebugSummary, response: &RopResponseDebugSummary, response_body_bytes: usize, cached_execute_response: bool, )`

# Calls

- [request_cookie_transport_debug](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/request_cookie_transport_debug.md)
- [cookie_value_debug](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/cookie_value_debug.md)
- [request_sequence_cookie_matches](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/cookies/request_sequence_cookie_matches.md)
- [outlook_startup_gate_summary](../../../../../../functions/crates/lpe-exchange/src/mapi/outlook_startup/outlook_startup_gate_summary.md)
- [normal_inbox_visible_row_missing_reason](../../../../../../functions/crates/lpe-exchange/src/mapi/outlook_startup/normal_inbox_visible_row_missing_reason.md)
- [normal_inbox_visible_row_release_request_shape](../../../../../../functions/crates/lpe-exchange/src/mapi/outlook_startup/normal_inbox_visible_row_release_request_shape.md)
- [advertised_default_view_pending_open](../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/advertised_default_view_pending_open.md)
- [default_view_advertisement_state](../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/default_view_advertisement_state.md)
- [default_view_advertisement_summary](../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/default_view_advertisement_summary.md)
- [format_inbox_post_fai_handoff_context](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/post_hierarchy/format_inbox_post_fai_handoff_context.md)
- [format_live_handle_debug_summary](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/format_live_handle_debug_summary.md)
- [write_outlook_trace](../../../../../../functions/crates/lpe-core/src/outlook_trace/write_outlook_trace.md)

# Called by

- [execute_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute_response.md)