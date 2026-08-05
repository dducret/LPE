---
type: Rust Method
title: default_view_advertisement_state
resource: crates/lpe-exchange/src/mapi/session.rs#L781-L809
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/log_post_common_views_handoff_execute_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/default_view_advertisement_state_marks_matching_open
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/default_view_advertisement_summary
  - functions/crates/lpe-exchange/src/mapi/transport/diagnostics/log_mapi_session_disconnect
---

# Signature

`pub(in crate::mapi) fn default_view_advertisement_state(&self) -> String`

# Called by

- [log_post_common_views_handoff_execute_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/log_post_common_views_handoff_execute_response.md)
- [default_view_advertisement_state_marks_matching_open](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/default_view_advertisement_state_marks_matching_open.md)
- [default_view_advertisement_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/default_view_advertisement_summary.md)
- [log_mapi_session_disconnect](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/diagnostics/log_mapi_session_disconnect.md)