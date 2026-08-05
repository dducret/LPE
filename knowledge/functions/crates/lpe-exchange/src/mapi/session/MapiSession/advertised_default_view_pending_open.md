---
type: Rust Method
title: advertised_default_view_pending_open
resource: crates/lpe-exchange/src/mapi/session.rs#L887-L901
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/log_post_common_views_handoff_execute_response
  - functions/crates/lpe-exchange/src/mapi/transport/diagnostics/advertised_default_view_pending_open_is_primary
  - functions/crates/lpe-exchange/src/mapi/transport/diagnostics/log_mapi_session_disconnect
---

# Signature

`pub(in crate::mapi) fn advertised_default_view_pending_open(&self) -> bool`

# Called by

- [log_post_common_views_handoff_execute_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/log_post_common_views_handoff_execute_response.md)
- [advertised_default_view_pending_open_is_primary](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/diagnostics/advertised_default_view_pending_open_is_primary.md)
- [log_mapi_session_disconnect](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/diagnostics/log_mapi_session_disconnect.md)