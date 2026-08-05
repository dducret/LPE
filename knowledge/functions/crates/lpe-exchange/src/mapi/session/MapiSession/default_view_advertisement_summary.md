---
type: Rust Method
title: default_view_advertisement_summary
resource: crates/lpe-exchange/src/mapi/session.rs#L828-L839
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/default_view_advertisement_state
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/log_post_common_views_handoff_execute_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/default_view_advertisement_state_tracks_multiple_owner_folders
  - functions/crates/lpe-exchange/src/mapi/transport/diagnostics/log_mapi_session_disconnect
---

# Signature

`pub(in crate::mapi) fn default_view_advertisement_summary(&self) -> String`

# Calls

- [default_view_advertisement_state](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/default_view_advertisement_state.md)

# Called by

- [log_post_common_views_handoff_execute_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/log_post_common_views_handoff_execute_response.md)
- [default_view_advertisement_state_tracks_multiple_owner_folders](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/default_view_advertisement_state_tracks_multiple_owner_folders.md)
- [log_mapi_session_disconnect](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/diagnostics/log_mapi_session_disconnect.md)