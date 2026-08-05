---
type: Rust Function
title: advertised_default_view_pending_open_is_primary
resource: crates/lpe-exchange/src/mapi/transport/diagnostics.rs#L655-L660
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/advertised_default_view_pending_open
  - functions/crates/lpe-exchange/src/mapi/transport/diagnostics/visible_inbox_release_without_query_rows_observed
  called_by:
  - functions/crates/lpe-exchange/src/mapi/transport/diagnostics/log_mapi_session_disconnect
---

# Signature

`pub(in crate::mapi) fn advertised_default_view_pending_open_is_primary( session: &MapiSession, ) -> bool`

# Calls

- [advertised_default_view_pending_open](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/advertised_default_view_pending_open.md)
- [visible_inbox_release_without_query_rows_observed](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/diagnostics/visible_inbox_release_without_query_rows_observed.md)

# Called by

- [log_mapi_session_disconnect](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/diagnostics/log_mapi_session_disconnect.md)