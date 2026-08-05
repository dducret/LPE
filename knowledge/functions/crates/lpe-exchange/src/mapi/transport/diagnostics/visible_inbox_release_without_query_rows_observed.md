---
type: Rust Function
title: visible_inbox_release_without_query_rows_observed
resource: crates/lpe-exchange/src/mapi/transport/diagnostics.rs#L550-L559
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/record_post_calendar_query_position_named_property_probe
  - functions/crates/lpe-exchange/src/mapi/transport/diagnostics/post_hierarchy_close_kind
  - functions/crates/lpe-exchange/src/mapi/transport/diagnostics/advertised_default_view_pending_open_is_primary
  - functions/crates/lpe-exchange/src/mapi/transport/diagnostics/log_mapi_session_disconnect
---

# Signature

`pub(in crate::mapi) fn visible_inbox_release_without_query_rows_observed( actions: &PostHierarchyActionState, ) -> bool`

# Called by

- [record_post_calendar_query_position_named_property_probe](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/record_post_calendar_query_position_named_property_probe.md)
- [post_hierarchy_close_kind](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/diagnostics/post_hierarchy_close_kind.md)
- [advertised_default_view_pending_open_is_primary](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/diagnostics/advertised_default_view_pending_open_is_primary.md)
- [log_mapi_session_disconnect](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/diagnostics/log_mapi_session_disconnect.md)