---
type: Rust Function
title: select_query_window
resource: crates/lpe-exchange/src/mapi/dispatch/tables.rs#L297-L309
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/associated_config/format_inbox_associated_wire_row_summary
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_inbox_view_descriptor_behavior_contract
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/format_common_views_inbox_shortcut_context
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_lifecycle/format_visible_inbox_first_row_projection_audit
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_inbox_associated_query_row_window
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_common_views_query_row_window
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_outlook_query_row_values_inner
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_normal_message_query_row_summary
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_contact_query_row_summary
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_calendar_event_query_position_summary
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_normal_message_find_row_failure_candidates
---

# Signature

`pub(super) fn select_query_window( total: usize, position: usize, forward_read: bool, row_count: usize, ) -> Vec<usize>`

# Called by

- [format_inbox_associated_wire_row_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/associated_config/format_inbox_associated_wire_row_summary.md)
- [format_inbox_view_descriptor_behavior_contract](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_inbox_view_descriptor_behavior_contract.md)
- [format_common_views_inbox_shortcut_context](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/format_common_views_inbox_shortcut_context.md)
- [format_visible_inbox_first_row_projection_audit](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_lifecycle/format_visible_inbox_first_row_projection_audit.md)
- [format_inbox_associated_query_row_window](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_inbox_associated_query_row_window.md)
- [format_common_views_query_row_window](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_common_views_query_row_window.md)
- [format_outlook_query_row_values_inner](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_outlook_query_row_values_inner.md)
- [format_normal_message_query_row_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_normal_message_query_row_summary.md)
- [format_contact_query_row_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_contact_query_row_summary.md)
- [format_calendar_event_query_position_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_calendar_event_query_position_summary.md)
- [format_normal_message_find_row_failure_candidates](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_normal_message_find_row_failure_candidates.md)