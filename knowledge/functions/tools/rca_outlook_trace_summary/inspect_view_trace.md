---
type: Python Function
title: inspect_view_trace
resource: tools/rca_outlook_trace_summary.py#L1105-L1132
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/rca_outlook_trace_summary/first_field
  - functions/LPE-CT/web/app/smoke/test/MockClassList/add
  - functions/tools/rca_outlook_trace_summary/record_visible_release_context
  - functions/tools/rca_outlook_trace_summary/record_post_visible_release_terminal_event
  - functions/tools/rca_outlook_trace_summary/record_default_view_query_position_without_rows
  - functions/tools/rca_outlook_trace_summary/record_default_view_id_collision
  - functions/tools/rca_outlook_trace_summary/record_inbox_default_view_advertisement
  - functions/tools/rca_outlook_trace_summary/record_default_view_descriptor_identity_columns
  - functions/tools/rca_outlook_trace_summary/record_descriptor_gap
  - functions/tools/rca_outlook_trace_summary/inspect_contract
  called_by:
  - functions/tools/rca_outlook_trace_summary/summarize_log
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_view_trace_classifies_only_direct_visible_release_event
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_view_trace_records_terminal_events_after_visible_release
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_default_view_id_collision_records_reused_folder_local_view_id
---

# Signature

`def inspect_view_trace(summary: dict[str, Any], trace_events: str) -> None:`

# Calls

- [first_field](../../../functions/tools/rca_outlook_trace_summary/first_field.md)
- [add](../../../functions/LPE-CT/web/app/smoke/test/MockClassList/add.md)
- [record_visible_release_context](../../../functions/tools/rca_outlook_trace_summary/record_visible_release_context.md)
- [record_post_visible_release_terminal_event](../../../functions/tools/rca_outlook_trace_summary/record_post_visible_release_terminal_event.md)
- [record_default_view_query_position_without_rows](../../../functions/tools/rca_outlook_trace_summary/record_default_view_query_position_without_rows.md)
- [record_default_view_id_collision](../../../functions/tools/rca_outlook_trace_summary/record_default_view_id_collision.md)
- [record_inbox_default_view_advertisement](../../../functions/tools/rca_outlook_trace_summary/record_inbox_default_view_advertisement.md)
- [record_default_view_descriptor_identity_columns](../../../functions/tools/rca_outlook_trace_summary/record_default_view_descriptor_identity_columns.md)
- [record_descriptor_gap](../../../functions/tools/rca_outlook_trace_summary/record_descriptor_gap.md)
- [inspect_contract](../../../functions/tools/rca_outlook_trace_summary/inspect_contract.md)

# Called by

- [summarize_log](../../../functions/tools/rca_outlook_trace_summary/summarize_log.md)
- [test_view_trace_classifies_only_direct_visible_release_event](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_view_trace_classifies_only_direct_visible_release_event.md)
- [test_view_trace_records_terminal_events_after_visible_release](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_view_trace_records_terminal_events_after_visible_release.md)
- [test_default_view_id_collision_records_reused_folder_local_view_id](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_default_view_id_collision_records_reused_folder_local_view_id.md)