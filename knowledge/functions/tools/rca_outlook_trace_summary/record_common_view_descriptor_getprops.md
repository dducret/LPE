---
type: Python Function
title: record_common_view_descriptor_getprops
resource: tools/rca_outlook_trace_summary.py#L876-L928
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/rca_outlook_trace_summary/first_field
  - functions/LPE-CT/web/app/smoke/test/MockClassList/add
  - functions/tools/rca_outlook_trace_summary/inbox_compact_descriptor_getprops_contract_issues
  called_by:
  - functions/tools/rca_outlook_trace_summary/summarize_log
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_common_view_descriptor_getprops_contract_is_reported
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_common_view_descriptor_getprops_dedupes_surface_and_debug_events
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_common_view_descriptor_getprops_flags_malformed_inbox_compact_contract
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_missing_common_view_descriptor_getprops_is_actionable
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_structured_common_view_descriptor_getprops_requires_requested_values
---

# Signature

`def record_common_view_descriptor_getprops( summary: dict[str, Any], contract: str, fields: dict[str, Any] ) -> None:`

# Calls

- [first_field](../../../functions/tools/rca_outlook_trace_summary/first_field.md)
- [add](../../../functions/LPE-CT/web/app/smoke/test/MockClassList/add.md)
- [inbox_compact_descriptor_getprops_contract_issues](../../../functions/tools/rca_outlook_trace_summary/inbox_compact_descriptor_getprops_contract_issues.md)

# Called by

- [summarize_log](../../../functions/tools/rca_outlook_trace_summary/summarize_log.md)
- [test_common_view_descriptor_getprops_contract_is_reported](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_common_view_descriptor_getprops_contract_is_reported.md)
- [test_common_view_descriptor_getprops_dedupes_surface_and_debug_events](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_common_view_descriptor_getprops_dedupes_surface_and_debug_events.md)
- [test_common_view_descriptor_getprops_flags_malformed_inbox_compact_contract](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_common_view_descriptor_getprops_flags_malformed_inbox_compact_contract.md)
- [test_missing_common_view_descriptor_getprops_is_actionable](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_missing_common_view_descriptor_getprops_is_actionable.md)
- [test_structured_common_view_descriptor_getprops_requires_requested_values](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_structured_common_view_descriptor_getprops_requires_requested_values.md)