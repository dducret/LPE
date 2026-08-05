---
type: Python Function
title: record_visible_release_context
resource: tools/rca_outlook_trace_summary.py#L1135-L1147
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockClassList/add
  - functions/tools/rca_outlook_trace_summary/record_visible_release_classification
  - functions/tools/rca_outlook_trace_summary/record_visible_release_request_metrics
  - functions/tools/rca_outlook_trace_summary/record_visible_release_setcolumns_shape
  - functions/tools/rca_outlook_trace_summary/record_visible_release_descriptor_window
  - functions/tools/rca_outlook_trace_summary/record_visible_release_descriptor_contract
  called_by:
  - functions/tools/rca_outlook_trace_summary/summarize_log
  - functions/tools/rca_outlook_trace_summary/inspect_view_trace
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_visible_inbox_descriptor_contract_flags_old_compact_shape
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_visible_inbox_descriptor_contract_accepts_ms_oxocfg_compact_shape
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_standalone_visible_release_context_is_classified
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_visible_release_request_metrics_are_counted
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_visible_release_setcolumns_shape_is_counted
---

# Signature

`def record_visible_release_context(summary: dict[str, Any], text: str) -> None:`

# Calls

- [add](../../../functions/LPE-CT/web/app/smoke/test/MockClassList/add.md)
- [record_visible_release_classification](../../../functions/tools/rca_outlook_trace_summary/record_visible_release_classification.md)
- [record_visible_release_request_metrics](../../../functions/tools/rca_outlook_trace_summary/record_visible_release_request_metrics.md)
- [record_visible_release_setcolumns_shape](../../../functions/tools/rca_outlook_trace_summary/record_visible_release_setcolumns_shape.md)
- [record_visible_release_descriptor_window](../../../functions/tools/rca_outlook_trace_summary/record_visible_release_descriptor_window.md)
- [record_visible_release_descriptor_contract](../../../functions/tools/rca_outlook_trace_summary/record_visible_release_descriptor_contract.md)

# Called by

- [summarize_log](../../../functions/tools/rca_outlook_trace_summary/summarize_log.md)
- [inspect_view_trace](../../../functions/tools/rca_outlook_trace_summary/inspect_view_trace.md)
- [test_visible_inbox_descriptor_contract_flags_old_compact_shape](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_visible_inbox_descriptor_contract_flags_old_compact_shape.md)
- [test_visible_inbox_descriptor_contract_accepts_ms_oxocfg_compact_shape](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_visible_inbox_descriptor_contract_accepts_ms_oxocfg_compact_shape.md)
- [test_standalone_visible_release_context_is_classified](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_standalone_visible_release_context_is_classified.md)
- [test_visible_release_request_metrics_are_counted](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_visible_release_request_metrics_are_counted.md)
- [test_visible_release_setcolumns_shape_is_counted](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_visible_release_setcolumns_shape_is_counted.md)