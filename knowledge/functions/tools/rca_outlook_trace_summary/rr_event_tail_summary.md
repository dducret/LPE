---
type: Python Function
title: rr_event_tail_summary
resource: tools/rca_outlook_trace_summary.py#L345-L369
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/tools/rca_outlook_trace_summary/summarize_rr
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_rr_event_tail_summary_keeps_endpoint_phase_and_codes
---

# Signature

`def rr_event_tail_summary(event: dict[str, Any], metadata: dict[str, Any]) -> str:`

# Calls

- [get](../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [summarize_rr](../../../functions/tools/rca_outlook_trace_summary/summarize_rr.md)
- [test_rr_event_tail_summary_keeps_endpoint_phase_and_codes](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_rr_event_tail_summary_keeps_endpoint_phase_and_codes.md)