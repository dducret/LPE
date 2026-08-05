---
type: Python Function
title: record_query_rows_response_frames
resource: tools/rca_outlook_trace_summary.py#L642-L680
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/tools/rca_outlook_trace_summary/query_rows_preview_text_hint
  called_by:
  - functions/tools/rca_outlook_trace_summary/summarize_log
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_query_rows_response_frames_are_counted_by_signature
---

# Signature

`def record_query_rows_response_frames( summary: dict[str, Any], fields: dict[str, Any], signature: str ) -> None:`

# Calls

- [get](../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [query_rows_preview_text_hint](../../../functions/tools/rca_outlook_trace_summary/query_rows_preview_text_hint.md)

# Called by

- [summarize_log](../../../functions/tools/rca_outlook_trace_summary/summarize_log.md)
- [test_query_rows_response_frames_are_counted_by_signature](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_query_rows_response_frames_are_counted_by_signature.md)