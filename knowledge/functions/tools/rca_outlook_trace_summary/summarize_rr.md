---
type: Python Function
title: summarize_rr
resource: tools/rca_outlook_trace_summary.py#L247-L311
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/rca_outlook_trace_summary/trace_jsonl_paths
  - functions/tools/test_rca_outlook_trace_summary/FakePath/open
  - functions/tools/rca_outlook_trace_summary/load_json_line
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/tools/rca_outlook_trace_summary/record_mapi_request_session
  - functions/tools/rca_outlook_trace_summary/rr_event_tail_summary
  - functions/tools/rca_outlook_trace_summary/classify_rr_setcolumns_release_response
  called_by:
  - functions/tools/rca_outlook_trace_summary/print_single_summary
  - functions/tools/rca_outlook_trace_summary/print_batch_summary
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_rr_summary_counts_setcolumns_release_response_frame
---

# Signature

`def summarize_rr(trace_dir: Path) -> dict[str, Any]:`

# Calls

- [trace_jsonl_paths](../../../functions/tools/rca_outlook_trace_summary/trace_jsonl_paths.md)
- [open](../../../functions/tools/test_rca_outlook_trace_summary/FakePath/open.md)
- [load_json_line](../../../functions/tools/rca_outlook_trace_summary/load_json_line.md)
- [get](../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [record_mapi_request_session](../../../functions/tools/rca_outlook_trace_summary/record_mapi_request_session.md)
- [rr_event_tail_summary](../../../functions/tools/rca_outlook_trace_summary/rr_event_tail_summary.md)
- [classify_rr_setcolumns_release_response](../../../functions/tools/rca_outlook_trace_summary/classify_rr_setcolumns_release_response.md)

# Called by

- [print_single_summary](../../../functions/tools/rca_outlook_trace_summary/print_single_summary.md)
- [print_batch_summary](../../../functions/tools/rca_outlook_trace_summary/print_batch_summary.md)
- [test_rr_summary_counts_setcolumns_release_response_frame](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_rr_summary_counts_setcolumns_release_response_frame.md)