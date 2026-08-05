---
type: Python Function
title: print_single_summary
resource: tools/rca_outlook_trace_summary.py#L2281-L2704
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/rca_outlook_trace_summary/summarize_rr
  - functions/tools/rca_outlook_trace_summary/summarize_log
  - functions/tools/rca_outlook_trace_summary/print_counter
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/tools/rca_outlook_trace_summary/format_build_dirty
  - functions/tools/rca_outlook_trace_summary/mismatched_capture_sessions
  - functions/tools/rca_outlook_trace_summary/unknown_tag_class_counts
  - functions/tools/rca_outlook_trace_summary/actionable_zero_default_tag_counts
  - functions/tools/rca_outlook_trace_summary/actionable_descriptor_gap_counts
  - functions/tools/rca_outlook_trace_summary/verdict_for_summary
  called_by:
  - functions/tools/rca_outlook_trace_summary/main
---

# Signature

`def print_single_summary( trace_dir: Path, log_path: Path | None ) -> tuple[dict[str, Any], dict[str, Any], str]:`

# Calls

- [summarize_rr](../../../functions/tools/rca_outlook_trace_summary/summarize_rr.md)
- [summarize_log](../../../functions/tools/rca_outlook_trace_summary/summarize_log.md)
- [print_counter](../../../functions/tools/rca_outlook_trace_summary/print_counter.md)
- [get](../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [format_build_dirty](../../../functions/tools/rca_outlook_trace_summary/format_build_dirty.md)
- [mismatched_capture_sessions](../../../functions/tools/rca_outlook_trace_summary/mismatched_capture_sessions.md)
- [unknown_tag_class_counts](../../../functions/tools/rca_outlook_trace_summary/unknown_tag_class_counts.md)
- [actionable_zero_default_tag_counts](../../../functions/tools/rca_outlook_trace_summary/actionable_zero_default_tag_counts.md)
- [actionable_descriptor_gap_counts](../../../functions/tools/rca_outlook_trace_summary/actionable_descriptor_gap_counts.md)
- [verdict_for_summary](../../../functions/tools/rca_outlook_trace_summary/verdict_for_summary.md)

# Called by

- [main](../../../functions/tools/rca_outlook_trace_summary/main.md)