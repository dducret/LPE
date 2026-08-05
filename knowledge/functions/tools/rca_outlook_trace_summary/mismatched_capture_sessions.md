---
type: Python Function
title: mismatched_capture_sessions
resource: tools/rca_outlook_trace_summary.py#L2269-L2278
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/tools/rca_outlook_trace_summary/print_single_summary
  - functions/tools/rca_outlook_trace_summary/verdict_for_summary
---

# Signature

`def mismatched_capture_sessions( rr: dict[str, Any], log: dict[str, Any], log_path: Path | None ) -> tuple[set[str], set[str]] | None:`

# Calls

- [get](../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [print_single_summary](../../../functions/tools/rca_outlook_trace_summary/print_single_summary.md)
- [verdict_for_summary](../../../functions/tools/rca_outlook_trace_summary/verdict_for_summary.md)