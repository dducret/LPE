---
type: Python Function
title: record_mixed_release_queryposition_response
resource: tools/rca_outlook_trace_summary.py#L1407-L1425
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/rca_outlook_trace_summary/classify_mixed_release_queryposition_response
  called_by:
  - functions/tools/rca_outlook_trace_summary/summarize_log
---

# Signature

`def record_mixed_release_queryposition_response( summary: dict[str, Any], fields: dict[str, Any] ) -> None:`

# Calls

- [classify_mixed_release_queryposition_response](../../../functions/tools/rca_outlook_trace_summary/classify_mixed_release_queryposition_response.md)

# Called by

- [summarize_log](../../../functions/tools/rca_outlook_trace_summary/summarize_log.md)