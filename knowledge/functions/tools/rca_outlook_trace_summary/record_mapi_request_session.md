---
type: Python Function
title: record_mapi_request_session
resource: tools/rca_outlook_trace_summary.py#L2263-L2266
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockClassList/add
  called_by:
  - functions/tools/rca_outlook_trace_summary/summarize_rr
  - functions/tools/rca_outlook_trace_summary/summarize_log
---

# Signature

`def record_mapi_request_session(sessions: set[str], request_id: Any) -> None:`

# Calls

- [add](../../../functions/LPE-CT/web/app/smoke/test/MockClassList/add.md)

# Called by

- [summarize_rr](../../../functions/tools/rca_outlook_trace_summary/summarize_rr.md)
- [summarize_log](../../../functions/tools/rca_outlook_trace_summary/summarize_log.md)