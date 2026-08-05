---
type: Python Function
title: record_calendar_contract_fingerprint
resource: tools/rca_outlook_trace_summary.py#L1687-L1712
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/rca_outlook_trace_summary/first_field
  - functions/LPE-CT/web/app/smoke/test/MockClassList/add
  called_by:
  - functions/tools/rca_outlook_trace_summary/summarize_log
  - functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_calendar_contract_fingerprint_records_stage_and_invariant_issue
---

# Signature

`def record_calendar_contract_fingerprint( summary: dict[str, Any], fields: dict[str, Any] ) -> None:`

# Calls

- [first_field](../../../functions/tools/rca_outlook_trace_summary/first_field.md)
- [add](../../../functions/LPE-CT/web/app/smoke/test/MockClassList/add.md)

# Called by

- [summarize_log](../../../functions/tools/rca_outlook_trace_summary/summarize_log.md)
- [test_calendar_contract_fingerprint_records_stage_and_invariant_issue](../../../functions/tools/test_rca_outlook_trace_summary/RcaOutlookTraceSummaryTests/test_calendar_contract_fingerprint_records_stage_and_invariant_issue.md)