---
type: Rust Function
title: latest_decision_summary
resource: LPE-CT/src/smtp/trace.rs#L35-L39
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/smtp/audit/quarantine_search_text
  - functions/LPE-CT/src/smtp/trace/quarantine_summary_from_message
---

# Signature

`pub(in crate::smtp) fn latest_decision_summary(trace: &[DecisionTraceEntry]) -> Option<String>`

# Called by

- [quarantine_search_text](../../../../../functions/LPE-CT/src/smtp/audit/quarantine_search_text.md)
- [quarantine_summary_from_message](../../../../../functions/LPE-CT/src/smtp/trace/quarantine_summary_from_message.md)