---
type: Python Function
title: umolk_dictionary_issue_context
resource: tools/rca_outlook_trace_summary.py#L848-L852
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/rca_outlook_trace_summary/first_field
  called_by:
  - functions/tools/rca_outlook_trace_summary/record_umolk_dictionary_shapes
  - functions/tools/rca_outlook_trace_summary/record_umolk_dictionary_contract
---

# Signature

`def umolk_dictionary_issue_context(text: str, issue: str) -> str:`

# Calls

- [first_field](../../../functions/tools/rca_outlook_trace_summary/first_field.md)

# Called by

- [record_umolk_dictionary_shapes](../../../functions/tools/rca_outlook_trace_summary/record_umolk_dictionary_shapes.md)
- [record_umolk_dictionary_contract](../../../functions/tools/rca_outlook_trace_summary/record_umolk_dictionary_contract.md)