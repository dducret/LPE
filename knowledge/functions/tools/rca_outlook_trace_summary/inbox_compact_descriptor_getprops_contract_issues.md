---
type: Python Function
title: inbox_compact_descriptor_getprops_contract_issues
resource: tools/rca_outlook_trace_summary.py#L931-L968
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/rca_outlook_trace_summary/first_field
  called_by:
  - functions/tools/rca_outlook_trace_summary/record_common_view_descriptor_getprops
---

# Signature

`def inbox_compact_descriptor_getprops_contract_issues( contract: str, fields: dict[str, Any], view_name: str | None, folder_id: str | None, ) -> list[str]:`

# Calls

- [first_field](../../../functions/tools/rca_outlook_trace_summary/first_field.md)

# Called by

- [record_common_view_descriptor_getprops](../../../functions/tools/rca_outlook_trace_summary/record_common_view_descriptor_getprops.md)