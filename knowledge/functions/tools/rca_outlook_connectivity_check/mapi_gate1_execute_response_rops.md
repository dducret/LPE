---
type: Python Function
title: mapi_gate1_execute_response_rops
resource: tools/rca_outlook_connectivity_check.py#L126-L130
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/rca_outlook/http/require
  - functions/tools/rca_outlook/http/content_type
  - functions/tools/rca_outlook/mapi/mapi_execute_response_rops
  - functions/tools/rca_outlook/mapi/mapi_http_binary_payload
  called_by:
  - functions/tools/rca_outlook_connectivity_check/execute
---

# Signature

`def mapi_gate1_execute_response_rops(response, label: str) -> bytes:`

# Calls

- [require](../../../functions/tools/rca_outlook/http/require.md)
- [content_type](../../../functions/tools/rca_outlook/http/content_type.md)
- [mapi_execute_response_rops](../../../functions/tools/rca_outlook/mapi/mapi_execute_response_rops.md)
- [mapi_http_binary_payload](../../../functions/tools/rca_outlook/mapi/mapi_http_binary_payload.md)

# Called by

- [execute](../../../functions/tools/rca_outlook_connectivity_check/execute.md)