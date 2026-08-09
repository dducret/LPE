---
type: Python Function
title: pox_response
resource: tools/test_rca_outlook_connectivity_check.py#L20-L33
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/tools/test_rca_outlook_connectivity_check/MapiGate1ReadinessTests/test_parse_pox_mapi_http_uses_exactly_one_publication
  - functions/tools/test_rca_outlook_connectivity_check/fake_request
---

# Signature

`def pox_response(include_legacy: bool = False) -> str:`

# Called by

- [test_parse_pox_mapi_http_uses_exactly_one_publication](../../../functions/tools/test_rca_outlook_connectivity_check/MapiGate1ReadinessTests/test_parse_pox_mapi_http_uses_exactly_one_publication.md)
- [fake_request](../../../functions/tools/test_rca_outlook_connectivity_check/fake_request.md)