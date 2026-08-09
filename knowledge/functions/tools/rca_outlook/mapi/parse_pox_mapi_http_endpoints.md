---
type: Python Function
title: parse_pox_mapi_http_endpoints
resource: tools/rca_outlook/mapi.py#L36-L73
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/Parser/next
  - functions/tools/rca_outlook/mapi/xml_local_name
  - functions/tools/rca_outlook/http/require
  - functions/tools/rca_outlook/mapi/xml_child_text
  - functions/crates/lpe-exchange/src/mapi_mailstore/client_state/CounterSet/intersection
  called_by:
  - functions/tools/rca_outlook_connectivity_check/check_mapi_gate1_readiness
  - functions/tools/test_rca_outlook_connectivity_check/MapiGate1ReadinessTests/test_parse_pox_mapi_http_uses_exactly_one_publication
---

# Signature

`def parse_pox_mapi_http_endpoints(xml: str, email: str) -> MapiHttpEndpoints:`

# Calls

- [next](../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)
- [xml_local_name](../../../../functions/tools/rca_outlook/mapi/xml_local_name.md)
- [require](../../../../functions/tools/rca_outlook/http/require.md)
- [xml_child_text](../../../../functions/tools/rca_outlook/mapi/xml_child_text.md)
- [intersection](../../../../functions/crates/lpe-exchange/src/mapi_mailstore/client_state/CounterSet/intersection.md)

# Called by

- [check_mapi_gate1_readiness](../../../../functions/tools/rca_outlook_connectivity_check/check_mapi_gate1_readiness.md)
- [test_parse_pox_mapi_http_uses_exactly_one_publication](../../../../functions/tools/test_rca_outlook_connectivity_check/MapiGate1ReadinessTests/test_parse_pox_mapi_http_uses_exactly_one_publication.md)