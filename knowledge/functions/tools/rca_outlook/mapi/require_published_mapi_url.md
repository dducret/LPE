---
type: Python Function
title: require_published_mapi_url
resource: tools/rca_outlook/mapi.py#L76-L91
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/rca_outlook/http/require
  called_by:
  - functions/tools/rca_outlook_connectivity_check/check_mapi_gate1_readiness
  - functions/tools/test_rca_outlook_connectivity_check/MapiGate1ReadinessTests/test_published_url_must_stay_on_public_edge_and_mailbox
---

# Signature

`def require_published_mapi_url(url: str, expected_host: str, email: str, endpoint: str) -> None:`

# Calls

- [require](../../../../functions/tools/rca_outlook/http/require.md)

# Called by

- [check_mapi_gate1_readiness](../../../../functions/tools/rca_outlook_connectivity_check/check_mapi_gate1_readiness.md)
- [test_published_url_must_stay_on_public_edge_and_mailbox](../../../../functions/tools/test_rca_outlook_connectivity_check/MapiGate1ReadinessTests/test_published_url_must_stay_on_public_edge_and_mailbox.md)