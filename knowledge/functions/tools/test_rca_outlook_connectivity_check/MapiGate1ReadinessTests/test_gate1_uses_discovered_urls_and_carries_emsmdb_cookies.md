---
type: Python Method
title: test_gate1_uses_discovered_urls_and_carries_emsmdb_cookies
resource: tools/test_rca_outlook_connectivity_check.py#L85-L153
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/rca_outlook_connectivity_check/check_mapi_gate1_readiness
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
---

# Signature

`def test_gate1_uses_discovered_urls_and_carries_emsmdb_cookies(self) -> None:`

# Calls

- [check_mapi_gate1_readiness](../../../../functions/tools/rca_outlook_connectivity_check/check_mapi_gate1_readiness.md)
- [get](../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)