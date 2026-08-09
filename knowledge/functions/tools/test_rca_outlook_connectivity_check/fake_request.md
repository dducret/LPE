---
type: Python Function
title: fake_request
resource: tools/test_rca_outlook_connectivity_check.py#L89-L134
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/test_rca_outlook_connectivity_check/pox_response
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/tools/test_rca_outlook_connectivity_check/hierarchy_rops
---

# Signature

`def fake_request(method, url, body=None, headers=None, *args, **kwargs):`

# Calls

- [pox_response](../../../functions/tools/test_rca_outlook_connectivity_check/pox_response.md)
- [get](../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [hierarchy_rops](../../../functions/tools/test_rca_outlook_connectivity_check/hierarchy_rops.md)