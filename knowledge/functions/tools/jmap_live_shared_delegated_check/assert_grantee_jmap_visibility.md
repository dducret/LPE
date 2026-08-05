---
type: Python Function
title: assert_grantee_jmap_visibility
resource: tools/jmap_live_shared_delegated_check.py#L360-L398
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/tools/jmap_live_shared_delegated_check/main
---

# Signature

`def assert_grantee_jmap_visibility(owner: AccountLogin, grantee: AccountLogin) -> None:`

# Calls

- [get](../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [main](../../../functions/tools/jmap_live_shared_delegated_check/main.md)