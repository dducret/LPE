---
type: Python Function
title: login
resource: tools/jmap_live_shared_delegated_check.py#L100-L120
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-core/src/sieve/Parser/next
---

# Signature

`def login(base_url: str, email: str, password: str) -> AccountLogin:`

# Calls

- [get](../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [next](../../../functions/crates/lpe-core/src/sieve/Parser/next.md)