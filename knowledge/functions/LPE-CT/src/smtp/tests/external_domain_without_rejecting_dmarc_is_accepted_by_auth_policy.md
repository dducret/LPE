---
type: Rust Function
title: external_domain_without_rejecting_dmarc_is_accepted_by_auth_policy
resource: LPE-CT/src/smtp/tests.rs#L3281-L3297
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/smtp/tests/auth_policy_config
  - functions/LPE-CT/src/smtp/tests/decide_auth_policy
---

# Signature

`fn external_domain_without_rejecting_dmarc_is_accepted_by_auth_policy()`

# Calls

- [auth_policy_config](../../../../../functions/LPE-CT/src/smtp/tests/auth_policy_config.md)
- [decide_auth_policy](../../../../../functions/LPE-CT/src/smtp/tests/decide_auth_policy.md)