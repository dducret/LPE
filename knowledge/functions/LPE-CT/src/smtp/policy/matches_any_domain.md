---
type: Rust Function
title: matches_any_domain
resource: LPE-CT/src/smtp/policy.rs#L31-L36
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/smtp/outbound_policy/resolve_outbound_route
  - functions/LPE-CT/src/smtp/outbound_policy/evaluate_outbound_throttle
---

# Signature

`pub(super) fn matches_any_domain(expected: Option<&str>, actual: &[String]) -> bool`

# Called by

- [resolve_outbound_route](../../../../../functions/LPE-CT/src/smtp/outbound_policy/resolve_outbound_route.md)
- [evaluate_outbound_throttle](../../../../../functions/LPE-CT/src/smtp/outbound_policy/evaluate_outbound_throttle.md)