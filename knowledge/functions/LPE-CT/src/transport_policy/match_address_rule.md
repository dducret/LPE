---
type: Rust Function
title: match_address_rule
resource: LPE-CT/src/transport_policy.rs#L449-L461
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/transport_policy/evaluate_address_policy_with_config
---

# Signature

`fn match_address_rule<'a>(rules: &'a [String], address: &str) -> Option<&'a str>`

# Called by

- [evaluate_address_policy_with_config](../../../../functions/LPE-CT/src/transport_policy/evaluate_address_policy_with_config.md)