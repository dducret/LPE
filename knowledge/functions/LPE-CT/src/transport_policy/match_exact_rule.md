---
type: Rust Function
title: match_exact_rule
resource: LPE-CT/src/transport_policy.rs#L463-L468
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/transport_policy/evaluate_attachment_policy_with_config
---

# Signature

`fn match_exact_rule<'a>(rules: &'a [String], value: &str) -> Option<&'a str>`

# Called by

- [evaluate_attachment_policy_with_config](../../../../functions/LPE-CT/src/transport_policy/evaluate_attachment_policy_with_config.md)