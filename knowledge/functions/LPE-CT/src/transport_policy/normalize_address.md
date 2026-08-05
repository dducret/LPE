---
type: Rust Function
title: normalize_address
resource: LPE-CT/src/transport_policy.rs#L445-L447
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/transport_policy/evaluate_address_policy_with_config
  - functions/LPE-CT/src/transport_policy/verify_recipient_with_core
---

# Signature

`fn normalize_address(value: &str) -> String`

# Called by

- [evaluate_address_policy_with_config](../../../../functions/LPE-CT/src/transport_policy/evaluate_address_policy_with_config.md)
- [verify_recipient_with_core](../../../../functions/LPE-CT/src/transport_policy/verify_recipient_with_core.md)