---
type: Rust Function
title: evaluate_address_policy
resource: LPE-CT/src/transport_policy.rs#L118-L121
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/transport_policy/address_policy_config_from_env
  - functions/LPE-CT/src/transport_policy/evaluate_address_policy_with_config
---

# Signature

`pub(crate) fn evaluate_address_policy(role: AddressRole, address: &str) -> AddressPolicyVerdict`

# Calls

- [address_policy_config_from_env](../../../../functions/LPE-CT/src/transport_policy/address_policy_config_from_env.md)
- [evaluate_address_policy_with_config](../../../../functions/LPE-CT/src/transport_policy/evaluate_address_policy_with_config.md)