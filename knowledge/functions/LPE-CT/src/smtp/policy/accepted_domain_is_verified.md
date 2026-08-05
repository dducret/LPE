---
type: Rust Function
title: accepted_domain_is_verified
resource: LPE-CT/src/smtp/policy.rs#L104-L109
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/smtp/outbound_delivery/relay_message_direct_mx
  - functions/LPE-CT/src/smtp/policy/recipient_domain_is_accepted
---

# Signature

`pub(super) fn accepted_domain_is_verified(config: &RuntimeConfig, domain: &str) -> bool`

# Called by

- [relay_message_direct_mx](../../../../../functions/LPE-CT/src/smtp/outbound_delivery/relay_message_direct_mx.md)
- [recipient_domain_is_accepted](../../../../../functions/LPE-CT/src/smtp/policy/recipient_domain_is_accepted.md)