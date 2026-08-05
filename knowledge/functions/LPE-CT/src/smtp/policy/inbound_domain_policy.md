---
type: Rust Function
title: inbound_domain_policy
resource: LPE-CT/src/smtp/policy.rs#L38-L92
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/smtp/inbound_policy/evaluate_inbound_policy
---

# Signature

`pub(super) fn inbound_domain_policy( config: &RuntimeConfig, rcpt_to: &[String], ) -> InboundDomainPolicy`

# Called by

- [evaluate_inbound_policy](../../../../../functions/LPE-CT/src/smtp/inbound_policy/evaluate_inbound_policy.md)