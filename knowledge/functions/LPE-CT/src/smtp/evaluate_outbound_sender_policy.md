---
type: Rust Function
title: evaluate_outbound_sender_policy
resource: LPE-CT/src/smtp.rs#L1190-L1206
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/smtp/outbound_sender_policy_addresses
  - functions/LPE-CT/src/transport_policy/evaluate_address_policy_with_config
  called_by:
  - functions/LPE-CT/src/smtp/process_outbound_handoff
---

# Signature

`fn evaluate_outbound_sender_policy( config: &RuntimeConfig, payload: &OutboundMessageHandoffRequest, ) -> Option<String>`

# Calls

- [outbound_sender_policy_addresses](../../../../functions/LPE-CT/src/smtp/outbound_sender_policy_addresses.md)
- [evaluate_address_policy_with_config](../../../../functions/LPE-CT/src/transport_policy/evaluate_address_policy_with_config.md)

# Called by

- [process_outbound_handoff](../../../../functions/LPE-CT/src/smtp/process_outbound_handoff.md)