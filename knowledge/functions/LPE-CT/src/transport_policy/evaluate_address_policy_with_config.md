---
type: Rust Function
title: evaluate_address_policy_with_config
resource: LPE-CT/src/transport_policy.rs#L84-L115
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/transport_policy/normalize_address
  - functions/LPE-CT/src/transport_policy/match_address_rule
  called_by:
  - functions/LPE-CT/src/smtp/process_outbound_handoff
  - functions/LPE-CT/src/smtp/evaluate_outbound_sender_policy
  - functions/LPE-CT/src/smtp/session/handle_smtp_command
  - functions/LPE-CT/src/submission/handle_submission_session
  - functions/LPE-CT/src/transport_policy/evaluate_address_policy
---

# Signature

`pub(crate) fn evaluate_address_policy_with_config( config: &AddressPolicyConfig, role: AddressRole, address: &str, ) -> AddressPolicyVerdict`

# Calls

- [normalize_address](../../../../functions/LPE-CT/src/transport_policy/normalize_address.md)
- [match_address_rule](../../../../functions/LPE-CT/src/transport_policy/match_address_rule.md)

# Called by

- [process_outbound_handoff](../../../../functions/LPE-CT/src/smtp/process_outbound_handoff.md)
- [evaluate_outbound_sender_policy](../../../../functions/LPE-CT/src/smtp/evaluate_outbound_sender_policy.md)
- [handle_smtp_command](../../../../functions/LPE-CT/src/smtp/session/handle_smtp_command.md)
- [handle_submission_session](../../../../functions/LPE-CT/src/submission/handle_submission_session.md)
- [evaluate_address_policy](../../../../functions/LPE-CT/src/transport_policy/evaluate_address_policy.md)