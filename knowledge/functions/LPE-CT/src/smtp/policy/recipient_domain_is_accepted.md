---
type: Rust Function
title: recipient_domain_is_accepted
resource: LPE-CT/src/smtp/policy.rs#L94-L102
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/smtp/policy/accepted_domain_is_verified
  called_by:
  - functions/LPE-CT/src/smtp/session/handle_smtp_command
---

# Signature

`pub(super) fn recipient_domain_is_accepted(config: &RuntimeConfig, recipient: &str) -> bool`

# Calls

- [accepted_domain_is_verified](../../../../../functions/LPE-CT/src/smtp/policy/accepted_domain_is_verified.md)

# Called by

- [handle_smtp_command](../../../../../functions/LPE-CT/src/smtp/session/handle_smtp_command.md)