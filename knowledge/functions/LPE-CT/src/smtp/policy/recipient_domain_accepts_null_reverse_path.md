---
type: Rust Function
title: recipient_domain_accepts_null_reverse_path
resource: LPE-CT/src/smtp/policy.rs#L111-L126
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/smtp/session/handle_smtp_command
---

# Signature

`pub(super) fn recipient_domain_accepts_null_reverse_path( config: &RuntimeConfig, recipient: &str, ) -> bool`

# Called by

- [handle_smtp_command](../../../../../functions/LPE-CT/src/smtp/session/handle_smtp_command.md)