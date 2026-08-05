---
type: Rust Function
title: rejected_smtp_reply
resource: LPE-CT/src/smtp/dsn.rs#L43-L59
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/smtp/session/handle_smtp_command
---

# Signature

`pub(super) fn rejected_smtp_reply(message: &QueuedMessage) -> String`

# Called by

- [handle_smtp_command](../../../../../functions/LPE-CT/src/smtp/session/handle_smtp_command.md)