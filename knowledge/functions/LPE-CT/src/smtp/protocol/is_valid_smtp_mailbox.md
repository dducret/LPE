---
type: Rust Function
title: is_valid_smtp_mailbox
resource: LPE-CT/src/smtp/protocol.rs#L182-L217
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/smtp/protocol/parse_smtp_path
---

# Signature

`fn is_valid_smtp_mailbox(address: &str) -> bool`

# Called by

- [parse_smtp_path](../../../../../functions/LPE-CT/src/smtp/protocol/parse_smtp_path.md)