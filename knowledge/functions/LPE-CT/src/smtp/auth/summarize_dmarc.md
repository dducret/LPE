---
type: Rust Function
title: summarize_dmarc
resource: LPE-CT/src/smtp/auth.rs#L210-L218
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/smtp/auth/authenticate_message
---

# Signature

`pub(in crate::smtp) fn summarize_dmarc(result: DmarcDisposition) -> String`

# Called by

- [authenticate_message](../../../../../functions/LPE-CT/src/smtp/auth/authenticate_message.md)