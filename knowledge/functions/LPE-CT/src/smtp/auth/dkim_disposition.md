---
type: Rust Function
title: dkim_disposition
resource: LPE-CT/src/smtp/auth.rs#L156-L180
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/smtp/auth/summarize_dkim
  - functions/LPE-CT/src/smtp/auth/authenticate_message
---

# Signature

`pub(in crate::smtp) fn dkim_disposition(results: &[DkimResult]) -> DkimDisposition`

# Called by

- [summarize_dkim](../../../../../functions/LPE-CT/src/smtp/auth/summarize_dkim.md)
- [authenticate_message](../../../../../functions/LPE-CT/src/smtp/auth/authenticate_message.md)