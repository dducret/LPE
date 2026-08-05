---
type: Rust Function
title: summarize_dkim
resource: LPE-CT/src/smtp/auth.rs#L199-L208
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/smtp/auth/dkim_disposition
  called_by:
  - functions/LPE-CT/src/smtp/auth/authenticate_message
---

# Signature

`pub(in crate::smtp) fn summarize_dkim(results: &[DkimResult], aligned: bool) -> String`

# Calls

- [dkim_disposition](../../../../../functions/LPE-CT/src/smtp/auth/dkim_disposition.md)

# Called by

- [authenticate_message](../../../../../functions/LPE-CT/src/smtp/auth/authenticate_message.md)