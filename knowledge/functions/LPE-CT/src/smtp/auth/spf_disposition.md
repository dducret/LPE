---
type: Rust Function
title: spf_disposition
resource: LPE-CT/src/smtp/auth.rs#L144-L154
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/smtp/auth/authenticate_message
---

# Signature

`pub(in crate::smtp) fn spf_disposition(result: &SpfResult) -> SpfDisposition`

# Called by

- [authenticate_message](../../../../../functions/LPE-CT/src/smtp/auth/authenticate_message.md)