---
type: Rust Function
title: recipient_verdict_label
resource: LPE-CT/src/transport_policy.rs#L491-L497
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/transport_policy/verify_recipient_with_core
---

# Signature

`fn recipient_verdict_label(verdict: &RecipientVerificationVerdict) -> &'static str`

# Called by

- [verify_recipient_with_core](../../../../functions/LPE-CT/src/transport_policy/verify_recipient_with_core.md)