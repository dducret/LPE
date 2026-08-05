---
type: Rust Function
title: recipient_verdict_from_record
resource: LPE-CT/src/transport_policy.rs#L507-L521
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/transport_policy/verify_recipient_with_core
---

# Signature

`fn recipient_verdict_from_record( verdict: &str, detail: Option<String>, ) -> RecipientVerificationVerdict`

# Called by

- [verify_recipient_with_core](../../../../functions/LPE-CT/src/transport_policy/verify_recipient_with_core.md)