---
type: Rust Function
title: recipient_verdict_detail
resource: LPE-CT/src/transport_policy.rs#L499-L505
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/transport_policy/verify_recipient_with_core
---

# Signature

`fn recipient_verdict_detail(verdict: &RecipientVerificationVerdict) -> Option<String>`

# Called by

- [verify_recipient_with_core](../../../../functions/LPE-CT/src/transport_policy/verify_recipient_with_core.md)