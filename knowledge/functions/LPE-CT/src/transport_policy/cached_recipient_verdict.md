---
type: Rust Function
title: cached_recipient_verdict
resource: LPE-CT/src/transport_policy.rs#L470-L476
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/LPE-CT/src/transport_policy/verify_recipient_with_core
---

# Signature

`fn cached_recipient_verdict(key: &str) -> Option<RecipientVerificationVerdict>`

# Calls

- [get](../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [verify_recipient_with_core](../../../../functions/LPE-CT/src/transport_policy/verify_recipient_with_core.md)