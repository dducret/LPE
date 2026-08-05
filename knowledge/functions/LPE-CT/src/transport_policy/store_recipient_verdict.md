---
type: Rust Function
title: store_recipient_verdict
resource: LPE-CT/src/transport_policy.rs#L478-L489
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/transport_policy/verify_recipient_with_core
---

# Signature

`fn store_recipient_verdict(key: &str, verdict: RecipientVerificationVerdict, ttl_seconds: u64)`

# Called by

- [verify_recipient_with_core](../../../../functions/LPE-CT/src/transport_policy/verify_recipient_with_core.md)