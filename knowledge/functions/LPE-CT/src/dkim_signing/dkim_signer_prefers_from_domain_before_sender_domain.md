---
type: Rust Function
title: dkim_signer_prefers_from_domain_before_sender_domain
resource: LPE-CT/src/dkim_signing.rs#L252-L303
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/env_test_lock
  - functions/LPE-CT/src/dkim_signing/payload
  - functions/LPE-CT/src/dkim_signing/maybe_sign_outbound_message
---

# Signature

`fn dkim_signer_prefers_from_domain_before_sender_domain()`

# Calls

- [env_test_lock](../../../../functions/LPE-CT/src/env_test_lock.md)
- [payload](../../../../functions/LPE-CT/src/dkim_signing/payload.md)
- [maybe_sign_outbound_message](../../../../functions/LPE-CT/src/dkim_signing/maybe_sign_outbound_message.md)