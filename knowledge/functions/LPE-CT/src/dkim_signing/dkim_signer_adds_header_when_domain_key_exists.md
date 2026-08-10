---
type: Rust Function
title: dkim_signer_adds_header_when_domain_key_exists
resource: LPE-CT/src/dkim_signing.rs#L207-L248
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/env_test_lock
  - functions/LPE-CT/src/dkim_signing/maybe_sign_outbound_message
  - functions/LPE-CT/src/dkim_signing/payload
---

# Signature

`fn dkim_signer_adds_header_when_domain_key_exists()`

# Calls

- [env_test_lock](../../../../functions/LPE-CT/src/env_test_lock.md)
- [maybe_sign_outbound_message](../../../../functions/LPE-CT/src/dkim_signing/maybe_sign_outbound_message.md)
- [payload](../../../../functions/LPE-CT/src/dkim_signing/payload.md)