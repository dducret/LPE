---
type: Rust Function
title: address_domain
resource: LPE-CT/src/dkim_signing.rs#L163-L170
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/dkim_signing/signing_domain_candidates
---

# Signature

`fn address_domain(value: &str) -> Option<String>`

# Called by

- [signing_domain_candidates](../../../../functions/LPE-CT/src/dkim_signing/signing_domain_candidates.md)