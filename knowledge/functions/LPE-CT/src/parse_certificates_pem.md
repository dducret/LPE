---
type: Rust Function
title: parse_certificates_pem
resource: LPE-CT/src/main.rs#L1143-L1152
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/context
  called_by:
  - functions/LPE-CT/src/validate_tls_pair_from_pem
---

# Signature

`fn parse_certificates_pem(value: &str) -> Result<Vec<CertificateDer<'static>>>`

# Calls

- [context](../../../functions/crates/lpe-core/src/sieve/context.md)

# Called by

- [validate_tls_pair_from_pem](../../../functions/LPE-CT/src/validate_tls_pair_from_pem.md)