---
type: Rust Function
title: validate_tls_pair_from_pem
resource: LPE-CT/src/main.rs#L1118-L1126
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/parse_certificates_pem
  - functions/LPE-CT/src/parse_private_key_pem
  - functions/crates/lpe-core/src/sieve/context
  called_by:
  - functions/LPE-CT/src/store_public_tls_profile
  - functions/LPE-CT/src/validate_tls_pair_from_paths
---

# Signature

`fn validate_tls_pair_from_pem(cert_pem: &str, key_pem: &str) -> Result<()>`

# Calls

- [parse_certificates_pem](../../../functions/LPE-CT/src/parse_certificates_pem.md)
- [parse_private_key_pem](../../../functions/LPE-CT/src/parse_private_key_pem.md)
- [context](../../../functions/crates/lpe-core/src/sieve/context.md)

# Called by

- [store_public_tls_profile](../../../functions/LPE-CT/src/store_public_tls_profile.md)
- [validate_tls_pair_from_paths](../../../functions/LPE-CT/src/validate_tls_pair_from_paths.md)