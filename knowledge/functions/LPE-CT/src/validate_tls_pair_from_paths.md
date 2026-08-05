---
type: Rust Function
title: validate_tls_pair_from_paths
resource: LPE-CT/src/main.rs#L1110-L1116
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/validate_tls_pair_from_pem
  called_by:
  - functions/LPE-CT/src/http_routes/select_public_tls_profile
---

# Signature

`fn validate_tls_pair_from_paths(cert_path: &str, key_path: &str) -> Result<()>`

# Calls

- [validate_tls_pair_from_pem](../../../functions/LPE-CT/src/validate_tls_pair_from_pem.md)

# Called by

- [select_public_tls_profile](../../../functions/LPE-CT/src/http_routes/select_public_tls_profile.md)