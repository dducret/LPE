---
type: Rust Function
title: parse_private_key_pem
resource: LPE-CT/src/main.rs#L1154-L1171
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

`fn parse_private_key_pem(value: &str) -> Result<PrivateKeyDer<'static>>`

# Calls

- [context](../../../functions/crates/lpe-core/src/sieve/context.md)

# Called by

- [validate_tls_pair_from_pem](../../../functions/LPE-CT/src/validate_tls_pair_from_pem.md)