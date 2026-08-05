---
type: Rust Function
title: address_binds_publicly
resource: LPE-CT/src/readiness.rs#L213-L247
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/readiness/ip_is_public
  - functions/crates/lpe-core/src/sieve/Parser/next
  called_by:
  - functions/LPE-CT/src/readiness/check_local_data_store_policy
---

# Signature

`pub(crate) fn address_binds_publicly(address: &str) -> bool`

# Calls

- [ip_is_public](../../../../functions/LPE-CT/src/readiness/ip_is_public.md)
- [next](../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)

# Called by

- [check_local_data_store_policy](../../../../functions/LPE-CT/src/readiness/check_local_data_store_policy.md)