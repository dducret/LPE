---
type: Rust Function
title: check_local_data_store_policy
resource: LPE-CT/src/readiness.rs#L163-L211
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/readiness/address_binds_publicly
  called_by:
  - functions/LPE-CT/src/http_routes/health_ready
---

# Signature

`pub(crate) fn check_local_data_store_policy( local_data_stores: &LocalDataStoresSettings, ) -> ReadinessCheck`

# Calls

- [address_binds_publicly](../../../../functions/LPE-CT/src/readiness/address_binds_publicly.md)

# Called by

- [health_ready](../../../../functions/LPE-CT/src/http_routes/health_ready.md)