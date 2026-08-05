---
type: Rust Function
title: object_key_is_deterministic_and_omits_tenant_domain_material
resource: crates/lpe-storage/src/storage_backend.rs#L900-L921
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/Parser/expect
  - functions/crates/lpe-storage/src/storage_backend/s3_object_key_for_placement
---

# Signature

`fn object_key_is_deterministic_and_omits_tenant_domain_material()`

# Calls

- [expect](../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)
- [s3_object_key_for_placement](../../../../../functions/crates/lpe-storage/src/storage_backend/s3_object_key_for_placement.md)