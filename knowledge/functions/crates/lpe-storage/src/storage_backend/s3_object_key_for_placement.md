---
type: Rust Function
title: s3_object_key_for_placement
resource: crates/lpe-storage/src/storage_backend.rs#L152-L162
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/storage_backend/s3_put_object
  - functions/crates/lpe-storage/src/storage_backend/s3_read_object
  - functions/crates/lpe-storage/src/storage_backend/s3_stat_object
  - functions/crates/lpe-storage/src/storage_backend/object_key_is_deterministic_and_omits_tenant_domain_material
---

# Signature

`pub(crate) fn s3_object_key_for_placement( config: &S3CompatiblePoolConfig, placement_id: Uuid, ) -> String`

# Called by

- [s3_put_object](../../../../../functions/crates/lpe-storage/src/storage_backend/s3_put_object.md)
- [s3_read_object](../../../../../functions/crates/lpe-storage/src/storage_backend/s3_read_object.md)
- [s3_stat_object](../../../../../functions/crates/lpe-storage/src/storage_backend/s3_stat_object.md)
- [object_key_is_deterministic_and_omits_tenant_domain_material](../../../../../functions/crates/lpe-storage/src/storage_backend/object_key_is_deterministic_and_omits_tenant_domain_material.md)