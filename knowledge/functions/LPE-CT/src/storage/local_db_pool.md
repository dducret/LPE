---
type: Rust Function
title: local_db_pool
resource: LPE-CT/src/storage.rs#L1168-L1198
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-storage/src/core/Storage/connect
  called_by:
  - functions/LPE-CT/src/storage/ensure_local_db_schema
---

# Signature

`async fn local_db_pool(config: &LocalDbConfig) -> Result<Option<&'static PgPool>>`

# Calls

- [get](../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [connect](../../../../functions/crates/lpe-storage/src/core/Storage/connect.md)

# Called by

- [ensure_local_db_schema](../../../../functions/LPE-CT/src/storage/ensure_local_db_schema.md)