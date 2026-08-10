---
type: Rust Function
title: ensure_local_db_schema
resource: LPE-CT/src/storage.rs#L34-L422
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/storage/local_db_pool
  - functions/crates/lpe-activesync/src/tests/query
  - functions/tools/rca_outlook_connectivity_check/execute
  - functions/LPE-CT/src/storage/ensure_pg_trgm_extension
---

# Signature

`pub(crate) async fn ensure_local_db_schema( config: &LocalDbConfig, ) -> Result<Option<&'static PgPool>>`

# Calls

- [local_db_pool](../../../../functions/LPE-CT/src/storage/local_db_pool.md)
- [query](../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [execute](../../../../functions/tools/rca_outlook_connectivity_check/execute.md)
- [ensure_pg_trgm_extension](../../../../functions/LPE-CT/src/storage/ensure_pg_trgm_extension.md)