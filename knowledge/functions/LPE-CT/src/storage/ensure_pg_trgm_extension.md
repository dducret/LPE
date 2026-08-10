---
type: Rust Function
title: ensure_pg_trgm_extension
resource: LPE-CT/src/storage.rs#L1200-L1214
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  - functions/tools/rca_outlook_connectivity_check/execute
  called_by:
  - functions/LPE-CT/src/storage/ensure_local_db_schema
---

# Signature

`async fn ensure_pg_trgm_extension(pool: &PgPool) -> bool`

# Calls

- [query](../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [execute](../../../../functions/tools/rca_outlook_connectivity_check/execute.md)

# Called by

- [ensure_local_db_schema](../../../../functions/LPE-CT/src/storage/ensure_local_db_schema.md)