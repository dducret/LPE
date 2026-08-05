---
type: Rust Module
title: schema_051_update
resource: crates/lpe-storage/tests/schema_051_update.rs#L1-L408
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/std-env-str-fromstr
  - external/anyhow-context-result
  - external/sqlx-postgres-pgconnectoptions-pgpooloptions-pgpool-row
  - external/uuid-uuid
  member_of:
  - packages/crates/lpe-storage
---

# Contains

- [schema_051_update_is_transactional_idempotent_and_version_bounded](../../../../functions/crates/lpe-storage/tests/schema_051_update/schema_051_update_is_transactional_idempotent_and_version_bounded.md)
- [run_update_scenarios](../../../../functions/crates/lpe-storage/tests/schema_051_update/run_update_scenarios.md)
- [sql_for_schema](../../../../functions/crates/lpe-storage/tests/schema_051_update/sql_for_schema.md)
- [recreate_source_schema](../../../../functions/crates/lpe-storage/tests/schema_051_update/recreate_source_schema.md)
- [execute_update](../../../../functions/crates/lpe-storage/tests/schema_051_update/execute_update.md)
- [assert_schema_version](../../../../functions/crates/lpe-storage/tests/schema_051_update/assert_schema_version.md)
- [assert_cache_fidelity_shape](../../../../functions/crates/lpe-storage/tests/schema_051_update/assert_cache_fidelity_shape.md)

# Imports

- `std::{env, str::FromStr}`
- `anyhow::{Context, Result}`
- `sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions},
    PgPool, Row,
}`
- `uuid::Uuid`

# Member of

- [lpe-storage](../../../../packages/crates/lpe-storage.md)