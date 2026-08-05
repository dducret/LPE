---
type: Rust Module
title: outlook_cache_fidelity_update
resource: crates/lpe-storage/tests/outlook_cache_fidelity_update.rs#L1-L424
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

- [outlook_cache_fidelity_update_runs_twice_and_rolls_back_rejected_shapes](../../../../functions/crates/lpe-storage/tests/outlook_cache_fidelity_update/outlook_cache_fidelity_update_runs_twice_and_rolls_back_rejected_shapes.md)
- [run_update_scenarios](../../../../functions/crates/lpe-storage/tests/outlook_cache_fidelity_update/run_update_scenarios.md)
- [update_sql_for_schema](../../../../functions/crates/lpe-storage/tests/outlook_cache_fidelity_update/update_sql_for_schema.md)
- [recreate_legacy_schema](../../../../functions/crates/lpe-storage/tests/outlook_cache_fidelity_update/recreate_legacy_schema.md)
- [execute_update](../../../../functions/crates/lpe-storage/tests/outlook_cache_fidelity_update/execute_update.md)
- [execute_update_expect_failure](../../../../functions/crates/lpe-storage/tests/outlook_cache_fidelity_update/execute_update_expect_failure.md)
- [assert_successful_update](../../../../functions/crates/lpe-storage/tests/outlook_cache_fidelity_update/assert_successful_update.md)
- [assert_legacy_shape_unchanged](../../../../functions/crates/lpe-storage/tests/outlook_cache_fidelity_update/assert_legacy_shape_unchanged.md)
- [logical_index_is_unique](../../../../functions/crates/lpe-storage/tests/outlook_cache_fidelity_update/logical_index_is_unique.md)
- [relation_exists](../../../../functions/crates/lpe-storage/tests/outlook_cache_fidelity_update/relation_exists.md)

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