---
type: Rust Function
title: run_update_scenarios
resource: crates/lpe-storage/tests/outlook_cache_fidelity_update.rs#L40-L71
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/tests/outlook_cache_fidelity_update/recreate_legacy_schema
  - functions/crates/lpe-storage/tests/outlook_cache_fidelity_update/update_sql_for_schema
  - functions/crates/lpe-core/src/sieve/context
  - functions/crates/lpe-storage/tests/outlook_cache_fidelity_update/assert_successful_update
  - functions/crates/lpe-storage/tests/outlook_cache_fidelity_update/execute_update_expect_failure
  - functions/crates/lpe-storage/tests/outlook_cache_fidelity_update/assert_legacy_shape_unchanged
---

# Signature

`async fn run_update_scenarios(pool: &PgPool, schema_name: &str) -> Result<()>`

# Calls

- [recreate_legacy_schema](../../../../../functions/crates/lpe-storage/tests/outlook_cache_fidelity_update/recreate_legacy_schema.md)
- [update_sql_for_schema](../../../../../functions/crates/lpe-storage/tests/outlook_cache_fidelity_update/update_sql_for_schema.md)
- [context](../../../../../functions/crates/lpe-core/src/sieve/context.md)
- [assert_successful_update](../../../../../functions/crates/lpe-storage/tests/outlook_cache_fidelity_update/assert_successful_update.md)
- [execute_update_expect_failure](../../../../../functions/crates/lpe-storage/tests/outlook_cache_fidelity_update/execute_update_expect_failure.md)
- [assert_legacy_shape_unchanged](../../../../../functions/crates/lpe-storage/tests/outlook_cache_fidelity_update/assert_legacy_shape_unchanged.md)