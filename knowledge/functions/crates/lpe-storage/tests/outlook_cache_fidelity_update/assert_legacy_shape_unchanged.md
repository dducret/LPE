---
type: Rust Function
title: assert_legacy_shape_unchanged
resource: crates/lpe-storage/tests/outlook_cache_fidelity_update.rs#L330-L387
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/context
  called_by:
  - functions/crates/lpe-storage/tests/outlook_cache_fidelity_update/run_update_scenarios
---

# Signature

`async fn assert_legacy_shape_unchanged( pool: &PgPool, schema_name: &str, incomplete_range_table_existed: bool, ) -> Result<()>`

# Calls

- [context](../../../../../functions/crates/lpe-core/src/sieve/context.md)

# Called by

- [run_update_scenarios](../../../../../functions/crates/lpe-storage/tests/outlook_cache_fidelity_update/run_update_scenarios.md)