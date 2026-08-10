---
type: Rust Function
title: recreate_legacy_schema
resource: crates/lpe-storage/tests/outlook_cache_fidelity_update.rs#L91-L210
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/rca_outlook_connectivity_check/execute
  called_by:
  - functions/crates/lpe-storage/tests/outlook_cache_fidelity_update/run_update_scenarios
---

# Signature

`async fn recreate_legacy_schema( pool: &PgPool, schema_name: &str, schema_version: &str, create_incomplete_range_table: bool, ) -> Result<()>`

# Calls

- [execute](../../../../../functions/tools/rca_outlook_connectivity_check/execute.md)

# Called by

- [run_update_scenarios](../../../../../functions/crates/lpe-storage/tests/outlook_cache_fidelity_update/run_update_scenarios.md)