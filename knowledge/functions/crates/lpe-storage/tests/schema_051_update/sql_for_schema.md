---
type: Rust Function
title: sql_for_schema
resource: crates/lpe-storage/tests/schema_051_update.rs#L224-L240
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/tests/schema_051_update/run_update_scenarios
---

# Signature

`fn sql_for_schema(sql: &str, schema_name: &str) -> Result<String>`

# Called by

- [run_update_scenarios](../../../../../functions/crates/lpe-storage/tests/schema_051_update/run_update_scenarios.md)