---
type: Rust Function
title: exercise_custom_calendar_grant_path
resource: crates/lpe-storage/tests/runtime_schema_drift.rs#L2859-L3058
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/context
  - functions/crates/lpe-activesync/src/tests/query
  - functions/tools/rca_outlook_connectivity_check/execute
  - functions/crates/lpe-storage/src/collaboration/grants/Storage/fetch_outgoing_collaboration_grants
  - functions/crates/lpe-storage/tests/runtime_schema_drift/runtime_calendar_event_input
  - functions/crates/lpe-storage/tests/runtime_schema_drift/expect_anyhow_failure
  called_by:
  - functions/crates/lpe-storage/tests/runtime_schema_drift/run_runtime_drift_validation
---

# Signature

`async fn exercise_custom_calendar_grant_path( storage: &Storage, pool: &PgPool, fixture: &RuntimeFixture, ) -> Result<()>`

# Calls

- [context](../../../../../functions/crates/lpe-core/src/sieve/context.md)
- [query](../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [execute](../../../../../functions/tools/rca_outlook_connectivity_check/execute.md)
- [fetch_outgoing_collaboration_grants](../../../../../functions/crates/lpe-storage/src/collaboration/grants/Storage/fetch_outgoing_collaboration_grants.md)
- [runtime_calendar_event_input](../../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/runtime_calendar_event_input.md)
- [expect_anyhow_failure](../../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/expect_anyhow_failure.md)

# Called by

- [run_runtime_drift_validation](../../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/run_runtime_drift_validation.md)