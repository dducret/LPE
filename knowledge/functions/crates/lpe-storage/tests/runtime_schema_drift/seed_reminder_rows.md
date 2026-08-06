---
type: Rust Function
title: seed_reminder_rows
resource: crates/lpe-storage/tests/runtime_schema_drift.rs#L1579-L1674
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-core/src/sieve/context
  called_by:
  - functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_notes_journal_reminder_path
---

# Signature

`async fn seed_reminder_rows(pool: &PgPool, fixture: &RuntimeFixture) -> Result<()>`

# Calls

- [query](../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [context](../../../../../functions/crates/lpe-core/src/sieve/context.md)

# Called by

- [exercise_notes_journal_reminder_path](../../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_notes_journal_reminder_path.md)