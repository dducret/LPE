---
type: Rust Function
title: runtime_calendar_event_input
resource: crates/lpe-storage/tests/runtime_schema_drift.rs#L2979-L3006
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_custom_calendar_grant_path
---

# Signature

`fn runtime_calendar_event_input( account_id: Uuid, id: Option<Uuid>, title: &str, ) -> UpsertClientEventInput`

# Called by

- [exercise_custom_calendar_grant_path](../../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_custom_calendar_grant_path.md)