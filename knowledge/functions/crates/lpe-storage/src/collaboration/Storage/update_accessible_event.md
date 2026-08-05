---
type: Rust Method
title: update_accessible_event
resource: crates/lpe-storage/src/collaboration.rs#L748-L801
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/Parser/next
  - functions/crates/lpe-storage/src/workspace/Storage/upsert_client_event_in_calendar
---

# Signature

`pub async fn update_accessible_event( &self, principal_account_id: Uuid, event_id: Uuid, input: UpsertClientEventInput, ) -> Result<AccessibleEvent>`

# Calls

- [next](../../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)
- [upsert_client_event_in_calendar](../../../../../../functions/crates/lpe-storage/src/workspace/Storage/upsert_client_event_in_calendar.md)