---
type: Rust Method
title: create_accessible_event
resource: crates/lpe-storage/src/collaboration.rs#L695-L746
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/collaboration/Storage/resolve_collection_access
  - functions/crates/lpe-storage/src/workspace/Storage/upsert_client_event_in_calendar
  - functions/crates/lpe-core/src/sieve/Parser/next
---

# Signature

`pub async fn create_accessible_event( &self, principal_account_id: Uuid, collection_id: Option<&str>, input: UpsertClientEventInput, ) -> Result<AccessibleEvent>`

# Calls

- [resolve_collection_access](../../../../../../functions/crates/lpe-storage/src/collaboration/Storage/resolve_collection_access.md)
- [upsert_client_event_in_calendar](../../../../../../functions/crates/lpe-storage/src/workspace/Storage/upsert_client_event_in_calendar.md)
- [next](../../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)