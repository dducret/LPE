---
type: Rust Method
title: fetch_accessible_deleted_events
resource: crates/lpe-storage/src/collaboration/deleted_events.rs#L21-L27
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/collaboration/Storage/fetch_accessible_events_internal
---

# Signature

`pub async fn fetch_accessible_deleted_events( &self, principal_account_id: Uuid, ) -> Result<Vec<AccessibleEvent>>`

# Calls

- [fetch_accessible_events_internal](../../../../../../../functions/crates/lpe-storage/src/collaboration/Storage/fetch_accessible_events_internal.md)