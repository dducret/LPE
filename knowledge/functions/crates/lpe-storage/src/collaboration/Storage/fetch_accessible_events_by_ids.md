---
type: Rust Method
title: fetch_accessible_events_by_ids
resource: crates/lpe-storage/src/collaboration.rs#L669-L679
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/collaboration/Storage/fetch_accessible_events_internal
---

# Signature

`pub async fn fetch_accessible_events_by_ids( &self, principal_account_id: Uuid, ids: &[Uuid], ) -> Result<Vec<AccessibleEvent>>`

# Calls

- [fetch_accessible_events_internal](../../../../../../functions/crates/lpe-storage/src/collaboration/Storage/fetch_accessible_events_internal.md)