---
type: Rust Method
title: fetch_accessible_events_in_collection
resource: crates/lpe-storage/src/collaboration.rs#L681-L693
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/collaboration/Storage/fetch_accessible_events_internal
---

# Signature

`pub async fn fetch_accessible_events_in_collection( &self, principal_account_id: Uuid, collection_id: &str, ) -> Result<Vec<AccessibleEvent>>`

# Calls

- [fetch_accessible_events_internal](../../../../../../functions/crates/lpe-storage/src/collaboration/Storage/fetch_accessible_events_internal.md)