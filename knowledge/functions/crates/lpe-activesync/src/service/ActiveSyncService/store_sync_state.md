---
type: Rust Method
title: store_sync_state
resource: crates/lpe-activesync/src/service.rs#L575-L592
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/sync_collection
---

# Signature

`async fn store_sync_state( &self, account_id: Uuid, device_id: &str, collection_id: &str, sync_key: &str, state: &StoredSyncState, ) -> Result<()>`

# Called by

- [sync_collection](../../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/sync_collection.md)