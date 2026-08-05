---
type: Rust Method
title: fetch_activesync_sync_state
resource: crates/lpe-storage/src/activesync.rs#L249-L285
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  - functions/crates/lpe-storage/src/activesync/activesync_collection_kind
---

# Signature

`pub async fn fetch_activesync_sync_state( &self, account_id: Uuid, device_id: &str, collection_id: &str, sync_key: &str, ) -> Result<Option<ActiveSyncSyncState>>`

# Calls

- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)
- [activesync_collection_kind](../../../../../../functions/crates/lpe-storage/src/activesync/activesync_collection_kind.md)