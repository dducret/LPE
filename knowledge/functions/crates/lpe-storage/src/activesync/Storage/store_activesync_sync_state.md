---
type: Rust Method
title: store_activesync_sync_state
resource: crates/lpe-storage/src/activesync.rs#L58-L102
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  - functions/crates/lpe-storage/src/activesync/activesync_collection_kind
  - functions/crates/lpe-activesync/src/tests/query
---

# Signature

`pub async fn store_activesync_sync_state( &self, account_id: Uuid, device_id: &str, collection_id: &str, sync_key: &str, snapshot_json: &str, ) -> Result<()>`

# Calls

- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)
- [activesync_collection_kind](../../../../../../functions/crates/lpe-storage/src/activesync/activesync_collection_kind.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)