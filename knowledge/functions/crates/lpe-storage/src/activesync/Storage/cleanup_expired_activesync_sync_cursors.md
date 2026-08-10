---
type: Rust Method
title: cleanup_expired_activesync_sync_cursors
resource: crates/lpe-storage/src/activesync.rs#L225-L247
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  - functions/crates/lpe-activesync/src/tests/query
  - functions/tools/rca_outlook_connectivity_check/execute
---

# Signature

`pub async fn cleanup_expired_activesync_sync_cursors( &self, account_id: Uuid, device_id: &str, ) -> Result<()>`

# Calls

- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [execute](../../../../../../functions/tools/rca_outlook_connectivity_check/execute.md)