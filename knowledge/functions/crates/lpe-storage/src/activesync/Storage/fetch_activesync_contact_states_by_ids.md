---
type: Rust Method
title: fetch_activesync_contact_states_by_ids
resource: crates/lpe-storage/src/activesync.rs#L405-L440
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  - functions/crates/lpe-activesync/src/tests/query
---

# Signature

`pub async fn fetch_activesync_contact_states_by_ids( &self, account_id: Uuid, ids: &[Uuid], ) -> Result<Vec<ActiveSyncItemState>>`

# Calls

- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)