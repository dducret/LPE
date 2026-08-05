---
type: Rust Method
title: fetch_activesync_email_states
resource: crates/lpe-storage/src/activesync.rs#L287-L329
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  - functions/crates/lpe-activesync/src/tests/query
---

# Signature

`pub async fn fetch_activesync_email_states( &self, account_id: Uuid, mailbox_id: Uuid, position: u64, limit: u64, ) -> Result<Vec<ActiveSyncItemState>>`

# Calls

- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)