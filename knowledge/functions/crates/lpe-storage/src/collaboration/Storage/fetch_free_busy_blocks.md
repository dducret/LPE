---
type: Rust Method
title: fetch_free_busy_blocks
resource: crates/lpe-storage/src/collaboration.rs#L117-L175
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  - functions/crates/lpe-storage/src/submission/Storage/account_identity_for_id
  - functions/crates/lpe-storage/src/collaboration/types/merge_free_busy_rows
  called_by:
  - functions/crates/lpe-admin-api/src/delegation/get_free_busy
  - functions/crates/lpe-storage/src/collaboration/Storage/compute_delegate_freebusy_messages
---

# Signature

`pub async fn fetch_free_busy_blocks( &self, principal_account_id: Uuid, owner_account_id: Uuid, starts_before: &str, ends_after: &str, ) -> Result<Vec<FreeBusyBlock>>`

# Calls

- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)
- [account_identity_for_id](../../../../../../functions/crates/lpe-storage/src/submission/Storage/account_identity_for_id.md)
- [merge_free_busy_rows](../../../../../../functions/crates/lpe-storage/src/collaboration/types/merge_free_busy_rows.md)

# Called by

- [get_free_busy](../../../../../../functions/crates/lpe-admin-api/src/delegation/get_free_busy.md)
- [compute_delegate_freebusy_messages](../../../../../../functions/crates/lpe-storage/src/collaboration/Storage/compute_delegate_freebusy_messages.md)