---
type: Rust Method
title: fetch_mailbox_delegation_grant
resource: crates/lpe-storage/src/submission/delegation.rs#L338-L373
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  called_by:
  - functions/crates/lpe-storage/src/submission/delegation/Storage/upsert_mailbox_delegation_grant
---

# Signature

`pub async fn fetch_mailbox_delegation_grant( &self, owner_account_id: Uuid, grantee_account_id: Uuid, ) -> Result<Option<MailboxDelegationGrant>>`

# Calls

- [tenant_id_for_account_id](../../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)

# Called by

- [upsert_mailbox_delegation_grant](../../../../../../../functions/crates/lpe-storage/src/submission/delegation/Storage/upsert_mailbox_delegation_grant.md)