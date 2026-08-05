---
type: Rust Method
title: fetch_accessible_mailbox_accounts
resource: crates/lpe-storage/src/submission/delegation.rs#L622-L689
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/submission/Storage/account_identity_for_id
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
---

# Signature

`pub async fn fetch_accessible_mailbox_accounts( &self, principal_account_id: Uuid, ) -> Result<Vec<MailboxAccountAccess>>`

# Calls

- [account_identity_for_id](../../../../../../../functions/crates/lpe-storage/src/submission/Storage/account_identity_for_id.md)
- [tenant_id_for_account_id](../../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)