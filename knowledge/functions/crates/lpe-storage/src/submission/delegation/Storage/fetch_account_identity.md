---
type: Rust Method
title: fetch_account_identity
resource: crates/lpe-storage/src/submission/delegation.rs#L18-L31
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/submission/Storage/account_identity_for_id
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
---

# Signature

`pub async fn fetch_account_identity(&self, account_id: Uuid) -> Result<MailboxAccountAccess>`

# Calls

- [account_identity_for_id](../../../../../../../functions/crates/lpe-storage/src/submission/Storage/account_identity_for_id.md)
- [tenant_id_for_account_id](../../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)