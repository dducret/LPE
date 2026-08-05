---
type: Rust Method
title: fetch_outgoing_mailbox_delegation_grants
resource: crates/lpe-storage/src/submission/delegation.rs#L553-L585
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
---

# Signature

`pub async fn fetch_outgoing_mailbox_delegation_grants( &self, owner_account_id: Uuid, ) -> Result<Vec<MailboxDelegationGrant>>`

# Calls

- [tenant_id_for_account_id](../../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)