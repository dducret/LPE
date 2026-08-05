---
type: Rust Method
title: default_mailbox_delegation_mailbox_id
resource: crates/lpe-storage/src/submission/delegation.rs#L326-L336
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/submission/delegation/Storage/upsert_mailbox_delegation_grant
---

# Signature

`pub(super) async fn default_mailbox_delegation_mailbox_id( &self, owner_account_id: Uuid, ) -> Result<Uuid>`

# Called by

- [upsert_mailbox_delegation_grant](../../../../../../../functions/crates/lpe-storage/src/submission/delegation/Storage/upsert_mailbox_delegation_grant.md)