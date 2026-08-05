---
type: Rust Method
title: require_mailbox_account_access
resource: crates/lpe-storage/src/submission/delegation.rs#L691-L701
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/submission/delegation/Storage/fetch_sender_identities
---

# Signature

`pub async fn require_mailbox_account_access( &self, principal_account_id: Uuid, target_account_id: Uuid, ) -> Result<MailboxAccountAccess>`

# Called by

- [fetch_sender_identities](../../../../../../../functions/crates/lpe-storage/src/submission/delegation/Storage/fetch_sender_identities.md)