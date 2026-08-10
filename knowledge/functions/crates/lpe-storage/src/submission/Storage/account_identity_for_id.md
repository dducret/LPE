---
type: Rust Method
title: account_identity_for_id
resource: crates/lpe-storage/src/submission.rs#L1247-L1269
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  called_by:
  - functions/crates/lpe-storage/src/collaboration/Storage/fetch_free_busy_blocks
  - functions/crates/lpe-storage/src/collaboration/Storage/fetch_accessible_collections
  - functions/crates/lpe-storage/src/submission/delegation/Storage/fetch_account_identity
  - functions/crates/lpe-storage/src/submission/delegation/Storage/fetch_accessible_mailbox_accounts
  - functions/crates/lpe-storage/src/submission/delegation/Storage/fetch_sender_identities
---

# Signature

`pub(crate) async fn account_identity_for_id( &self, account_id: Uuid, ) -> Result<AccountIdentity>`

# Calls

- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)

# Called by

- [fetch_free_busy_blocks](../../../../../../functions/crates/lpe-storage/src/collaboration/Storage/fetch_free_busy_blocks.md)
- [fetch_accessible_collections](../../../../../../functions/crates/lpe-storage/src/collaboration/Storage/fetch_accessible_collections.md)
- [fetch_account_identity](../../../../../../functions/crates/lpe-storage/src/submission/delegation/Storage/fetch_account_identity.md)
- [fetch_accessible_mailbox_accounts](../../../../../../functions/crates/lpe-storage/src/submission/delegation/Storage/fetch_accessible_mailbox_accounts.md)
- [fetch_sender_identities](../../../../../../functions/crates/lpe-storage/src/submission/delegation/Storage/fetch_sender_identities.md)