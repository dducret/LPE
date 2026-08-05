---
type: Rust Method
title: mailbox_access_for_account
resource: crates/lpe-activesync/src/service.rs#L111-L121
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/mailbox_accesses
  called_by:
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/apply_draft_sync_commands
---

# Signature

`async fn mailbox_access_for_account( &self, principal: &AuthenticatedPrincipal, target_account_id: Uuid, ) -> Result<lpe_storage::MailboxAccountAccess>`

# Calls

- [mailbox_accesses](../../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/mailbox_accesses.md)

# Called by

- [apply_draft_sync_commands](../../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/apply_draft_sync_commands.md)