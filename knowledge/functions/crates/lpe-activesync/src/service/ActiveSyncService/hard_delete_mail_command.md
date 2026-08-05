---
type: Rust Method
title: hard_delete_mail_command
resource: crates/lpe-activesync/src/service.rs#L1012-L1032
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/apply_mail_sync_commands
---

# Signature

`async fn hard_delete_mail_command( &self, principal: &AuthenticatedPrincipal, collection: &CollectionDefinition, mailbox_id: Uuid, message_id: Uuid, server_id: &str, ) -> Result<()>`

# Called by

- [apply_mail_sync_commands](../../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/apply_mail_sync_commands.md)