---
type: Rust Method
title: mailbox_access_for_from_address
resource: crates/lpe-activesync/src/service.rs#L123-L143
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/mailbox_accesses
  called_by:
  - functions/crates/lpe-activesync/src/service/submission/ActiveSyncService/handle_send_mail
  - functions/crates/lpe-activesync/src/service/submission/ActiveSyncService/handle_smart_compose
---

# Signature

`async fn mailbox_access_for_from_address( &self, principal: &AuthenticatedPrincipal, from_address: Option<&str>, ) -> Result<lpe_storage::MailboxAccountAccess>`

# Calls

- [mailbox_accesses](../../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/mailbox_accesses.md)

# Called by

- [handle_send_mail](../../../../../../functions/crates/lpe-activesync/src/service/submission/ActiveSyncService/handle_send_mail.md)
- [handle_smart_compose](../../../../../../functions/crates/lpe-activesync/src/service/submission/ActiveSyncService/handle_smart_compose.md)