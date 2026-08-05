---
type: Rust Method
title: mailbox_accesses
resource: crates/lpe-activesync/src/service.rs#L102-L109
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/mailbox_access_for_account
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/mailbox_access_for_from_address
  - functions/crates/lpe-activesync/src/service/item_operations/ActiveSyncService/handle_item_operations_fetch
  - functions/crates/lpe-activesync/src/service/submission/ActiveSyncService/resolve_source_message
---

# Signature

`async fn mailbox_accesses( &self, principal: &AuthenticatedPrincipal, ) -> Result<Vec<lpe_storage::MailboxAccountAccess>>`

# Called by

- [mailbox_access_for_account](../../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/mailbox_access_for_account.md)
- [mailbox_access_for_from_address](../../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/mailbox_access_for_from_address.md)
- [handle_item_operations_fetch](../../../../../../functions/crates/lpe-activesync/src/service/item_operations/ActiveSyncService/handle_item_operations_fetch.md)
- [resolve_source_message](../../../../../../functions/crates/lpe-activesync/src/service/submission/ActiveSyncService/resolve_source_message.md)