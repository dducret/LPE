---
type: Rust Method
title: requested_mailbox_folder_ids
resource: crates/lpe-exchange/src/service.rs#L358-L384
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/sync_state/requested_sync_state
  - functions/crates/lpe-exchange/src/service/ews/sync_state/mailbox_sync_state_folder_id
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/service/ews/request_ids/requested_mailbox_role
---

# Signature

`async fn requested_mailbox_folder_ids( &self, principal: &AccountPrincipal, request: &str, ) -> Result<Vec<Uuid>>`

# Calls

- [requested_sync_state](../../../../../../functions/crates/lpe-exchange/src/service/ews/sync_state/requested_sync_state.md)
- [mailbox_sync_state_folder_id](../../../../../../functions/crates/lpe-exchange/src/service/ews/sync_state/mailbox_sync_state_folder_id.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [requested_mailbox_role](../../../../../../functions/crates/lpe-exchange/src/service/ews/request_ids/requested_mailbox_role.md)