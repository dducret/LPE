---
type: Rust Method
title: copy_mailbox_folder_tree
resource: crates/lpe-exchange/src/service.rs#L779-L843
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/folders/ensure_custom_mailbox
  - functions/crates/lpe-exchange/src/service/ews/folders/mailbox_by_id
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/service/ExchangeService/copy_folder
---

# Signature

`async fn copy_mailbox_folder_tree( &self, principal: &AccountPrincipal, source_id: Uuid, target_parent_id: Option<Uuid>, ) -> Result<JmapMailbox>`

# Calls

- [ensure_custom_mailbox](../../../../../../functions/crates/lpe-exchange/src/service/ews/folders/ensure_custom_mailbox.md)
- [mailbox_by_id](../../../../../../functions/crates/lpe-exchange/src/service/ews/folders/mailbox_by_id.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [copy_folder](../../../../../../functions/crates/lpe-exchange/src/service/ExchangeService/copy_folder.md)