---
type: Rust Method
title: empty_mailbox_folder
resource: crates/lpe-exchange/src/service.rs#L843-L906
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/folders/mailbox_by_id
  - functions/crates/lpe-exchange/src/service/ews/folders/ensure_custom_mailbox
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/service/ExchangeService/empty_folder
---

# Signature

`async fn empty_mailbox_folder( &self, principal: &AccountPrincipal, folder_id: Uuid, delete_subfolders: bool, ) -> Result<()>`

# Calls

- [mailbox_by_id](../../../../../../functions/crates/lpe-exchange/src/service/ews/folders/mailbox_by_id.md)
- [ensure_custom_mailbox](../../../../../../functions/crates/lpe-exchange/src/service/ews/folders/ensure_custom_mailbox.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [empty_folder](../../../../../../functions/crates/lpe-exchange/src/service/ExchangeService/empty_folder.md)