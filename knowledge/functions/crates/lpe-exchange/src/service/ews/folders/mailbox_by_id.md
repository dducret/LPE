---
type: Rust Function
title: mailbox_by_id
resource: crates/lpe-exchange/src/service/ews/folders.rs#L613-L621
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/ExchangeService/move_folder
  - functions/crates/lpe-exchange/src/service/ExchangeService/update_folder
  - functions/crates/lpe-exchange/src/service/ExchangeService/copy_mailbox_folder_tree
  - functions/crates/lpe-exchange/src/service/ExchangeService/empty_mailbox_folder
---

# Signature

`pub(in crate::service) fn mailbox_by_id( mailboxes: &[JmapMailbox], mailbox_id: Uuid, ) -> Result<&JmapMailbox>`

# Called by

- [move_folder](../../../../../../../functions/crates/lpe-exchange/src/service/ExchangeService/move_folder.md)
- [update_folder](../../../../../../../functions/crates/lpe-exchange/src/service/ExchangeService/update_folder.md)
- [copy_mailbox_folder_tree](../../../../../../../functions/crates/lpe-exchange/src/service/ExchangeService/copy_mailbox_folder_tree.md)
- [empty_mailbox_folder](../../../../../../../functions/crates/lpe-exchange/src/service/ExchangeService/empty_mailbox_folder.md)