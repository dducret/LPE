---
type: Rust Function
title: versioned_change_key
resource: crates/lpe-exchange/src/service/ews/ids.rs#L66-L73
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/calendar/calendar_change_key
  - functions/crates/lpe-exchange/src/service/ews/contacts/contact_change_key
  - functions/crates/lpe-exchange/src/service/ews/folders/HierarchySyncFolder/new
  - functions/crates/lpe-exchange/src/service/ews/folders/mailbox_folder_change_key
  - functions/crates/lpe-exchange/src/service/ews/folders/public_folder_change_key
  - functions/crates/lpe-exchange/src/service/ews/folders/collection_folder_change_key
  - functions/crates/lpe-exchange/src/service/ews/mail/message_change_key
  - functions/crates/lpe-exchange/src/service/ews/public_folders/public_folder_item_change_key
  - functions/crates/lpe-exchange/src/service/ews/sharing/ExchangeService/accept_sharing_invitation
  - functions/crates/lpe-exchange/src/service/ews/tasks/task_change_key
---

# Signature

`pub(in crate::service) fn versioned_change_key(kind: &str, id: &str, version: &str) -> String`

# Called by

- [calendar_change_key](../../../../../../../functions/crates/lpe-exchange/src/service/ews/calendar/calendar_change_key.md)
- [contact_change_key](../../../../../../../functions/crates/lpe-exchange/src/service/ews/contacts/contact_change_key.md)
- [new](../../../../../../../functions/crates/lpe-exchange/src/service/ews/folders/HierarchySyncFolder/new.md)
- [mailbox_folder_change_key](../../../../../../../functions/crates/lpe-exchange/src/service/ews/folders/mailbox_folder_change_key.md)
- [public_folder_change_key](../../../../../../../functions/crates/lpe-exchange/src/service/ews/folders/public_folder_change_key.md)
- [collection_folder_change_key](../../../../../../../functions/crates/lpe-exchange/src/service/ews/folders/collection_folder_change_key.md)
- [message_change_key](../../../../../../../functions/crates/lpe-exchange/src/service/ews/mail/message_change_key.md)
- [public_folder_item_change_key](../../../../../../../functions/crates/lpe-exchange/src/service/ews/public_folders/public_folder_item_change_key.md)
- [accept_sharing_invitation](../../../../../../../functions/crates/lpe-exchange/src/service/ews/sharing/ExchangeService/accept_sharing_invitation.md)
- [task_change_key](../../../../../../../functions/crates/lpe-exchange/src/service/ews/tasks/task_change_key.md)