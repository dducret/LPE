---
type: Rust Function
title: mailbox_is_trash_or_descendant
resource: crates/lpe-exchange/src/mapi/dispatch/folders.rs#L1458-L1474
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_delete_messages_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_deletes/append_synchronization_import_deletes_response
---

# Signature

`pub(super) fn mailbox_is_trash_or_descendant(mailbox_id: Uuid, mailboxes: &[JmapMailbox]) -> bool`

# Called by

- [append_delete_messages_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_delete_messages_response.md)
- [append_synchronization_import_deletes_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_deletes/append_synchronization_import_deletes_response.md)