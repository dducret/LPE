---
type: Rust Function
title: mailbox_parent_folder_id_for_dispatch
resource: crates/lpe-exchange/src/mapi/dispatch/folders.rs#L1444-L1456
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/append_folder_move_copy_response
---

# Signature

`pub(super) fn mailbox_parent_folder_id_for_dispatch( mailbox: &JmapMailbox, mailboxes: &[JmapMailbox], ) -> u64`

# Called by

- [append_folder_move_copy_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/append_folder_move_copy_response.md)