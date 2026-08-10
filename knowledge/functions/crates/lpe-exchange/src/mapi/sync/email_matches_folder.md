---
type: Rust Function
title: email_matches_folder
resource: crates/lpe-exchange/src/mapi/sync.rs#L1445-L1465
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/folders/role_for_folder_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/hard_delete_folder_contents
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/hard_delete_mailbox_tree_contents
  - functions/crates/lpe-exchange/src/mapi/dispatch/messages/fallback_open_message_folder_id
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message_move/completed_message_move_replay_identity
  - functions/crates/lpe-exchange/src/mapi/sync/message_for_id
  - functions/crates/lpe-exchange/src/mapi/sync/scope/emails_for_folder
---

# Signature

`pub(in crate::mapi) fn email_matches_folder( email: &JmapEmail, folder_id: u64, mailboxes: &[JmapMailbox], ) -> bool`

# Calls

- [role_for_folder_id](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/role_for_folder_id.md)

# Called by

- [hard_delete_folder_contents](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/hard_delete_folder_contents.md)
- [hard_delete_mailbox_tree_contents](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/hard_delete_mailbox_tree_contents.md)
- [fallback_open_message_folder_id](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/messages/fallback_open_message_folder_id.md)
- [completed_message_move_replay_identity](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message_move/completed_message_move_replay_identity.md)
- [message_for_id](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/message_for_id.md)
- [emails_for_folder](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/scope/emails_for_folder.md)