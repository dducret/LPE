---
type: Rust Function
title: debug_object_scope_for_id
resource: crates/lpe-exchange/src/mapi/dispatch/object_ids.rs#L7-L76
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/folders/is_advertised_special_folder
  - functions/crates/lpe-exchange/src/mapi/sync/mapi_item_id_matches
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/event_for_id
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/contact_for_id
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/task_for_id
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/note_for_id
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/journal_entry_for_id
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/conversation_action_message_for_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/object_ids/append_long_term_id_from_id_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/object_ids/append_id_from_long_term_id_response
---

# Signature

`pub(super) fn debug_object_scope_for_id( object_id: Option<u64>, mailboxes: &[JmapMailbox], emails: &[JmapEmail], snapshot: &MapiMailStoreSnapshot, ) -> &'static str`

# Calls

- [is_advertised_special_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/is_advertised_special_folder.md)
- [mapi_item_id_matches](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/mapi_item_id_matches.md)
- [event_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/event_for_id.md)
- [contact_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/contact_for_id.md)
- [task_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/task_for_id.md)
- [note_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/note_for_id.md)
- [journal_entry_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/journal_entry_for_id.md)
- [conversation_action_message_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/conversation_action_message_for_id.md)

# Called by

- [append_long_term_id_from_id_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/object_ids/append_long_term_id_from_id_response.md)
- [append_id_from_long_term_id_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/object_ids/append_id_from_long_term_id_response.md)