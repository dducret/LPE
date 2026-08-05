---
type: Rust Method
title: todo_search_message_for_id
resource: crates/lpe-exchange/src/mapi_store/snapshot.rs#L1031-L1035
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/todo_search_messages
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/search_folder_message_for_id
---

# Signature

`pub(crate) fn todo_search_message_for_id(&self, message_id: u64) -> Option<&MapiMessage>`

# Calls

- [todo_search_messages](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/todo_search_messages.md)

# Called by

- [search_folder_message_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/search_folder_message_for_id.md)