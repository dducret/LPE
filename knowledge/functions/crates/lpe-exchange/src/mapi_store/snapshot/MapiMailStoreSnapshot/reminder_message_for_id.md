---
type: Rust Method
title: reminder_message_for_id
resource: crates/lpe-exchange/src/mapi_store/snapshot.rs#L1161-L1165
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/reminder_messages
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/search_folder_message_for_id
---

# Signature

`pub(crate) fn reminder_message_for_id(&self, message_id: u64) -> Option<&MapiMessage>`

# Calls

- [reminder_messages](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/reminder_messages.md)

# Called by

- [search_folder_message_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/search_folder_message_for_id.md)