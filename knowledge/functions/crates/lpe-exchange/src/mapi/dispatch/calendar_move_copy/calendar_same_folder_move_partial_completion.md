---
type: Rust Function
title: calendar_same_folder_move_partial_completion
resource: crates/lpe-exchange/src/mapi/dispatch/calendar_move_copy.rs#L3-L33
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/move_copy_want_copy
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/collaboration_folder_for_id
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/move_copy_message_ids
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/event_for_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/message_move_copy/append_move_copy_messages_response
---

# Signature

`pub(super) fn calendar_same_folder_move_partial_completion( request: &RopRequest, source_folder_id: u64, target_folder_id: u64, snapshot: &MapiMailStoreSnapshot, ) -> Option<bool>`

# Calls

- [move_copy_want_copy](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/move_copy_want_copy.md)
- [collaboration_folder_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/collaboration_folder_for_id.md)
- [move_copy_message_ids](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/move_copy_message_ids.md)
- [event_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/event_for_id.md)

# Called by

- [append_move_copy_messages_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/message_move_copy/append_move_copy_messages_response.md)