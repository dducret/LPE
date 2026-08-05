---
type: Rust Function
title: mapi_item_id_matches
resource: crates/lpe-exchange/src/mapi/sync.rs#L1395-L1397
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/object_id_matches
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/messages/unique_message_for_id
  - functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_set_read_flags_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_message_status_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/object_ids/debug_object_scope_for_id
  - functions/crates/lpe-exchange/src/mapi/sync/message_for_id
---

# Signature

`pub(in crate::mapi) fn mapi_item_id_matches(canonical_id: &Uuid, object_id: u64) -> bool`

# Calls

- [object_id_matches](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/object_id_matches.md)

# Called by

- [unique_message_for_id](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/messages/unique_message_for_id.md)
- [append_set_read_flags_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_set_read_flags_response.md)
- [append_message_status_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_message_status_response.md)
- [debug_object_scope_for_id](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/object_ids/debug_object_scope_for_id.md)
- [message_for_id](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/message_for_id.md)