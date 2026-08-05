---
type: Rust Function
title: rop_get_valid_attachments_response
resource: crates/lpe-exchange/src/mapi/rop/attachments.rs#L6-L46
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/event_for_id
  - functions/crates/lpe-exchange/src/mapi/rop/attachments/rop_get_valid_attachment_numbers_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_get_valid_attachments_response
  - functions/crates/lpe-exchange/src/mapi/rop/tests/calendar_event_get_valid_attachments_rejects_missing_event_handle
---

# Signature

`pub(in crate::mapi) fn rop_get_valid_attachments_response( request: &RopRequest, object: Option<&MapiObject>, snapshot: &MapiMailStoreSnapshot, pending_attachment_deletions: &HashSet<(u64, u64, u32)>, ) -> Vec<u8>`

# Calls

- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [response_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index.md)
- [event_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/event_for_id.md)
- [rop_get_valid_attachment_numbers_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/attachments/rop_get_valid_attachment_numbers_response.md)

# Called by

- [append_get_valid_attachments_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_get_valid_attachments_response.md)
- [calendar_event_get_valid_attachments_rejects_missing_event_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/calendar_event_get_valid_attachments_rejects_missing_event_handle.md)