---
type: Rust Function
title: append_reload_cached_information_response
resource: crates/lpe-exchange/src/mapi/dispatch/messages.rs#L673-L697
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/reload_cached_information_reserved
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index
  - functions/crates/lpe-exchange/src/mapi/rop/responses/rop_reload_cached_information_response
  - functions/crates/lpe-exchange/src/mapi/session/input_object
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/message_state/append_message_state_dispatch_response
---

# Signature

`pub(super) fn append_reload_cached_information_response( session: &MapiSession, handle_slots: &[u32], request: &RopRequest, mailboxes: &[JmapMailbox], emails: &[JmapEmail], snapshot: &MapiMailStoreSnapshot, responses: &mut Vec<u8>, )`

# Calls

- [reload_cached_information_reserved](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/reload_cached_information_reserved.md)
- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [response_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index.md)
- [rop_reload_cached_information_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_reload_cached_information_response.md)
- [input_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_object.md)

# Called by

- [append_message_state_dispatch_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/message_state/append_message_state_dispatch_response.md)