---
type: Rust Function
title: unread_from_read_flags
resource: crates/lpe-exchange/src/mapi/tables/flags.rs#L7-L14
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_set_message_read_flag_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_set_read_flags_response
---

# Signature

`pub(in crate::mapi) fn unread_from_read_flags(read_flags: Option<u8>) -> Option<bool>`

# Called by

- [append_set_message_read_flag_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_set_message_read_flag_response.md)
- [append_set_read_flags_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_set_read_flags_response.md)