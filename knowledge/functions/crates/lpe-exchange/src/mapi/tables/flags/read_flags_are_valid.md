---
type: Rust Function
title: read_flags_are_valid
resource: crates/lpe-exchange/src/mapi/tables/flags.rs#L16-L42
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_set_message_read_flag_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_set_read_flags_response
---

# Signature

`pub(in crate::mapi) fn read_flags_are_valid(read_flags: Option<u8>, allow_default: bool) -> bool`

# Called by

- [append_set_message_read_flag_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_set_message_read_flag_response.md)
- [append_set_read_flags_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_set_read_flags_response.md)