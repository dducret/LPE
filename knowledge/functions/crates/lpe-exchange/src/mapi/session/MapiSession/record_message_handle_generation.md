---
type: Rust Method
title: record_message_handle_generation
resource: crates/lpe-exchange/src/mapi/session.rs#L121-L129
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/message_save_generation
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/message_open/append_open_message_response
---

# Signature

`pub(in crate::mapi) fn record_message_handle_generation( &mut self, handle: u32, folder_id: u64, message_id: u64, )`

# Calls

- [message_save_generation](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/message_save_generation.md)

# Called by

- [append_open_message_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/message_open/append_open_message_response.md)