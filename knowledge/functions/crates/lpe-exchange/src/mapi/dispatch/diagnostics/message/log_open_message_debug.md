---
type: Rust Function
title: log_open_message_debug
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/message.rs#L3-L47
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/recipients/message_recipients
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/message_open/append_open_message_response
---

# Signature

`pub(in crate::mapi::dispatch) fn log_open_message_debug( principal: &AccountPrincipal, request: &RopRequest, handle: u32, folder_id: u64, message_id: u64, source: &str, email: &JmapEmail, response_len: usize, )`

# Calls

- [message_recipients](../../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/recipients/message_recipients.md)

# Called by

- [append_open_message_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/message_open/append_open_message_response.md)