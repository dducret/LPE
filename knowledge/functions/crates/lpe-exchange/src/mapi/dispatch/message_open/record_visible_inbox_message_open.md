---
type: Rust Function
title: record_visible_inbox_message_open
resource: crates/lpe-exchange/src/mapi/dispatch/message_open.rs#L538-L576
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_outlook_view_failure_trace_event
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/message_open/append_open_message_response
---

# Signature

`fn record_visible_inbox_message_open( session: &mut MapiSession, request_id: &str, request: &RopRequest, handle: u32, folder_id: u64, message_id: u64, source: &str, email: &JmapEmail, response_len: usize, )`

# Calls

- [record_outlook_view_failure_trace_event](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_outlook_view_failure_trace_event.md)

# Called by

- [append_open_message_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/message_open/append_open_message_response.md)