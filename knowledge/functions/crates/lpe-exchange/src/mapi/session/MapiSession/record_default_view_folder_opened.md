---
type: Rust Method
title: record_default_view_folder_opened
resource: crates/lpe-exchange/src/mapi/session.rs#L273-L288
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_outlook_view_failure_trace_event
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/folder_open/append_open_folder_response
---

# Signature

`pub(in crate::mapi) fn record_default_view_folder_opened( &mut self, handle: u32, folder_id: u64, context: String, )`

# Calls

- [record_outlook_view_failure_trace_event](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_outlook_view_failure_trace_event.md)

# Called by

- [append_open_folder_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folder_open/append_open_folder_response.md)