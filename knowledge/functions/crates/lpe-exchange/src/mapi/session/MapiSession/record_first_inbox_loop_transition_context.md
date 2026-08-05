---
type: Rust Method
title: record_first_inbox_loop_transition_context
resource: crates/lpe-exchange/src/mapi/session.rs#L597-L606
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/folder_open/append_open_folder_response
---

# Signature

`pub(in crate::mapi) fn record_first_inbox_loop_transition_context(&mut self, context: String)`

# Called by

- [append_open_folder_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folder_open/append_open_folder_response.md)