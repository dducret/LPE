---
type: Rust Method
title: record_last_inbox_open_folder_context
resource: crates/lpe-exchange/src/mapi/session.rs#L405-L407
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/folder_open/append_open_folder_response
---

# Signature

`pub(in crate::mapi) fn record_last_inbox_open_folder_context(&mut self, context: String)`

# Called by

- [append_open_folder_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folder_open/append_open_folder_response.md)