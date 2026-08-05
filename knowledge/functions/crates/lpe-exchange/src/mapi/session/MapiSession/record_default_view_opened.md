---
type: Rust Method
title: record_default_view_opened
resource: crates/lpe-exchange/src/mapi/session.rs#L726-L761
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/message_open/append_open_message_response
---

# Signature

`pub(in crate::mapi) fn record_default_view_opened( &mut self, request_id: &str, view_folder_id: u64, view_message_id: u64, ) -> bool`

# Called by

- [append_open_message_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/message_open/append_open_message_response.md)