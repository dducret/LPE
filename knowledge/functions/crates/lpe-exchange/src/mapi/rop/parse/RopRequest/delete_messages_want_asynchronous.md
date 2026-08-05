---
type: Rust Method
title: delete_messages_want_asynchronous
resource: crates/lpe-exchange/src/mapi/rop/parse.rs#L742-L750
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_delete_messages_response
---

# Signature

`pub(in crate::mapi) fn delete_messages_want_asynchronous(&self) -> Option<u8>`

# Called by

- [append_delete_messages_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_delete_messages_response.md)