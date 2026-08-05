---
type: Rust Function
title: append_free_bookmark_response
resource: crates/lpe-exchange/src/mapi/dispatch/tables.rs#L1362-L1370
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/input_object_mut
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/free_bookmark_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_status_or_bookmark_dispatch_response
---

# Signature

`pub(super) fn append_free_bookmark_response( session: &mut MapiSession, handle_slots: &[u32], request: &RopRequest, responses: &mut Vec<u8>, )`

# Calls

- [input_object_mut](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_object_mut.md)
- [free_bookmark_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/free_bookmark_response.md)

# Called by

- [append_status_or_bookmark_dispatch_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_status_or_bookmark_dispatch_response.md)