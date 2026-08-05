---
type: Rust Function
title: append_transport_folder_response
resource: crates/lpe-exchange/src/mapi/dispatch/submission.rs#L137-L145
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/input_object
  - functions/crates/lpe-exchange/src/mapi/dispatch/submission/transport_folder_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/submission/append_transport_info_dispatch_response
---

# Signature

`pub(super) fn append_transport_folder_response( session: &MapiSession, handle_slots: &[u32], request: &RopRequest, responses: &mut Vec<u8>, )`

# Calls

- [input_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_object.md)
- [transport_folder_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/submission/transport_folder_response.md)

# Called by

- [append_transport_info_dispatch_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/submission/append_transport_info_dispatch_response.md)