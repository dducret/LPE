---
type: Rust Function
title: append_options_data_response
resource: crates/lpe-exchange/src/mapi/dispatch/submission.rs#L147-L155
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/input_object
  - functions/crates/lpe-exchange/src/mapi/dispatch/submission/options_data_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/submission/append_transport_info_dispatch_response
---

# Signature

`pub(super) fn append_options_data_response( session: &MapiSession, handle_slots: &[u32], request: &RopRequest, responses: &mut Vec<u8>, )`

# Calls

- [input_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_object.md)
- [options_data_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/submission/options_data_response.md)

# Called by

- [append_transport_info_dispatch_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/submission/append_transport_info_dispatch_response.md)