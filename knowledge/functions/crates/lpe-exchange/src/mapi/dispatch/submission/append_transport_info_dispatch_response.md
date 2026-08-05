---
type: Rust Function
title: append_transport_info_dispatch_response
resource: crates/lpe-exchange/src/mapi/dispatch/submission.rs#L157-L172
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/submission/append_transport_folder_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/submission/append_options_data_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/submission/append_submission_dispatch_response
---

# Signature

`pub(super) fn append_transport_info_dispatch_response( session: &MapiSession, handle_slots: &[u32], request: &RopRequest, responses: &mut Vec<u8>, )`

# Calls

- [append_transport_folder_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/submission/append_transport_folder_response.md)
- [append_options_data_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/submission/append_options_data_response.md)

# Called by

- [append_submission_dispatch_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/submission/append_submission_dispatch_response.md)