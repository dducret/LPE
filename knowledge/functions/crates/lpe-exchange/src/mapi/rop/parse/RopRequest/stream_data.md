---
type: Rust Method
title: stream_data
resource: crates/lpe-exchange/src/mapi/rop/parse.rs#L319-L326
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_upload_state/append_upload_state_stream_continue_response
---

# Signature

`pub(in crate::mapi) fn stream_data(&self) -> &[u8]`

# Calls

- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [append_upload_state_stream_continue_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_upload_state/append_upload_state_stream_continue_response.md)