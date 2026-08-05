---
type: Rust Method
title: upload_state_transfer_size
resource: crates/lpe-exchange/src/mapi/rop/parse.rs#L352-L357
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_upload_state/append_upload_state_stream_begin_response
---

# Signature

`pub(in crate::mapi) fn upload_state_transfer_size(&self) -> Option<u32>`

# Calls

- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [append_upload_state_stream_begin_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_upload_state/append_upload_state_stream_begin_response.md)