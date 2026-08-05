---
type: Rust Method
title: fast_transfer_upload_data
resource: crates/lpe-exchange/src/mapi/rop/parse.rs#L328-L343
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/staged_fast_transfer_destination_buffer
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_fast_transfer_destination_put_buffer_response
---

# Signature

`pub(in crate::mapi) fn fast_transfer_upload_data(&self) -> &[u8]`

# Calls

- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [staged_fast_transfer_destination_buffer](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/staged_fast_transfer_destination_buffer.md)
- [append_fast_transfer_destination_put_buffer_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_fast_transfer_destination_put_buffer_response.md)