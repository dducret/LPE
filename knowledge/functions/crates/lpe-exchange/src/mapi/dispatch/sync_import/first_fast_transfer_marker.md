---
type: Rust Function
title: first_fast_transfer_marker
resource: crates/lpe-exchange/src/mapi/dispatch/sync_import.rs#L7-L12
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_fast_transfer_destination_put_buffer_response
---

# Signature

`pub(super) fn first_fast_transfer_marker(request: &RopRequest) -> Option<u32>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [append_fast_transfer_destination_put_buffer_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_fast_transfer_destination_put_buffer_response.md)