---
type: Rust Method
title: fast_transfer_uses_server_determined_buffer_size
resource: crates/lpe-exchange/src/mapi/rop/parse.rs#L311-L317
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_get_buffer/fast_transfer_source_get_buffer_transfer_size
---

# Signature

`pub(in crate::mapi) fn fast_transfer_uses_server_determined_buffer_size(&self) -> bool`

# Calls

- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [execute_rops](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops.md)
- [fast_transfer_source_get_buffer_transfer_size](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_get_buffer/fast_transfer_source_get_buffer_transfer_size.md)