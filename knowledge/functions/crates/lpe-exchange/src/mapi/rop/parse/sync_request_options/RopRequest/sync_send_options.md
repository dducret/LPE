---
type: Rust Method
title: sync_send_options
resource: crates/lpe-exchange/src/mapi/rop/parse/sync_request_options.rs#L20-L22
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_configure/append_synchronization_configure_response
---

# Signature

`pub(in crate::mapi) fn sync_send_options(&self) -> u8`

# Calls

- [get](../../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [append_synchronization_configure_response](../../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_configure/append_synchronization_configure_response.md)