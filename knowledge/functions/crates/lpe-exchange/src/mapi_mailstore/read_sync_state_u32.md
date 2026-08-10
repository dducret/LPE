---
type: Rust Function
title: read_sync_state_u32
resource: crates/lpe-exchange/src/mapi_mailstore.rs#L707-L712
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/sync_state_property_value
---

# Signature

`fn read_sync_state_u32(bytes: &[u8], offset: &mut usize) -> Option<u32>`

# Calls

- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [sync_state_property_value](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/sync_state_property_value.md)