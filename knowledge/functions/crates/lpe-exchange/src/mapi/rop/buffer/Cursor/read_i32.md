---
type: Rust Method
title: read_i32
resource: crates/lpe-exchange/src/mapi/rop/buffer.rs#L18-L21
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_bytes
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/read_fast_transfer_property_value
  - functions/crates/lpe-exchange/src/mapi/properties/values/parse_mapi_property_value
  - functions/crates/lpe-exchange/src/mapi/rop/request_reader/read_rop_request_with_logon_id
---

# Signature

`pub(in crate::mapi) fn read_i32(&mut self) -> Result<i32>`

# Calls

- [read_bytes](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_bytes.md)

# Called by

- [read_fast_transfer_property_value](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/read_fast_transfer_property_value.md)
- [parse_mapi_property_value](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/parse_mapi_property_value.md)
- [read_rop_request_with_logon_id](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/request_reader/read_rop_request_with_logon_id.md)