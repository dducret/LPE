---
type: Rust Function
title: rop_options_data_response
resource: crates/lpe-exchange/src/mapi/rop/responses.rs#L295-L302
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/write_u16
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/submission/options_data_response
---

# Signature

`pub(in crate::mapi) fn rop_options_data_response(request: &RopRequest) -> Vec<u8>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [write_u16](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/write_u16.md)

# Called by

- [options_data_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/submission/options_data_response.md)