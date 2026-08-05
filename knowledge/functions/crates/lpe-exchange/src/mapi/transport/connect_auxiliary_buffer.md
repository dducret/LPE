---
type: Rust Function
title: connect_auxiliary_buffer
resource: crates/lpe-exchange/src/mapi/transport.rs#L490-L508
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/write_u16
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/transport/connect_response
  - functions/crates/lpe-exchange/src/mapi/transport/tests/connect_body_debug_summary_decodes_fields
---

# Signature

`pub(in crate::mapi) fn connect_auxiliary_buffer() -> Vec<u8>`

# Calls

- [write_u16](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/write_u16.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [connect_response](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/connect_response.md)
- [connect_body_debug_summary_decodes_fields](../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/connect_body_debug_summary_decodes_fields.md)