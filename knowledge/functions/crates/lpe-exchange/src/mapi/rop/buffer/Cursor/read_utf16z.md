---
type: Rust Method
title: read_utf16z
resource: crates/lpe-exchange/src/mapi/rop/buffer.rs#L66-L76
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_u16
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/values/parse_mapi_property_value
  - functions/crates/lpe-exchange/src/mapi/rop/recipients/read_recipient_string
  - functions/crates/lpe-exchange/src/mapi/rop/request_reader/read_rop_request_with_logon_id
  - functions/crates/lpe-exchange/src/mapi/transport/diagnostics/summarize_connect_body
---

# Signature

`pub(in crate::mapi) fn read_utf16z(&mut self) -> Result<String>`

# Calls

- [read_u16](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_u16.md)
- [push](../../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [parse_mapi_property_value](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/parse_mapi_property_value.md)
- [read_recipient_string](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/recipients/read_recipient_string.md)
- [read_rop_request_with_logon_id](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/request_reader/read_rop_request_with_logon_id.md)
- [summarize_connect_body](../../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/diagnostics/summarize_connect_body.md)