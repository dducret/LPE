---
type: Rust Method
title: read_ascii_z
resource: crates/lpe-exchange/src/mapi/rop/buffer.rs#L53-L64
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/remaining
  called_by:
  - functions/crates/lpe-exchange/src/mapi/nspi/dn_to_mid/parse_dn_to_mid_names
  - functions/crates/lpe-exchange/src/mapi/properties/values/parse_mapi_property_value
  - functions/crates/lpe-exchange/src/mapi/rop/recipients/parse_wrapped_pending_recipient_row
  - functions/crates/lpe-exchange/src/mapi/rop/recipients/read_recipient_string
  - functions/crates/lpe-exchange/src/mapi/rop/request_reader/read_rop_request_with_logon_id
  - functions/crates/lpe-exchange/src/mapi/transport/diagnostics/summarize_connect_body
---

# Signature

`pub(in crate::mapi) fn read_ascii_z(&mut self) -> Result<String>`

# Calls

- [remaining](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/remaining.md)

# Called by

- [parse_dn_to_mid_names](../../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/dn_to_mid/parse_dn_to_mid_names.md)
- [parse_mapi_property_value](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/parse_mapi_property_value.md)
- [parse_wrapped_pending_recipient_row](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/recipients/parse_wrapped_pending_recipient_row.md)
- [read_recipient_string](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/recipients/read_recipient_string.md)
- [read_rop_request_with_logon_id](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/request_reader/read_rop_request_with_logon_id.md)
- [summarize_connect_body](../../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/diagnostics/summarize_connect_body.md)