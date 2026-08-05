---
type: Rust Function
title: serialize_rop_request
resource: crates/lpe-exchange/src/mapi/rop/serialize.rs#L9-L118
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/typed
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/write_u16
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/tests/microsoft_oxcrops_rop_buffer_request_examples_parse_expected_fields
---

# Signature

`pub(in crate::mapi) fn serialize_rop_request(request: &RopRequest) -> Result<Vec<u8>>`

# Calls

- [typed](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/typed.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [write_u16](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/write_u16.md)

# Called by

- [microsoft_oxcrops_rop_buffer_request_examples_parse_expected_fields](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/microsoft_oxcrops_rop_buffer_request_examples_parse_expected_fields.md)