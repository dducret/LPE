---
type: Rust Function
title: rop_read_stream_response
resource: crates/lpe-exchange/src/mapi/rop/responses.rs#L190-L211
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/input_handle_index
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/read_byte_count
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_read_stream_response
---

# Signature

`pub(in crate::mapi) fn rop_read_stream_response( request: &RopRequest, stream: &mut MapiObject, ) -> Vec<u8>`

# Calls

- [input_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/input_handle_index.md)
- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [read_byte_count](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/read_byte_count.md)

# Called by

- [append_read_stream_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_read_stream_response.md)