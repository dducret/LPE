---
type: Rust Function
title: rop_copy_to_stream_response
resource: crates/lpe-exchange/src/mapi/rop/responses.rs#L256-L266
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/write_u64
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_copy_to_stream_response
---

# Signature

`pub(in crate::mapi) fn rop_copy_to_stream_response( request: &RopRequest, read: usize, written: usize, ) -> Vec<u8>`

# Calls

- [write_u64](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/write_u64.md)

# Called by

- [append_copy_to_stream_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_copy_to_stream_response.md)