---
type: Rust Function
title: rop_fast_transfer_put_buffer_response
resource: crates/lpe-exchange/src/mapi/rop/responses.rs#L468-L481
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/write_u16
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_fast_transfer_destination_put_buffer_response
---

# Signature

`pub(in crate::mapi) fn rop_fast_transfer_put_buffer_response( request: &RopRequest, used_size: usize, ) -> Vec<u8>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [write_u16](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/write_u16.md)

# Called by

- [append_fast_transfer_destination_put_buffer_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_fast_transfer_destination_put_buffer_response.md)