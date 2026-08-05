---
type: Rust Function
title: rop_fast_transfer_source_copy_response
resource: crates/lpe-exchange/src/mapi/sync/responses.rs#L9-L13
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_fast_transfer_source_copy_messages_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_fast_transfer_source_copy_response
---

# Signature

`pub(in crate::mapi) fn rop_fast_transfer_source_copy_response(request: &RopRequest) -> Vec<u8>`

# Called by

- [append_fast_transfer_source_copy_messages_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_fast_transfer_source_copy_messages_response.md)
- [append_fast_transfer_source_copy_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_fast_transfer_source_copy_response.md)