---
type: Rust Function
title: fast_transfer_property_included
resource: crates/lpe-exchange/src/mapi_mailstore.rs#L271-L277
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/FastTransferDirectPropertyFilter/for_rop
  - functions/crates/lpe-exchange/src/mapi_mailstore/FastTransferDirectPropertyFilter/includes
  called_by:
  - functions/crates/lpe-exchange/src/mapi/sync/fast_transfer_message_children
---

# Signature

`pub(crate) fn fast_transfer_property_included( rop_id: u8, property_tags: &[u32], property_tag: u32, ) -> bool`

# Calls

- [for_rop](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/FastTransferDirectPropertyFilter/for_rop.md)
- [includes](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/FastTransferDirectPropertyFilter/includes.md)

# Called by

- [fast_transfer_message_children](../../../../../functions/crates/lpe-exchange/src/mapi/sync/fast_transfer_message_children.md)