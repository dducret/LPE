---
type: Rust Function
title: fast_transfer_message_children
resource: crates/lpe-exchange/src/mapi/sync.rs#L1104-L1128
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/fast_transfer_property_included
  called_by:
  - functions/crates/lpe-exchange/src/mapi/sync/fast_transfer_manifest_for_object
---

# Signature

`fn fast_transfer_message_children( rop_id: u8, level: u8, property_tags: &[u32], ) -> mapi_mailstore::FastTransferMessageChildren`

# Calls

- [fast_transfer_property_included](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/fast_transfer_property_included.md)

# Called by

- [fast_transfer_manifest_for_object](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/fast_transfer_manifest_for_object.md)