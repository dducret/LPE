---
type: Rust Method
title: for_rop
resource: crates/lpe-exchange/src/mapi_mailstore.rs#L248-L256
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/sync/fast_transfer_manifest_for_object
  - functions/crates/lpe-exchange/src/mapi_mailstore/fast_transfer_property_included
---

# Signature

`pub(crate) fn for_rop(rop_id: u8, property_tags: &'a [u32]) -> Self`

# Called by

- [fast_transfer_manifest_for_object](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/fast_transfer_manifest_for_object.md)
- [fast_transfer_property_included](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/fast_transfer_property_included.md)