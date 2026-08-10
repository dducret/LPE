---
type: Rust Function
title: property_tag_matches
resource: crates/lpe-exchange/src/mapi_mailstore.rs#L287-L295
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/FastTransferDirectPropertyFilter/includes
  - functions/crates/lpe-exchange/src/mapi_mailstore/property_tag_requested
---

# Signature

`pub(super) fn property_tag_matches(requested_property_tag: u32, property_tag: u32) -> bool`

# Calls

- [canonical_property_storage_tag](../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)

# Called by

- [includes](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/FastTransferDirectPropertyFilter/includes.md)
- [property_tag_requested](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/property_tag_requested.md)