---
type: Rust Function
title: normalized_get_properties_request
resource: crates/lpe-exchange/src/mapi/dispatch/property_tags.rs#L3-L19
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/property_tags
  - functions/crates/lpe-exchange/src/mapi/session/named_properties/MapiSession/normalize_named_property_tag
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_get_properties_specific_response
---

# Signature

`pub(super) fn normalized_get_properties_request( session: &MapiSession, request: &RopRequest, ) -> RopRequest`

# Calls

- [property_tags](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/property_tags.md)
- [normalize_named_property_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/named_properties/MapiSession/normalize_named_property_tag.md)

# Called by

- [append_get_properties_specific_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_get_properties_specific_response.md)