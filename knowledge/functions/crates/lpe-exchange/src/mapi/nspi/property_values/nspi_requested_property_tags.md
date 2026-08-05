---
type: Rust Function
title: nspi_requested_property_tags
resource: crates/lpe-exchange/src/mapi/nspi/property_values.rs#L164-L184
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_property_tag_is_supported
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_rowset_response
  - functions/crates/lpe-exchange/src/mapi/nspi/nspi_matches_response
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_get_props_property_tags
---

# Signature

`pub(in crate::mapi) fn nspi_requested_property_tags(request: &[u8]) -> Vec<u32>`

# Calls

- [nspi_property_tag_is_supported](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_property_tag_is_supported.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [nspi_rowset_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_rowset_response.md)
- [nspi_matches_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/nspi_matches_response.md)
- [nspi_get_props_property_tags](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/nspi_get_props_property_tags.md)