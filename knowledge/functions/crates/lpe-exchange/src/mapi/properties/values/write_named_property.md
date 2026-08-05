---
type: Rust Function
title: write_named_property
resource: crates/lpe-exchange/src/mapi/properties/values.rs#L444-L464
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/named_properties/rop_get_names_from_property_ids_response
  - functions/crates/lpe-exchange/src/mapi/rop/named_properties/rop_query_named_properties_response
---

# Signature

`pub(in crate::mapi) fn write_named_property(row: &mut Vec<u8>, property: &MapiNamedProperty)`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [rop_get_names_from_property_ids_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/named_properties/rop_get_names_from_property_ids_response.md)
- [rop_query_named_properties_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/named_properties/rop_query_named_properties_response.md)