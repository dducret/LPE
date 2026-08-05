---
type: Rust Function
title: serialize_pending_navigation_shortcut_row
resource: crates/lpe-exchange/src/mapi/tables/pending.rs#L3-L10
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/pending/navigation_shortcut_from_mapi_properties
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/serialize_navigation_shortcut_row
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_specific_response_with_custom
  - functions/crates/lpe-exchange/src/mapi/rop/serialize_object_property
---

# Signature

`pub(in crate::mapi) fn serialize_pending_navigation_shortcut_row( properties: &HashMap<u32, MapiValue>, principal: &AccountPrincipal, columns: &[u32], ) -> Vec<u8>`

# Calls

- [navigation_shortcut_from_mapi_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/pending/navigation_shortcut_from_mapi_properties.md)
- [serialize_navigation_shortcut_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/serialize_navigation_shortcut_row.md)

# Called by

- [rop_get_properties_specific_response_with_custom](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_specific_response_with_custom.md)
- [serialize_object_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/serialize_object_property.md)