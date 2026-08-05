---
type: Rust Function
title: serialize_pending_event_row
resource: crates/lpe-exchange/src/mapi/tables/pending.rs#L523-L577
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/event_input_from_mapi
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/default_event_for_mapping
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/default_event_input
  - functions/crates/lpe-exchange/src/mapi/properties/default_mapping_rights
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value
  - functions/crates/lpe-exchange/src/mapi/tables/collaboration_items/serialize_event_row
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_specific_response_with_custom
  - functions/crates/lpe-exchange/src/mapi/rop/serialize_object_property
---

# Signature

`pub(in crate::mapi) fn serialize_pending_event_row( principal: &AccountPrincipal, properties: &HashMap<u32, MapiValue>, columns: &[u32], ) -> Vec<u8>`

# Calls

- [event_input_from_mapi](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/event_input_from_mapi.md)
- [default_event_for_mapping](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/default_event_for_mapping.md)
- [default_event_input](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/default_event_input.md)
- [default_mapping_rights](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/default_mapping_rights.md)
- [canonical_property_storage_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [write_mapi_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value.md)
- [serialize_event_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/collaboration_items/serialize_event_row.md)

# Called by

- [rop_get_properties_specific_response_with_custom](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_specific_response_with_custom.md)
- [serialize_object_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/serialize_object_property.md)