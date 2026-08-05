---
type: Rust Function
title: serialize_common_view_named_view_row_with_mailbox_guid
resource: crates/lpe-exchange/src/mapi/tables/associated_contents.rs#L77-L90
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/common_view_named_view_property_value
  - functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value
  - functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_property_default
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/serialize_debug_associated_row
  - functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_specific_response_with_custom
  - functions/crates/lpe-exchange/src/mapi/rop/serialize_object_property
---

# Signature

`pub(in crate::mapi) fn serialize_common_view_named_view_row_with_mailbox_guid( message: &MapiCommonViewNamedViewMessage, mailbox_guid: Uuid, columns: &[u32], ) -> Vec<u8>`

# Calls

- [common_view_named_view_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/common_view_named_view_property_value.md)
- [write_mapi_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value.md)
- [write_property_default](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_property_default.md)

# Called by

- [serialize_debug_associated_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/serialize_debug_associated_row.md)
- [rop_get_properties_specific_response_with_custom](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_specific_response_with_custom.md)
- [serialize_object_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/serialize_object_property.md)