---
type: Rust Function
title: serialize_logon_row
resource: crates/lpe-exchange/src/mapi/tables/folders.rs#L776-L788
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/folder/logon_property_value
  - functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value
  - functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_property_default
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/serialize_object_property
  - functions/crates/lpe-exchange/src/mapi/rop/debug/outlook_logon_bootstrap_row_shape
  - functions/crates/lpe-exchange/src/mapi/tables/folders/write_logon_property_row
---

# Signature

`pub(in crate::mapi) fn serialize_logon_row( principal: &AccountPrincipal, columns: &[u32], ) -> Vec<u8>`

# Calls

- [logon_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/folder/logon_property_value.md)
- [write_mapi_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value.md)
- [write_property_default](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_property_default.md)

# Called by

- [serialize_object_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/serialize_object_property.md)
- [outlook_logon_bootstrap_row_shape](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/outlook_logon_bootstrap_row_shape.md)
- [write_logon_property_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/write_logon_property_row.md)