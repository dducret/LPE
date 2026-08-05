---
type: Rust Function
title: parse_mapi_restriction_from
resource: crates/lpe-exchange/src/mapi/rop/restrictions.rs#L16-L135
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_u8
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_u16
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/rop/parse/parse_tagged_property_value
  - functions/crates/lpe-exchange/src/mapi/properties/values/MapiValue/into_text
  - functions/crates/lpe-exchange/src/mapi/rop/parse/parse_tagged_property
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/restrictions/parse_mapi_restriction
---

# Signature

`pub(in crate::mapi) fn parse_mapi_restriction_from( cursor: &mut Cursor<'_>, ) -> Result<MapiRestriction>`

# Calls

- [read_u8](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_u8.md)
- [read_u16](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_u16.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [parse_tagged_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/parse_tagged_property_value.md)
- [into_text](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/MapiValue/into_text.md)
- [parse_tagged_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/parse_tagged_property.md)

# Called by

- [parse_mapi_restriction](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/restrictions/parse_mapi_restriction.md)