---
type: Rust Function
title: parse_tagged_property_value
resource: crates/lpe-exchange/src/mapi/rop/parse.rs#L1448-L1450
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/parse/parse_tagged_property
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/restrictions/parse_mapi_restriction_from
---

# Signature

`pub(in crate::mapi) fn parse_tagged_property_value(cursor: &mut Cursor<'_>) -> Result<MapiValue>`

# Calls

- [parse_tagged_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/parse_tagged_property.md)

# Called by

- [parse_mapi_restriction_from](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/restrictions/parse_mapi_restriction_from.md)