---
type: Rust Function
title: round_trip
resource: crates/lpe-exchange/src/mapi/properties/tests.rs#L497-L501
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value
  - functions/crates/lpe-exchange/src/mapi/properties/values/parse_mapi_property_value
---

# Signature

`fn round_trip(property_tag: u32, value: &MapiValue) -> MapiValue`

# Calls

- [write_mapi_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value.md)
- [parse_mapi_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/parse_mapi_property_value.md)