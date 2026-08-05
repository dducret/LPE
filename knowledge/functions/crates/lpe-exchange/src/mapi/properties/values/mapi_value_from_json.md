---
type: Rust Function
title: mapi_value_from_json
resource: crates/lpe-exchange/src/mapi/properties/values.rs#L88-L131
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/properties/values/MapiValue/as_bool
  - functions/crates/lpe-exchange/src/mapi/properties/values/MapiValue/as_i64
  - functions/crates/lpe-exchange/src/mapi/properties/values/json_i64_values
  - functions/crates/lpe-exchange/src/mapi/properties/values/json_hex_values
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/values/mapi_properties_from_json
---

# Signature

`fn mapi_value_from_json(value: &serde_json::Value) -> Option<MapiValue>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [as_bool](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/MapiValue/as_bool.md)
- [as_i64](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/MapiValue/as_i64.md)
- [json_i64_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/json_i64_values.md)
- [json_hex_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/json_hex_values.md)

# Called by

- [mapi_properties_from_json](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/mapi_properties_from_json.md)