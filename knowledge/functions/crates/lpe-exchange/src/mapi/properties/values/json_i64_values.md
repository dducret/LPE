---
type: Rust Function
title: json_i64_values
resource: crates/lpe-exchange/src/mapi/properties/values.rs#L133-L142
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/values/MapiValue/as_i64
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/values/mapi_value_from_json
---

# Signature

`fn json_i64_values<T>(value: &serde_json::Value) -> Option<Vec<T>> where T: TryFrom<i64>,`

# Calls

- [as_i64](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/MapiValue/as_i64.md)

# Called by

- [mapi_value_from_json](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/mapi_value_from_json.md)