---
type: Rust Function
title: json_hex_values
resource: crates/lpe-exchange/src/mapi/properties/values.rs#L144-L150
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/values/mapi_value_from_json
---

# Signature

`fn json_hex_values(value: &serde_json::Value) -> Option<Vec<Vec<u8>>>`

# Called by

- [mapi_value_from_json](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/mapi_value_from_json.md)