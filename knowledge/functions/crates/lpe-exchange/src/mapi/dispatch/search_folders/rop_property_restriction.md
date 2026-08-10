---
type: Rust Function
title: rop_property_restriction
resource: crates/lpe-exchange/src/mapi/dispatch/search_folders.rs#L924-L959
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/property_tag_for_search_field
  - functions/crates/lpe-exchange/src/mapi/properties/values/MapiValue/as_bool
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_rfc3339_utc
  - functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from
  - functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/rop_restriction_from_json_clause
---

# Signature

`fn rop_property_restriction( field: &str, relop: u8, value: &Value, use_unicode: bool, ) -> Result<Vec<u8>, u32>`

# Calls

- [property_tag_for_search_field](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/property_tag_for_search_field.md)
- [as_bool](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/MapiValue/as_bool.md)
- [filetime_from_rfc3339_utc](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_rfc3339_utc.md)
- [try_from](../../../../../../../functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from.md)
- [write_mapi_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value.md)

# Called by

- [rop_restriction_from_json_clause](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/rop_restriction_from_json_clause.md)