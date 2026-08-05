---
type: Rust Function
title: parse_tagged_property
resource: crates/lpe-exchange/src/mapi/rop/parse.rs#L1452-L1456
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/parse/parse_property_value_for_tag
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/import_property_values
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/import_hierarchy_values
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/import_delete_source_keys
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/property_values
  - functions/crates/lpe-exchange/src/mapi/rop/parse/parse_tagged_property_value
  - functions/crates/lpe-exchange/src/mapi/rop/property_rows/parse_modify_rows
  - functions/crates/lpe-exchange/src/mapi/rop/request_reader/read_rop_request_with_logon_id
  - functions/crates/lpe-exchange/src/mapi/rop/restrictions/parse_mapi_restriction_from
---

# Signature

`pub(in crate::mapi) fn parse_tagged_property(cursor: &mut Cursor<'_>) -> Result<(u32, MapiValue)>`

# Calls

- [parse_property_value_for_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/parse_property_value_for_tag.md)
- [canonical_property_storage_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)

# Called by

- [import_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/import_property_values.md)
- [import_hierarchy_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/import_hierarchy_values.md)
- [import_delete_source_keys](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/import_delete_source_keys.md)
- [property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/property_values.md)
- [parse_tagged_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/parse_tagged_property_value.md)
- [parse_modify_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/property_rows/parse_modify_rows.md)
- [read_rop_request_with_logon_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/request_reader/read_rop_request_with_logon_id.md)
- [parse_mapi_restriction_from](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/restrictions/parse_mapi_restriction_from.md)