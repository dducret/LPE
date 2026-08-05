---
type: Rust Function
title: parse_modify_rows
resource: crates/lpe-exchange/src/mapi/rop/property_rows.rs#L39-L53
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_u8
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_u16
  - functions/crates/lpe-exchange/src/mapi/rop/parse/parse_tagged_property
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/property_rows/RopRequest/modify_rules_rows
  - functions/crates/lpe-exchange/src/mapi/rop/property_rows/RopRequest/modify_permissions_rows
---

# Signature

`fn parse_modify_rows(payload: &[u8], count: usize) -> Result<Vec<ModifyRulesRow>>`

# Calls

- [read_u8](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_u8.md)
- [read_u16](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_u16.md)
- [parse_tagged_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/parse_tagged_property.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [modify_rules_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/property_rows/RopRequest/modify_rules_rows.md)
- [modify_permissions_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/property_rows/RopRequest/modify_permissions_rows.md)