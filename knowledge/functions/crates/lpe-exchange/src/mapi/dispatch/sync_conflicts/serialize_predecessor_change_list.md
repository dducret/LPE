---
type: Rust Function
title: serialize_predecessor_change_list
resource: crates/lpe-exchange/src/mapi/dispatch/sync_conflicts.rs#L127-L138
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from
---

# Signature

`fn serialize_predecessor_change_list(entries: &BTreeMap<[u8; 16], Vec<u8>>) -> Result<Vec<u8>>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [try_from](../../../../../../../functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from.md)