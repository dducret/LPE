---
type: Rust Function
title: serialize_predecessor_change_list
resource: crates/lpe-storage/src/mapi_events.rs#L1478-L1492
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
---

# Signature

`fn serialize_predecessor_change_list(entries: BTreeMap<[u8; 16], Vec<u8>>) -> Result<Vec<u8>>`

# Calls

- [try_from](../../../../../functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from.md)
- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)