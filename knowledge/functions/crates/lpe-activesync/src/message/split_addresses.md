---
type: Rust Function
title: split_addresses
resource: crates/lpe-activesync/src/message.rs#L294-L328
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-activesync/src/message/parse_address_list
---

# Signature

`fn split_addresses(value: &str) -> Vec<String>`

# Calls

- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [parse_address_list](../../../../../functions/crates/lpe-activesync/src/message/parse_address_list.md)