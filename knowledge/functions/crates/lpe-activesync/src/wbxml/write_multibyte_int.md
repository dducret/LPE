---
type: Rust Function
title: write_multibyte_int
resource: crates/lpe-activesync/src/wbxml.rs#L240-L249
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-activesync/src/wbxml/encode_node
---

# Signature

`fn write_multibyte_int(mut value: u32, out: &mut Vec<u8>)`

# Calls

- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [encode_node](../../../../../functions/crates/lpe-activesync/src/wbxml/encode_node.md)