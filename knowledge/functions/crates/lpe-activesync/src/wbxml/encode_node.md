---
type: Rust Function
title: encode_node
resource: crates/lpe-activesync/src/wbxml.rs#L72-L105
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from
  - functions/crates/lpe-activesync/src/wbxml/token_for
  - functions/crates/lpe-activesync/src/wbxml/write_multibyte_int
  called_by:
  - functions/crates/lpe-activesync/src/wbxml/encode_wbxml
---

# Signature

`fn encode_node(node: &WbxmlNode, current_page: &mut u8, out: &mut Vec<u8>)`

# Calls

- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [try_from](../../../../../functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from.md)
- [token_for](../../../../../functions/crates/lpe-activesync/src/wbxml/token_for.md)
- [write_multibyte_int](../../../../../functions/crates/lpe-activesync/src/wbxml/write_multibyte_int.md)

# Called by

- [encode_wbxml](../../../../../functions/crates/lpe-activesync/src/wbxml/encode_wbxml.md)