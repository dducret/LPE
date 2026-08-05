---
type: Rust Function
title: parse_node
resource: crates/lpe-activesync/src/wbxml.rs#L127-L224
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from
  - functions/crates/lpe-activesync/src/wbxml/name_for
  - functions/crates/lpe-activesync/src/wbxml/read_inline_string
  - functions/crates/lpe-activesync/src/wbxml/read_multibyte_int
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-activesync/src/wbxml/decode_wbxml
---

# Signature

`fn parse_node(bytes: &[u8], cursor: &mut usize, current_page: &mut u8) -> Result<WbxmlNode>`

# Calls

- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [try_from](../../../../../functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from.md)
- [name_for](../../../../../functions/crates/lpe-activesync/src/wbxml/name_for.md)
- [read_inline_string](../../../../../functions/crates/lpe-activesync/src/wbxml/read_inline_string.md)
- [read_multibyte_int](../../../../../functions/crates/lpe-activesync/src/wbxml/read_multibyte_int.md)
- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [decode_wbxml](../../../../../functions/crates/lpe-activesync/src/wbxml/decode_wbxml.md)