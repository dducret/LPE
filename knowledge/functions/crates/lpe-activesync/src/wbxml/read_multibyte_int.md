---
type: Rust Function
title: read_multibyte_int
resource: crates/lpe-activesync/src/wbxml.rs#L226-L238
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-activesync/src/wbxml/decode_wbxml
  - functions/crates/lpe-activesync/src/wbxml/parse_node
---

# Signature

`fn read_multibyte_int(bytes: &[u8], cursor: &mut usize) -> Result<u32>`

# Calls

- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [decode_wbxml](../../../../../functions/crates/lpe-activesync/src/wbxml/decode_wbxml.md)
- [parse_node](../../../../../functions/crates/lpe-activesync/src/wbxml/parse_node.md)