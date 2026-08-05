---
type: Rust Function
title: read_inline_string
resource: crates/lpe-activesync/src/wbxml.rs#L251-L259
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-activesync/src/wbxml/parse_node
---

# Signature

`fn read_inline_string(bytes: &[u8], cursor: &mut usize) -> Result<String>`

# Called by

- [parse_node](../../../../../functions/crates/lpe-activesync/src/wbxml/parse_node.md)