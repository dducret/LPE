---
type: Rust Method
title: poll_write
resource: LPE-CT/src/smtp/tests.rs#L1402-L1409
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
---

# Signature

`fn poll_write( mut self: Pin<&mut Self>, _cx: &mut TaskContext<'_>, data: &[u8], ) -> Poll<std::io::Result<usize>>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)