---
type: Rust Method
title: poll_read
resource: LPE-CT/src/smtp/tls.rs#L36-L51
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/position
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/remaining
---

# Signature

`fn poll_read( mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>, buf: &mut ReadBuf<'_>, ) -> Poll<std::io::Result<()>>`

# Calls

- [position](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/position.md)
- [remaining](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/remaining.md)