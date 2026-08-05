---
type: Rust Method
title: handle_enable
resource: crates/lpe-imap/src/service.rs#L507-L536
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-imap/src/service/Session/handle_request
---

# Signature

`async fn handle_enable<W>(&mut self, tag: &str, arguments: &str, writer: &mut W) -> Result<bool> where W: AsyncWriteExt + Unpin,`

# Called by

- [handle_request](../../../../../../functions/crates/lpe-imap/src/service/Session/handle_request.md)