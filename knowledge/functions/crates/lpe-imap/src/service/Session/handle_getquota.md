---
type: Rust Method
title: handle_getquota
resource: crates/lpe-imap/src/service.rs#L570-L580
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-imap/src/service/Session/handle_request
---

# Signature

`async fn handle_getquota<W>(&self, tag: &str, _arguments: &str, writer: &mut W) -> Result<bool> where W: AsyncWriteExt + Unpin,`

# Called by

- [handle_request](../../../../../../functions/crates/lpe-imap/src/service/Session/handle_request.md)