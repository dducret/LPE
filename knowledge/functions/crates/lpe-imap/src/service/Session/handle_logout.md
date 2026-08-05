---
type: Rust Method
title: handle_logout
resource: crates/lpe-imap/src/service.rs#L582-L592
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-imap/src/service/Session/handle_request
---

# Signature

`async fn handle_logout<W>(&self, tag: &str, writer: &mut W) -> Result<()> where W: AsyncWriteExt + Unpin,`

# Called by

- [handle_request](../../../../../../functions/crates/lpe-imap/src/service/Session/handle_request.md)