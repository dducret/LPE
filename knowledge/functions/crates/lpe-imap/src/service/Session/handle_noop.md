---
type: Rust Method
title: handle_noop
resource: crates/lpe-imap/src/service.rs#L462-L472
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-imap/src/service/Session/refresh_selected_updates
  called_by:
  - functions/crates/lpe-imap/src/service/Session/handle_request
---

# Signature

`async fn handle_noop<W>(&mut self, tag: &str, writer: &mut W) -> Result<bool> where W: AsyncWriteExt + Unpin,`

# Calls

- [refresh_selected_updates](../../../../../../functions/crates/lpe-imap/src/service/Session/refresh_selected_updates.md)

# Called by

- [handle_request](../../../../../../functions/crates/lpe-imap/src/service/Session/handle_request.md)