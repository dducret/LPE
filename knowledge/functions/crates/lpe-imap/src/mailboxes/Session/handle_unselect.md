---
type: Rust Method
title: handle_unselect
resource: crates/lpe-imap/src/mailboxes.rs#L546-L557
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-imap/src/service/Session/require_selected
  called_by:
  - functions/crates/lpe-imap/src/service/Session/handle_request
---

# Signature

`pub(crate) async fn handle_unselect<W>(&mut self, tag: &str, writer: &mut W) -> Result<bool> where W: AsyncWriteExt + Unpin,`

# Calls

- [require_selected](../../../../../../functions/crates/lpe-imap/src/service/Session/require_selected.md)

# Called by

- [handle_request](../../../../../../functions/crates/lpe-imap/src/service/Session/handle_request.md)