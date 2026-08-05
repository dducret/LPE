---
type: Rust Method
title: handle_close
resource: crates/lpe-imap/src/mailboxes.rs#L522-L544
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-imap/src/service/Session/require_selected
  - functions/crates/lpe-imap/src/mailboxes/Session/delete_selected_indices
  called_by:
  - functions/crates/lpe-imap/src/service/Session/handle_request
---

# Signature

`pub(crate) async fn handle_close<W>(&mut self, tag: &str, writer: &mut W) -> Result<bool> where W: AsyncWriteExt + Unpin,`

# Calls

- [require_selected](../../../../../../functions/crates/lpe-imap/src/service/Session/require_selected.md)
- [delete_selected_indices](../../../../../../functions/crates/lpe-imap/src/mailboxes/Session/delete_selected_indices.md)

# Called by

- [handle_request](../../../../../../functions/crates/lpe-imap/src/service/Session/handle_request.md)