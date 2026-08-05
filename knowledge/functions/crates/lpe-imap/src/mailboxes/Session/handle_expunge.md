---
type: Rust Method
title: handle_expunge
resource: crates/lpe-imap/src/mailboxes.rs#L559-L578
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-imap/src/service/Session/require_selected
  - functions/crates/lpe-imap/src/mailboxes/Session/expunge_selected_indices
  - functions/crates/lpe-imap/src/service/Session/refresh_selected
  called_by:
  - functions/crates/lpe-imap/src/service/Session/handle_request
---

# Signature

`pub(crate) async fn handle_expunge<W>(&mut self, tag: &str, writer: &mut W) -> Result<bool> where W: AsyncWriteExt + Unpin,`

# Calls

- [require_selected](../../../../../../functions/crates/lpe-imap/src/service/Session/require_selected.md)
- [expunge_selected_indices](../../../../../../functions/crates/lpe-imap/src/mailboxes/Session/expunge_selected_indices.md)
- [refresh_selected](../../../../../../functions/crates/lpe-imap/src/service/Session/refresh_selected.md)

# Called by

- [handle_request](../../../../../../functions/crates/lpe-imap/src/service/Session/handle_request.md)