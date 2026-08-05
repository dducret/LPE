---
type: Rust Method
title: handle_check
resource: crates/lpe-imap/src/mailboxes.rs#L509-L520
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-imap/src/service/Session/require_selected
  - functions/crates/lpe-imap/src/service/Session/refresh_selected_updates
  called_by:
  - functions/crates/lpe-imap/src/service/Session/handle_request
---

# Signature

`pub(crate) async fn handle_check<W>(&mut self, tag: &str, writer: &mut W) -> Result<bool> where W: AsyncWriteExt + Unpin,`

# Calls

- [require_selected](../../../../../../functions/crates/lpe-imap/src/service/Session/require_selected.md)
- [refresh_selected_updates](../../../../../../functions/crates/lpe-imap/src/service/Session/refresh_selected_updates.md)

# Called by

- [handle_request](../../../../../../functions/crates/lpe-imap/src/service/Session/handle_request.md)