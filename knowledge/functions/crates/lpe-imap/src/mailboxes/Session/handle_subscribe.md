---
type: Rust Method
title: handle_subscribe
resource: crates/lpe-imap/src/mailboxes.rs#L149-L177
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-imap/src/mailboxes/Session/resolve_mailbox_by_name
  called_by:
  - functions/crates/lpe-imap/src/service/Session/handle_request
---

# Signature

`pub(crate) async fn handle_subscribe<W>( &mut self, tag: &str, arguments: &str, writer: &mut W, ) -> Result<bool> where W: AsyncWriteExt + Unpin,`

# Calls

- [resolve_mailbox_by_name](../../../../../../functions/crates/lpe-imap/src/mailboxes/Session/resolve_mailbox_by_name.md)

# Called by

- [handle_request](../../../../../../functions/crates/lpe-imap/src/service/Session/handle_request.md)