---
type: Rust Method
title: handle_getquotaroot
resource: crates/lpe-imap/src/service.rs#L538-L568
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-imap/src/mailboxes/Session/resolve_mailbox_by_name
  - functions/crates/lpe-imap/src/mailboxes/render_mailbox_path
  called_by:
  - functions/crates/lpe-imap/src/service/Session/handle_request
---

# Signature

`async fn handle_getquotaroot<W>( &mut self, tag: &str, arguments: &str, writer: &mut W, ) -> Result<bool> where W: AsyncWriteExt + Unpin,`

# Calls

- [resolve_mailbox_by_name](../../../../../../functions/crates/lpe-imap/src/mailboxes/Session/resolve_mailbox_by_name.md)
- [render_mailbox_path](../../../../../../functions/crates/lpe-imap/src/mailboxes/render_mailbox_path.md)

# Called by

- [handle_request](../../../../../../functions/crates/lpe-imap/src/service/Session/handle_request.md)