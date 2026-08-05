---
type: Rust Method
title: handle_xlist
resource: crates/lpe-imap/src/mailboxes.rs#L32-L43
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-imap/src/mailboxes/Session/handle_mailbox_listing
  called_by:
  - functions/crates/lpe-imap/src/service/Session/handle_request
---

# Signature

`pub(crate) async fn handle_xlist<W>( &mut self, tag: &str, arguments: &str, writer: &mut W, ) -> Result<bool> where W: AsyncWriteExt + Unpin,`

# Calls

- [handle_mailbox_listing](../../../../../../functions/crates/lpe-imap/src/mailboxes/Session/handle_mailbox_listing.md)

# Called by

- [handle_request](../../../../../../functions/crates/lpe-imap/src/service/Session/handle_request.md)