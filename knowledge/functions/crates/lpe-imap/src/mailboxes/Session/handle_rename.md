---
type: Rust Method
title: handle_rename
resource: crates/lpe-imap/src/mailboxes.rs#L342-L383
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-imap/src/parse/parse_mailbox_path
  - functions/crates/lpe-imap/src/mailboxes/Session/resolve_mailbox_path
  called_by:
  - functions/crates/lpe-imap/src/service/Session/handle_request
---

# Signature

`pub(crate) async fn handle_rename<W>( &mut self, tag: &str, arguments: &str, writer: &mut W, ) -> Result<bool> where W: AsyncWriteExt + Unpin,`

# Calls

- [parse_mailbox_path](../../../../../../functions/crates/lpe-imap/src/parse/parse_mailbox_path.md)
- [resolve_mailbox_path](../../../../../../functions/crates/lpe-imap/src/mailboxes/Session/resolve_mailbox_path.md)

# Called by

- [handle_request](../../../../../../functions/crates/lpe-imap/src/service/Session/handle_request.md)