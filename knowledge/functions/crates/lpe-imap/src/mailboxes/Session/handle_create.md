---
type: Rust Method
title: handle_create
resource: crates/lpe-imap/src/mailboxes.rs#L278-L307
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-imap/src/parse/parse_mailbox_path_token
  called_by:
  - functions/crates/lpe-imap/src/service/Session/handle_request
---

# Signature

`pub(crate) async fn handle_create<W>( &mut self, tag: &str, arguments: &str, writer: &mut W, ) -> Result<bool> where W: AsyncWriteExt + Unpin,`

# Calls

- [parse_mailbox_path_token](../../../../../../functions/crates/lpe-imap/src/parse/parse_mailbox_path_token.md)

# Called by

- [handle_request](../../../../../../functions/crates/lpe-imap/src/service/Session/handle_request.md)