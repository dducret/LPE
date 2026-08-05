---
type: Rust Method
title: handle_delete
resource: crates/lpe-imap/src/mailboxes.rs#L309-L340
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-imap/src/mailboxes/Session/resolve_mailbox_by_name
---

# Signature

`pub(crate) async fn handle_delete<W>( &mut self, tag: &str, arguments: &str, writer: &mut W, ) -> Result<bool> where W: AsyncWriteExt + Unpin,`

# Calls

- [resolve_mailbox_by_name](../../../../../../functions/crates/lpe-imap/src/mailboxes/Session/resolve_mailbox_by_name.md)