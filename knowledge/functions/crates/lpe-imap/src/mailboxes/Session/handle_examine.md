---
type: Rust Method
title: handle_examine
resource: crates/lpe-imap/src/mailboxes.rs#L397-L407
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-imap/src/mailboxes/Session/handle_select_mode
  called_by:
  - functions/crates/lpe-imap/src/service/Session/handle_request
---

# Signature

`pub(crate) async fn handle_examine<W>( &mut self, tag: &str, arguments: &str, writer: &mut W, ) -> Result<bool> where W: AsyncWriteExt + Unpin,`

# Calls

- [handle_select_mode](../../../../../../functions/crates/lpe-imap/src/mailboxes/Session/handle_select_mode.md)

# Called by

- [handle_request](../../../../../../functions/crates/lpe-imap/src/service/Session/handle_request.md)