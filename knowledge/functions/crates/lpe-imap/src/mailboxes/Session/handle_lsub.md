---
type: Rust Method
title: handle_lsub
resource: crates/lpe-imap/src/mailboxes.rs#L105-L147
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-imap/src/mailboxes/parse_list_pattern
  - functions/crates/lpe-imap/src/mailboxes/render_mailbox_path
  - functions/crates/lpe-imap/src/mailboxes/mailbox_matches_pattern
  called_by:
  - functions/crates/lpe-imap/src/service/Session/handle_request
---

# Signature

`pub(crate) async fn handle_lsub<W>( &mut self, tag: &str, arguments: &str, writer: &mut W, ) -> Result<bool> where W: AsyncWriteExt + Unpin,`

# Calls

- [parse_list_pattern](../../../../../../functions/crates/lpe-imap/src/mailboxes/parse_list_pattern.md)
- [render_mailbox_path](../../../../../../functions/crates/lpe-imap/src/mailboxes/render_mailbox_path.md)
- [mailbox_matches_pattern](../../../../../../functions/crates/lpe-imap/src/mailboxes/mailbox_matches_pattern.md)

# Called by

- [handle_request](../../../../../../functions/crates/lpe-imap/src/service/Session/handle_request.md)