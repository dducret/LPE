---
type: Rust Method
title: handle_select_mode
resource: crates/lpe-imap/src/mailboxes.rs#L409-L507
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-imap/src/mailboxes/parse_select_mailbox_path
  - functions/crates/lpe-imap/src/mailboxes/mailbox_matches_path
  - functions/crates/lpe-imap/src/mailboxes/render_mailbox_path
  called_by:
  - functions/crates/lpe-imap/src/mailboxes/Session/handle_select
  - functions/crates/lpe-imap/src/mailboxes/Session/handle_examine
---

# Signature

`async fn handle_select_mode<W>( &mut self, tag: &str, arguments: &str, writer: &mut W, read_only: bool, ) -> Result<bool> where W: AsyncWriteExt + Unpin,`

# Calls

- [parse_select_mailbox_path](../../../../../../functions/crates/lpe-imap/src/mailboxes/parse_select_mailbox_path.md)
- [mailbox_matches_path](../../../../../../functions/crates/lpe-imap/src/mailboxes/mailbox_matches_path.md)
- [render_mailbox_path](../../../../../../functions/crates/lpe-imap/src/mailboxes/render_mailbox_path.md)

# Called by

- [handle_select](../../../../../../functions/crates/lpe-imap/src/mailboxes/Session/handle_select.md)
- [handle_examine](../../../../../../functions/crates/lpe-imap/src/mailboxes/Session/handle_examine.md)