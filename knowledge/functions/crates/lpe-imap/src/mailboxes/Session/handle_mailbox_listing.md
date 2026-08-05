---
type: Rust Method
title: handle_mailbox_listing
resource: crates/lpe-imap/src/mailboxes.rs#L45-L103
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-imap/src/mailboxes/parse_list_pattern
  - functions/crates/lpe-imap/src/mailboxes/render_mailbox_path
  - functions/crates/lpe-imap/src/mailboxes/mailbox_matches_pattern
  called_by:
  - functions/crates/lpe-imap/src/mailboxes/Session/handle_list
  - functions/crates/lpe-imap/src/mailboxes/Session/handle_xlist
---

# Signature

`async fn handle_mailbox_listing<W>( &mut self, tag: &str, arguments: &str, writer: &mut W, command_name: &str, legacy_xlist: bool, ) -> Result<bool> where W: AsyncWriteExt + Unpin,`

# Calls

- [parse_list_pattern](../../../../../../functions/crates/lpe-imap/src/mailboxes/parse_list_pattern.md)
- [render_mailbox_path](../../../../../../functions/crates/lpe-imap/src/mailboxes/render_mailbox_path.md)
- [mailbox_matches_pattern](../../../../../../functions/crates/lpe-imap/src/mailboxes/mailbox_matches_pattern.md)

# Called by

- [handle_list](../../../../../../functions/crates/lpe-imap/src/mailboxes/Session/handle_list.md)
- [handle_xlist](../../../../../../functions/crates/lpe-imap/src/mailboxes/Session/handle_xlist.md)