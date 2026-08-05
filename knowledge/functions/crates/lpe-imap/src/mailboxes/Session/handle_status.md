---
type: Rust Method
title: handle_status
resource: crates/lpe-imap/src/mailboxes.rs#L224-L276
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-imap/src/parse/parse_mailbox_path_token
  - functions/crates/lpe-imap/src/mailboxes/mailbox_matches_path
  - functions/crates/lpe-imap/src/mailboxes/render_mailbox_path
  - functions/crates/lpe-imap/src/render/parse_status_items
  - functions/crates/lpe-imap/src/render/render_status_response
  called_by:
  - functions/crates/lpe-imap/src/service/Session/handle_request
---

# Signature

`pub(crate) async fn handle_status<W>( &mut self, tag: &str, arguments: &str, writer: &mut W, ) -> Result<bool> where W: AsyncWriteExt + Unpin,`

# Calls

- [parse_mailbox_path_token](../../../../../../functions/crates/lpe-imap/src/parse/parse_mailbox_path_token.md)
- [mailbox_matches_path](../../../../../../functions/crates/lpe-imap/src/mailboxes/mailbox_matches_path.md)
- [render_mailbox_path](../../../../../../functions/crates/lpe-imap/src/mailboxes/render_mailbox_path.md)
- [parse_status_items](../../../../../../functions/crates/lpe-imap/src/render/parse_status_items.md)
- [render_status_response](../../../../../../functions/crates/lpe-imap/src/render/render_status_response.md)

# Called by

- [handle_request](../../../../../../functions/crates/lpe-imap/src/service/Session/handle_request.md)