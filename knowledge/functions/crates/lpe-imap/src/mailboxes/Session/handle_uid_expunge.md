---
type: Rust Method
title: handle_uid_expunge
resource: crates/lpe-imap/src/mailboxes.rs#L580-L603
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-imap/src/service/Session/require_selected
  - functions/crates/lpe-imap/src/render/resolve_message_indexes
  - functions/crates/lpe-imap/src/mailboxes/Session/expunge_selected_indices
  - functions/crates/lpe-imap/src/service/Session/refresh_selected
  called_by:
  - functions/crates/lpe-imap/src/uid/Session/handle_uid
---

# Signature

`pub(crate) async fn handle_uid_expunge<W>( &mut self, tag: &str, arguments: &str, writer: &mut W, ) -> Result<bool> where W: AsyncWriteExt + Unpin,`

# Calls

- [require_selected](../../../../../../functions/crates/lpe-imap/src/service/Session/require_selected.md)
- [resolve_message_indexes](../../../../../../functions/crates/lpe-imap/src/render/resolve_message_indexes.md)
- [expunge_selected_indices](../../../../../../functions/crates/lpe-imap/src/mailboxes/Session/expunge_selected_indices.md)
- [refresh_selected](../../../../../../functions/crates/lpe-imap/src/service/Session/refresh_selected.md)

# Called by

- [handle_uid](../../../../../../functions/crates/lpe-imap/src/uid/Session/handle_uid.md)