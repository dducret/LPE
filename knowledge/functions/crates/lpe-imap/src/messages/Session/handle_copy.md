---
type: Rust Method
title: handle_copy
resource: crates/lpe-imap/src/messages.rs#L269-L332
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-imap/src/parse/split_two
  - functions/crates/lpe-imap/src/mailboxes/Session/resolve_mailbox_by_name
  - functions/crates/lpe-imap/src/service/Session/require_selected
  - functions/crates/lpe-imap/src/messages/ensure_copy_allowed
  - functions/crates/lpe-imap/src/render/resolve_message_indexes
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-imap/src/service/Session/refresh_selected
  called_by:
  - functions/crates/lpe-imap/src/service/Session/handle_request
  - functions/crates/lpe-imap/src/uid/Session/handle_uid
---

# Signature

`pub(crate) async fn handle_copy<W>( &mut self, tag: &str, arguments: &str, writer: &mut W, ref_kind: MessageRefKind, ) -> Result<bool> where W: AsyncWriteExt + Unpin,`

# Calls

- [split_two](../../../../../../functions/crates/lpe-imap/src/parse/split_two.md)
- [resolve_mailbox_by_name](../../../../../../functions/crates/lpe-imap/src/mailboxes/Session/resolve_mailbox_by_name.md)
- [require_selected](../../../../../../functions/crates/lpe-imap/src/service/Session/require_selected.md)
- [ensure_copy_allowed](../../../../../../functions/crates/lpe-imap/src/messages/ensure_copy_allowed.md)
- [resolve_message_indexes](../../../../../../functions/crates/lpe-imap/src/render/resolve_message_indexes.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [refresh_selected](../../../../../../functions/crates/lpe-imap/src/service/Session/refresh_selected.md)

# Called by

- [handle_request](../../../../../../functions/crates/lpe-imap/src/service/Session/handle_request.md)
- [handle_uid](../../../../../../functions/crates/lpe-imap/src/uid/Session/handle_uid.md)