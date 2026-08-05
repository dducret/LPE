---
type: Rust Method
title: handle_append
resource: crates/lpe-imap/src/append.rs#L19-L231
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-imap/src/append/Session/resolve_append_mailbox
  - functions/crates/lpe-imap/src/parse/parse_literal_size
  - functions/crates/lpe-imap/src/append/validate_append_attachments
  - functions/crates/lpe-storage/src/mail/parse_rfc822_message
  - functions/crates/lpe-imap/src/append/sent_append_ack_uid
  - functions/crates/lpe-imap/src/service/Session/refresh_selected
  called_by:
  - functions/crates/lpe-imap/src/service/Session/handle_request
---

# Signature

`pub(crate) async fn handle_append<R, W>( &mut self, reader: &mut BufReader<R>, writer: &mut W, tag: &str, arguments: &str, ) -> Result<bool> where R: AsyncReadExt + Unpin, W: AsyncWriteExt + Unpin,`

# Calls

- [resolve_append_mailbox](../../../../../../functions/crates/lpe-imap/src/append/Session/resolve_append_mailbox.md)
- [parse_literal_size](../../../../../../functions/crates/lpe-imap/src/parse/parse_literal_size.md)
- [validate_append_attachments](../../../../../../functions/crates/lpe-imap/src/append/validate_append_attachments.md)
- [parse_rfc822_message](../../../../../../functions/crates/lpe-storage/src/mail/parse_rfc822_message.md)
- [sent_append_ack_uid](../../../../../../functions/crates/lpe-imap/src/append/sent_append_ack_uid.md)
- [refresh_selected](../../../../../../functions/crates/lpe-imap/src/service/Session/refresh_selected.md)

# Called by

- [handle_request](../../../../../../functions/crates/lpe-imap/src/service/Session/handle_request.md)