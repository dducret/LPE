---
type: Rust Method
title: handle_uid
resource: crates/lpe-imap/src/uid.rs#L8-L44
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-imap/src/parse/split_two
  - functions/crates/lpe-imap/src/messages/Session/handle_fetch
  - functions/crates/lpe-imap/src/messages/Session/handle_store
  - functions/crates/lpe-imap/src/messages/Session/handle_copy
  - functions/crates/lpe-imap/src/messages/Session/handle_move
  - functions/crates/lpe-imap/src/mailboxes/Session/handle_uid_expunge
  called_by:
  - functions/crates/lpe-imap/src/service/Session/handle_request
---

# Signature

`pub(crate) async fn handle_uid<R, W>( &mut self, _reader: &mut BufReader<R>, writer: &mut W, tag: &str, arguments: &str, ) -> Result<bool> where R: AsyncReadExt + Unpin, W: AsyncWriteExt + Unpin,`

# Calls

- [split_two](../../../../../../functions/crates/lpe-imap/src/parse/split_two.md)
- [handle_fetch](../../../../../../functions/crates/lpe-imap/src/messages/Session/handle_fetch.md)
- [handle_store](../../../../../../functions/crates/lpe-imap/src/messages/Session/handle_store.md)
- [handle_copy](../../../../../../functions/crates/lpe-imap/src/messages/Session/handle_copy.md)
- [handle_move](../../../../../../functions/crates/lpe-imap/src/messages/Session/handle_move.md)
- [handle_uid_expunge](../../../../../../functions/crates/lpe-imap/src/mailboxes/Session/handle_uid_expunge.md)

# Called by

- [handle_request](../../../../../../functions/crates/lpe-imap/src/service/Session/handle_request.md)