---
type: Rust Method
title: handle_fetch
resource: crates/lpe-imap/src/messages.rs#L19-L103
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-imap/src/messages/parse_fetch_arguments
  - functions/crates/lpe-imap/src/render/parse_fetch_attributes
  - functions/crates/lpe-imap/src/render/ensure_uid_fetch_attributes
  - functions/crates/lpe-imap/src/messages/ensure_condstore_fetch_attributes
  - functions/crates/lpe-imap/src/service/Session/refresh_selected
  - functions/crates/lpe-imap/src/service/Session/require_selected
  - functions/crates/lpe-imap/src/render/resolve_message_indexes
  - functions/crates/lpe-imap/src/render/render_fetch_response
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-imap/src/service/Session/handle_request
  - functions/crates/lpe-imap/src/uid/Session/handle_uid
---

# Signature

`pub(crate) async fn handle_fetch<W>( &mut self, tag: &str, arguments: &str, writer: &mut W, ref_kind: MessageRefKind, ) -> Result<bool> where W: AsyncWriteExt + Unpin,`

# Calls

- [parse_fetch_arguments](../../../../../../functions/crates/lpe-imap/src/messages/parse_fetch_arguments.md)
- [parse_fetch_attributes](../../../../../../functions/crates/lpe-imap/src/render/parse_fetch_attributes.md)
- [ensure_uid_fetch_attributes](../../../../../../functions/crates/lpe-imap/src/render/ensure_uid_fetch_attributes.md)
- [ensure_condstore_fetch_attributes](../../../../../../functions/crates/lpe-imap/src/messages/ensure_condstore_fetch_attributes.md)
- [refresh_selected](../../../../../../functions/crates/lpe-imap/src/service/Session/refresh_selected.md)
- [require_selected](../../../../../../functions/crates/lpe-imap/src/service/Session/require_selected.md)
- [resolve_message_indexes](../../../../../../functions/crates/lpe-imap/src/render/resolve_message_indexes.md)
- [render_fetch_response](../../../../../../functions/crates/lpe-imap/src/render/render_fetch_response.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [handle_request](../../../../../../functions/crates/lpe-imap/src/service/Session/handle_request.md)
- [handle_uid](../../../../../../functions/crates/lpe-imap/src/uid/Session/handle_uid.md)