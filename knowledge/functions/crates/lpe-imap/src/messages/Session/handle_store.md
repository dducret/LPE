---
type: Rust Method
title: handle_store
resource: crates/lpe-imap/src/messages.rs#L105-L212
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-imap/src/store_args/parse_store_arguments
  - functions/crates/lpe-imap/src/store_args/parse_store_mode
  - functions/crates/lpe-imap/src/store_args/parse_flag_list
  - functions/crates/lpe-imap/src/messages/ensure_store_flags_supported
  - functions/crates/lpe-imap/src/service/Session/require_selected
  - functions/crates/lpe-imap/src/render/resolve_message_indexes
  - functions/crates/lpe-imap/src/messages/flag_present
  - functions/crates/lpe-imap/src/service/Session/refresh_selected
  - functions/crates/lpe-imap/src/render/render_modified_set
  called_by:
  - functions/crates/lpe-imap/src/service/Session/handle_request
  - functions/crates/lpe-imap/src/uid/Session/handle_uid
---

# Signature

`pub(crate) async fn handle_store<W>( &mut self, tag: &str, arguments: &str, writer: &mut W, ref_kind: MessageRefKind, ) -> Result<bool> where W: AsyncWriteExt + Unpin,`

# Calls

- [parse_store_arguments](../../../../../../functions/crates/lpe-imap/src/store_args/parse_store_arguments.md)
- [parse_store_mode](../../../../../../functions/crates/lpe-imap/src/store_args/parse_store_mode.md)
- [parse_flag_list](../../../../../../functions/crates/lpe-imap/src/store_args/parse_flag_list.md)
- [ensure_store_flags_supported](../../../../../../functions/crates/lpe-imap/src/messages/ensure_store_flags_supported.md)
- [require_selected](../../../../../../functions/crates/lpe-imap/src/service/Session/require_selected.md)
- [resolve_message_indexes](../../../../../../functions/crates/lpe-imap/src/render/resolve_message_indexes.md)
- [flag_present](../../../../../../functions/crates/lpe-imap/src/messages/flag_present.md)
- [refresh_selected](../../../../../../functions/crates/lpe-imap/src/service/Session/refresh_selected.md)
- [render_modified_set](../../../../../../functions/crates/lpe-imap/src/render/render_modified_set.md)

# Called by

- [handle_request](../../../../../../functions/crates/lpe-imap/src/service/Session/handle_request.md)
- [handle_uid](../../../../../../functions/crates/lpe-imap/src/uid/Session/handle_uid.md)