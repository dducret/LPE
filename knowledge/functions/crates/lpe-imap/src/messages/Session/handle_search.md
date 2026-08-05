---
type: Rust Method
title: handle_search
resource: crates/lpe-imap/src/messages.rs#L214-L267
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-imap/src/messages/strip_search_return_options
  - functions/crates/lpe-imap/src/service/Session/refresh_selected
  - functions/crates/lpe-imap/src/service/Session/require_selected
  - functions/crates/lpe-imap/src/search/SearchExpression/from_tokens
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
---

# Signature

`pub(crate) async fn handle_search<W>( &mut self, tag: &str, arguments: &str, writer: &mut W, ref_kind: MessageRefKind, ) -> Result<bool> where W: AsyncWriteExt + Unpin,`

# Calls

- [strip_search_return_options](../../../../../../functions/crates/lpe-imap/src/messages/strip_search_return_options.md)
- [refresh_selected](../../../../../../functions/crates/lpe-imap/src/service/Session/refresh_selected.md)
- [require_selected](../../../../../../functions/crates/lpe-imap/src/service/Session/require_selected.md)
- [from_tokens](../../../../../../functions/crates/lpe-imap/src/search/SearchExpression/from_tokens.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)