---
type: Rust Method
title: handle_getacl
resource: crates/lpe-imap/src/acl.rs#L30-L81
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-imap/src/mailboxes/Session/resolve_mailbox_by_name
  - functions/crates/lpe-imap/src/acl/combine_acl_state
  - functions/crates/lpe-imap/src/acl/render_acl_rights
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-imap/src/service/Session/handle_request
---

# Signature

`pub(crate) async fn handle_getacl<W>( &mut self, tag: &str, arguments: &str, writer: &mut W, ) -> Result<bool> where W: AsyncWriteExt + Unpin,`

# Calls

- [resolve_mailbox_by_name](../../../../../../functions/crates/lpe-imap/src/mailboxes/Session/resolve_mailbox_by_name.md)
- [combine_acl_state](../../../../../../functions/crates/lpe-imap/src/acl/combine_acl_state.md)
- [render_acl_rights](../../../../../../functions/crates/lpe-imap/src/acl/render_acl_rights.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [handle_request](../../../../../../functions/crates/lpe-imap/src/service/Session/handle_request.md)