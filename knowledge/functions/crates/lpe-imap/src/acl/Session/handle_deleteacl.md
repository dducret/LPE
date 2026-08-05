---
type: Rust Method
title: handle_deleteacl
resource: crates/lpe-imap/src/acl.rs#L174-L193
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-imap/src/acl/Session/apply_acl_update
  called_by:
  - functions/crates/lpe-imap/src/service/Session/handle_request
---

# Signature

`pub(crate) async fn handle_deleteacl<W>( &mut self, tag: &str, arguments: &str, writer: &mut W, ) -> Result<bool> where W: AsyncWriteExt + Unpin,`

# Calls

- [apply_acl_update](../../../../../../functions/crates/lpe-imap/src/acl/Session/apply_acl_update.md)

# Called by

- [handle_request](../../../../../../functions/crates/lpe-imap/src/service/Session/handle_request.md)