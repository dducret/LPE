---
type: Rust Method
title: handle_login
resource: crates/lpe-imap/src/auth.rs#L10-L39
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-mail-auth/src/auth/authenticate_plain_credentials
  called_by:
  - functions/crates/lpe-imap/src/service/Session/handle_request
---

# Signature

`pub(crate) async fn handle_login<W>( &mut self, tag: &str, arguments: &str, writer: &mut W, ) -> Result<bool> where W: AsyncWriteExt + Unpin,`

# Calls

- [authenticate_plain_credentials](../../../../../../functions/crates/lpe-mail-auth/src/auth/authenticate_plain_credentials.md)

# Called by

- [handle_request](../../../../../../functions/crates/lpe-imap/src/service/Session/handle_request.md)