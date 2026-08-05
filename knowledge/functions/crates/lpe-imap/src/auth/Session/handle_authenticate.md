---
type: Rust Method
title: handle_authenticate
resource: crates/lpe-imap/src/auth.rs#L41-L139
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-imap/src/auth/parse_plain_initial_response
  - functions/crates/lpe-mail-auth/src/auth/authenticate_plain_credentials
  - functions/crates/lpe-imap/src/auth/read_authenticate_response
  - functions/crates/lpe-imap/src/auth/parse_login_response
  - functions/crates/lpe-mail-auth/src/auth/authenticate_bearer_access_token
  called_by:
  - functions/crates/lpe-imap/src/service/Session/handle_request
---

# Signature

`pub(crate) async fn handle_authenticate<R, W>( &mut self, tag: &str, arguments: &str, reader: &mut BufReader<R>, writer: &mut W, ) -> Result<bool> where R: AsyncReadExt + Unpin, W: AsyncWriteExt + Unpin,`

# Calls

- [parse_plain_initial_response](../../../../../../functions/crates/lpe-imap/src/auth/parse_plain_initial_response.md)
- [authenticate_plain_credentials](../../../../../../functions/crates/lpe-mail-auth/src/auth/authenticate_plain_credentials.md)
- [read_authenticate_response](../../../../../../functions/crates/lpe-imap/src/auth/read_authenticate_response.md)
- [parse_login_response](../../../../../../functions/crates/lpe-imap/src/auth/parse_login_response.md)
- [authenticate_bearer_access_token](../../../../../../functions/crates/lpe-mail-auth/src/auth/authenticate_bearer_access_token.md)

# Called by

- [handle_request](../../../../../../functions/crates/lpe-imap/src/service/Session/handle_request.md)