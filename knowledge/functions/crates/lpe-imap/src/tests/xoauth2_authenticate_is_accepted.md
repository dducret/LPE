---
type: Rust Function
title: xoauth2_authenticate_is_accepted
resource: crates/lpe-imap/src/tests.rs#L3529-L3578
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-mail-auth/src/oauth/issue_oauth_access_token
  - functions/crates/lpe-imap/src/service/ImapServer/with_validator
  - functions/crates/lpe-storage/src/core/Storage/connect
  - functions/crates/lpe-imap/src/tests/read_response
  - functions/crates/lpe-imap/src/tests/send_command
---

# Signature

`async fn xoauth2_authenticate_is_accepted()`

# Calls

- [issue_oauth_access_token](../../../../../functions/crates/lpe-mail-auth/src/oauth/issue_oauth_access_token.md)
- [with_validator](../../../../../functions/crates/lpe-imap/src/service/ImapServer/with_validator.md)
- [connect](../../../../../functions/crates/lpe-storage/src/core/Storage/connect.md)
- [read_response](../../../../../functions/crates/lpe-imap/src/tests/read_response.md)
- [send_command](../../../../../functions/crates/lpe-imap/src/tests/send_command.md)