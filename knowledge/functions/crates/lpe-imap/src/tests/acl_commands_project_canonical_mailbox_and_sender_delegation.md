---
type: Rust Function
title: acl_commands_project_canonical_mailbox_and_sender_delegation
resource: crates/lpe-imap/src/tests.rs#L3783-L3866
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-imap/src/service/ImapServer/with_validator
  - functions/crates/lpe-storage/src/core/Storage/connect
  - functions/crates/lpe-imap/src/tests/read_response
  - functions/crates/lpe-imap/src/tests/send_command
  - functions/crates/lpe-imap/src/tests/assert_documented_capabilities
  - functions/crates/lpe-core/src/sieve/Parser/expect
---

# Signature

`async fn acl_commands_project_canonical_mailbox_and_sender_delegation()`

# Calls

- [with_validator](../../../../../functions/crates/lpe-imap/src/service/ImapServer/with_validator.md)
- [connect](../../../../../functions/crates/lpe-storage/src/core/Storage/connect.md)
- [read_response](../../../../../functions/crates/lpe-imap/src/tests/read_response.md)
- [send_command](../../../../../functions/crates/lpe-imap/src/tests/send_command.md)
- [assert_documented_capabilities](../../../../../functions/crates/lpe-imap/src/tests/assert_documented_capabilities.md)
- [expect](../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)