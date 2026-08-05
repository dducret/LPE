---
type: Rust Function
title: unicode_nested_paths_and_list_wildcards_work_by_segment
resource: crates/lpe-imap/src/tests.rs#L1383-L1482
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-imap/src/service/ImapServer/with_validator
  - functions/crates/lpe-storage/src/core/Storage/connect
  - functions/crates/lpe-imap/src/tests/read_response
  - functions/crates/lpe-imap/src/tests/send_command
  - functions/crates/lpe-core/src/sieve/Parser/expect
---

# Signature

`async fn unicode_nested_paths_and_list_wildcards_work_by_segment()`

# Calls

- [with_validator](../../../../../functions/crates/lpe-imap/src/service/ImapServer/with_validator.md)
- [connect](../../../../../functions/crates/lpe-storage/src/core/Storage/connect.md)
- [read_response](../../../../../functions/crates/lpe-imap/src/tests/read_response.md)
- [send_command](../../../../../functions/crates/lpe-imap/src/tests/send_command.md)
- [expect](../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)