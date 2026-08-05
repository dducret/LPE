---
type: Rust Function
title: outlook_first_login_list_select_sync_transcript
resource: crates/lpe-imap/src/tests.rs#L1784-L2057
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
  - functions/crates/lpe-imap/src/tests/parse_response_number_after
  - functions/crates/lpe-imap/src/tests/parse_literal_size_after_label
---

# Signature

`async fn outlook_first_login_list_select_sync_transcript()`

# Calls

- [with_validator](../../../../../functions/crates/lpe-imap/src/service/ImapServer/with_validator.md)
- [connect](../../../../../functions/crates/lpe-storage/src/core/Storage/connect.md)
- [read_response](../../../../../functions/crates/lpe-imap/src/tests/read_response.md)
- [send_command](../../../../../functions/crates/lpe-imap/src/tests/send_command.md)
- [assert_documented_capabilities](../../../../../functions/crates/lpe-imap/src/tests/assert_documented_capabilities.md)
- [parse_response_number_after](../../../../../functions/crates/lpe-imap/src/tests/parse_response_number_after.md)
- [parse_literal_size_after_label](../../../../../functions/crates/lpe-imap/src/tests/parse_literal_size_after_label.md)