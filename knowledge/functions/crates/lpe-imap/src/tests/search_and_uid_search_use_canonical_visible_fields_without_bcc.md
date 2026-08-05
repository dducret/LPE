---
type: Rust Function
title: search_and_uid_search_use_canonical_visible_fields_without_bcc
resource: crates/lpe-imap/src/tests.rs#L2792-L2959
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-imap/src/service/ImapServer/with_validator
  - functions/crates/lpe-storage/src/core/Storage/connect
  - functions/crates/lpe-imap/src/tests/read_response
  - functions/crates/lpe-imap/src/tests/send_command
---

# Signature

`async fn search_and_uid_search_use_canonical_visible_fields_without_bcc()`

# Calls

- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [with_validator](../../../../../functions/crates/lpe-imap/src/service/ImapServer/with_validator.md)
- [connect](../../../../../functions/crates/lpe-storage/src/core/Storage/connect.md)
- [read_response](../../../../../functions/crates/lpe-imap/src/tests/read_response.md)
- [send_command](../../../../../functions/crates/lpe-imap/src/tests/send_command.md)