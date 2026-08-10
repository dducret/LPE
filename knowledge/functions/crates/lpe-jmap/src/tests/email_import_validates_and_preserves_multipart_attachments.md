---
type: Rust Function
title: email_import_validates_and_preserves_multipart_attachments
resource: crates/lpe-jmap/src/tests.rs#L7605-L7694
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-jmap/src/tests/validator_sequence
  - functions/crates/lpe-jmap/src/service/JmapService/handle_api_request
---

# Signature

`async fn email_import_validates_and_preserves_multipart_attachments()`

# Calls

- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [validator_sequence](../../../../../functions/crates/lpe-jmap/src/tests/validator_sequence.md)
- [handle_api_request](../../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_api_request.md)