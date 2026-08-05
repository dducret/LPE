---
type: Rust Function
title: ping_detects_changes_across_multiple_monitored_collections
resource: crates/lpe-activesync/src/tests.rs#L5403-L5445
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/FakeStore/sent_mailbox
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-activesync/src/tests/ping_request
  - functions/crates/lpe-activesync/src/tests/ping
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/text_value
---

# Signature

`async fn ping_detects_changes_across_multiple_monitored_collections()`

# Calls

- [sent_mailbox](../../../../../functions/crates/lpe-activesync/src/tests/FakeStore/sent_mailbox.md)
- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [ping_request](../../../../../functions/crates/lpe-activesync/src/tests/ping_request.md)
- [ping](../../../../../functions/crates/lpe-activesync/src/tests/ping.md)
- [text_value](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/text_value.md)