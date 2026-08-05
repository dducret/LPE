---
type: Rust Function
title: get_item_returns_requested_mime_content_without_leaking_bcc_for_normal_mailbox
resource: crates/lpe-exchange/src/tests/ews.rs#L7991-L8024
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/tests/decoded_mime_content
---

# Signature

`async fn get_item_returns_requested_mime_content_without_leaking_bcc_for_normal_mailbox()`

# Calls

- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [decoded_mime_content](../../../../../../functions/crates/lpe-exchange/src/tests/decoded_mime_content.md)