---
type: Rust Function
title: normalize_subject
resource: crates/lpe-storage/src/util.rs#L31-L33
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/inbound/Storage/deliver_inbound_message
  - functions/crates/lpe-storage/src/message_ops/Storage/update_jmap_email_content
  - functions/crates/lpe-storage/src/message_ops/Storage/import_jmap_email
  - functions/crates/lpe-storage/src/submission/Storage/save_draft_message
  - functions/crates/lpe-storage/src/submission/Storage/submit_message
---

# Signature

`pub(crate) fn normalize_subject(value: &str) -> String`

# Called by

- [deliver_inbound_message](../../../../../functions/crates/lpe-storage/src/inbound/Storage/deliver_inbound_message.md)
- [update_jmap_email_content](../../../../../functions/crates/lpe-storage/src/message_ops/Storage/update_jmap_email_content.md)
- [import_jmap_email](../../../../../functions/crates/lpe-storage/src/message_ops/Storage/import_jmap_email.md)
- [save_draft_message](../../../../../functions/crates/lpe-storage/src/submission/Storage/save_draft_message.md)
- [submit_message](../../../../../functions/crates/lpe-storage/src/submission/Storage/submit_message.md)