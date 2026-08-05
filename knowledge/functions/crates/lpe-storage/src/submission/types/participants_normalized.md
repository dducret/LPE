---
type: Rust Function
title: participants_normalized
resource: crates/lpe-storage/src/submission/types.rs#L352-L366
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-storage/src/inbound/Storage/deliver_inbound_message
  - functions/crates/lpe-storage/src/message_ops/Storage/import_jmap_email
  - functions/crates/lpe-storage/src/shared/participants_normalized_ignores_bcc_addresses
  - functions/crates/lpe-storage/src/shared/participants_normalized_remains_visible_only_even_with_bcc_display_name
  - functions/crates/lpe-storage/src/shared/participants_normalized_allows_null_reverse_path
  - functions/crates/lpe-storage/src/submission/Storage/save_draft_message
  - functions/crates/lpe-storage/src/submission/Storage/submit_message
---

# Signature

`pub(crate) fn participants_normalized( from_address: &str, recipients: &[(&'static str, SubmittedRecipientInput)], ) -> String`

# Calls

- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [deliver_inbound_message](../../../../../../functions/crates/lpe-storage/src/inbound/Storage/deliver_inbound_message.md)
- [import_jmap_email](../../../../../../functions/crates/lpe-storage/src/message_ops/Storage/import_jmap_email.md)
- [participants_normalized_ignores_bcc_addresses](../../../../../../functions/crates/lpe-storage/src/shared/participants_normalized_ignores_bcc_addresses.md)
- [participants_normalized_remains_visible_only_even_with_bcc_display_name](../../../../../../functions/crates/lpe-storage/src/shared/participants_normalized_remains_visible_only_even_with_bcc_display_name.md)
- [participants_normalized_allows_null_reverse_path](../../../../../../functions/crates/lpe-storage/src/shared/participants_normalized_allows_null_reverse_path.md)
- [save_draft_message](../../../../../../functions/crates/lpe-storage/src/submission/Storage/save_draft_message.md)
- [submit_message](../../../../../../functions/crates/lpe-storage/src/submission/Storage/submit_message.md)