---
type: Rust Function
title: parse_activesync_file_reference
resource: crates/lpe-storage/src/util.rs#L169-L180
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/Parser/next
  called_by:
  - functions/crates/lpe-storage/src/activesync/Storage/fetch_activesync_attachment_content
  - functions/crates/lpe-storage/src/attachments/Storage/delete_message_attachment
---

# Signature

`pub(crate) fn parse_activesync_file_reference(value: &str) -> Option<(Uuid, Uuid)>`

# Calls

- [next](../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)

# Called by

- [fetch_activesync_attachment_content](../../../../../functions/crates/lpe-storage/src/activesync/Storage/fetch_activesync_attachment_content.md)
- [delete_message_attachment](../../../../../functions/crates/lpe-storage/src/attachments/Storage/delete_message_attachment.md)