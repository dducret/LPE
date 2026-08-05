---
type: Rust Function
title: collection_deletes_as_moves
resource: crates/lpe-activesync/src/service/body_preferences.rs#L48-L56
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/text_value
  called_by:
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/apply_mail_sync_commands
---

# Signature

`pub(super) fn collection_deletes_as_moves(collection_node: &WbxmlNode) -> bool`

# Calls

- [text_value](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/text_value.md)

# Called by

- [apply_mail_sync_commands](../../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/apply_mail_sync_commands.md)