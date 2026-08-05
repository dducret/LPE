---
type: Rust Method
title: apply_mail_sync_commands
resource: crates/lpe-activesync/src/service.rs#L891-L1010
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/snapshot/parse_collection_mailbox_id
  - functions/crates/lpe-activesync/src/service/body_preferences/collection_deletes_as_moves
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/text_value
  - functions/crates/lpe-activesync/src/service/application_data/mail_flag_update
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/with_text
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/trash_collection
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/hard_delete_mail_command
  called_by:
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/sync_collection
---

# Signature

`async fn apply_mail_sync_commands( &self, principal: &AuthenticatedPrincipal, collection: &CollectionDefinition, collection_node: &WbxmlNode, ) -> Result<Vec<WbxmlNode>>`

# Calls

- [parse_collection_mailbox_id](../../../../../../functions/crates/lpe-activesync/src/snapshot/parse_collection_mailbox_id.md)
- [collection_deletes_as_moves](../../../../../../functions/crates/lpe-activesync/src/service/body_preferences/collection_deletes_as_moves.md)
- [text_value](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/text_value.md)
- [mail_flag_update](../../../../../../functions/crates/lpe-activesync/src/service/application_data/mail_flag_update.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [with_text](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/with_text.md)
- [trash_collection](../../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/trash_collection.md)
- [hard_delete_mail_command](../../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/hard_delete_mail_command.md)

# Called by

- [sync_collection](../../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/sync_collection.md)