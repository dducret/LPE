---
type: Rust Method
title: handle_move_item
resource: crates/lpe-activesync/src/service/move_items.rs#L42-L136
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/text_value
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/with_text
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/resolve_collection
  - functions/crates/lpe-activesync/src/snapshot/mail_collection
  - functions/crates/lpe-activesync/src/snapshot/parse_collection_mailbox_id
  called_by:
  - functions/crates/lpe-activesync/src/service/move_items/ActiveSyncService/handle_move_items
---

# Signature

`async fn handle_move_item( &self, principal: &AuthenticatedPrincipal, move_node: &WbxmlNode, ) -> Result<WbxmlNode>`

# Calls

- [text_value](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/text_value.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [with_text](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/with_text.md)
- [resolve_collection](../../../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/resolve_collection.md)
- [mail_collection](../../../../../../../functions/crates/lpe-activesync/src/snapshot/mail_collection.md)
- [parse_collection_mailbox_id](../../../../../../../functions/crates/lpe-activesync/src/snapshot/parse_collection_mailbox_id.md)

# Called by

- [handle_move_items](../../../../../../../functions/crates/lpe-activesync/src/service/move_items/ActiveSyncService/handle_move_items.md)