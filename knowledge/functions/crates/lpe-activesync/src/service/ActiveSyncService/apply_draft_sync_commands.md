---
type: Rust Method
title: apply_draft_sync_commands
resource: crates/lpe-activesync/src/service.rs#L1045-L1172
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/mailbox_access_for_account
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/text_value
  - functions/crates/lpe-activesync/src/message/draft_input_from_application_data
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/with_text
  - functions/crates/lpe-activesync/src/message/merged_draft_input
  called_by:
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/sync_collection
---

# Signature

`async fn apply_draft_sync_commands( &self, principal: &AuthenticatedPrincipal, collection: &CollectionDefinition, collection_node: &WbxmlNode, ) -> Result<Vec<WbxmlNode>>`

# Calls

- [mailbox_access_for_account](../../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/mailbox_access_for_account.md)
- [text_value](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/text_value.md)
- [draft_input_from_application_data](../../../../../../functions/crates/lpe-activesync/src/message/draft_input_from_application_data.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [with_text](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/with_text.md)
- [merged_draft_input](../../../../../../functions/crates/lpe-activesync/src/message/merged_draft_input.md)

# Called by

- [sync_collection](../../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/sync_collection.md)