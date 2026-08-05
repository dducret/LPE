---
type: Rust Method
title: apply_calendar_sync_commands
resource: crates/lpe-activesync/src/service.rs#L1269-L1362
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/text_value
  - functions/crates/lpe-activesync/src/service/application_data/parse_event_input
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/with_text
  - functions/crates/lpe-core/src/sieve/Parser/next
  called_by:
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/sync_collection
---

# Signature

`async fn apply_calendar_sync_commands( &self, principal: &AuthenticatedPrincipal, collection_node: &WbxmlNode, ) -> Result<Vec<WbxmlNode>>`

# Calls

- [text_value](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/text_value.md)
- [parse_event_input](../../../../../../functions/crates/lpe-activesync/src/service/application_data/parse_event_input.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [with_text](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/with_text.md)
- [next](../../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)

# Called by

- [sync_collection](../../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/sync_collection.md)