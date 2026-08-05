---
type: Rust Method
title: get_item_estimate_response
resource: crates/lpe-activesync/src/service/get_item_estimate.rs#L48-L106
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/text_value
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/resolve_collection
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/with_text
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/load_requested_sync_state
  - functions/crates/lpe-activesync/src/service/sync_helpers/decode_sync_state
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/collection_state
  - functions/crates/lpe-activesync/src/snapshot/diff_collection_states
  called_by:
  - functions/crates/lpe-activesync/src/service/get_item_estimate/ActiveSyncService/handle_get_item_estimate
---

# Signature

`async fn get_item_estimate_response( &self, principal: &AuthenticatedPrincipal, device_id: &str, collection_request: &WbxmlNode, ) -> Result<WbxmlNode>`

# Calls

- [text_value](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/text_value.md)
- [resolve_collection](../../../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/resolve_collection.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [with_text](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/with_text.md)
- [load_requested_sync_state](../../../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/load_requested_sync_state.md)
- [decode_sync_state](../../../../../../../functions/crates/lpe-activesync/src/service/sync_helpers/decode_sync_state.md)
- [collection_state](../../../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/collection_state.md)
- [diff_collection_states](../../../../../../../functions/crates/lpe-activesync/src/snapshot/diff_collection_states.md)

# Called by

- [handle_get_item_estimate](../../../../../../../functions/crates/lpe-activesync/src/service/get_item_estimate/ActiveSyncService/handle_get_item_estimate.md)