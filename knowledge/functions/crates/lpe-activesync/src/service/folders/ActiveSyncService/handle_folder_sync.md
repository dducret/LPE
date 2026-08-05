---
type: Rust Method
title: handle_folder_sync
resource: crates/lpe-activesync/src/service/folders.rs#L21-L118
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/service/command_status_response
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/text_value
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/folder_collections
  - functions/crates/lpe-activesync/src/service/folders/folder_hierarchy_snapshot
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/load_requested_sync_state
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/with_text
  - functions/crates/lpe-activesync/src/response/wbxml_response
  - functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str
  - functions/crates/lpe-activesync/src/snapshot/diff_snapshots
  - functions/crates/lpe-activesync/src/service/folders/push_folder_metadata
  called_by:
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/handle_parsed_request
---

# Signature

`pub(super) async fn handle_folder_sync( &self, principal: &AuthenticatedPrincipal, device_id: &str, protocol_version: &str, request: &WbxmlNode, ) -> Result<Response>`

# Calls

- [command_status_response](../../../../../../../functions/crates/lpe-activesync/src/service/command_status_response.md)
- [text_value](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/text_value.md)
- [folder_collections](../../../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/folder_collections.md)
- [folder_hierarchy_snapshot](../../../../../../../functions/crates/lpe-activesync/src/service/folders/folder_hierarchy_snapshot.md)
- [load_requested_sync_state](../../../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/load_requested_sync_state.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [with_text](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/with_text.md)
- [wbxml_response](../../../../../../../functions/crates/lpe-activesync/src/response/wbxml_response.md)
- [from_str](../../../../../../../functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str.md)
- [diff_snapshots](../../../../../../../functions/crates/lpe-activesync/src/snapshot/diff_snapshots.md)
- [push_folder_metadata](../../../../../../../functions/crates/lpe-activesync/src/service/folders/push_folder_metadata.md)

# Called by

- [handle_parsed_request](../../../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/handle_parsed_request.md)