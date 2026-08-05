---
type: Rust Method
title: changed_ping_collections
resource: crates/lpe-activesync/src/service/ping.rs#L166-L182
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/collection_state
  - functions/crates/lpe-activesync/src/snapshot/diff_collection_states
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-activesync/src/service/ping/ActiveSyncService/handle_ping
---

# Signature

`async fn changed_ping_collections( &self, account_id: Uuid, monitored: &[(CollectionDefinition, StoredSyncState)], ) -> Result<Vec<String>>`

# Calls

- [collection_state](../../../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/collection_state.md)
- [diff_collection_states](../../../../../../../functions/crates/lpe-activesync/src/snapshot/diff_collection_states.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [handle_ping](../../../../../../../functions/crates/lpe-activesync/src/service/ping/ActiveSyncService/handle_ping.md)