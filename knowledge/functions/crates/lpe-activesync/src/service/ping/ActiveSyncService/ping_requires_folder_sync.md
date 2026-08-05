---
type: Rust Method
title: ping_requires_folder_sync
resource: crates/lpe-activesync/src/service/ping.rs#L145-L164
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/current_hierarchy_generation
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/device_hierarchy_is_current
  called_by:
  - functions/crates/lpe-activesync/src/service/ping/ActiveSyncService/handle_ping
---

# Signature

`async fn ping_requires_folder_sync( &self, account_id: Uuid, device_id: &str, monitored: &[(CollectionDefinition, StoredSyncState)], ) -> Result<bool>`

# Calls

- [current_hierarchy_generation](../../../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/current_hierarchy_generation.md)
- [device_hierarchy_is_current](../../../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/device_hierarchy_is_current.md)

# Called by

- [handle_ping](../../../../../../../functions/crates/lpe-activesync/src/service/ping/ActiveSyncService/handle_ping.md)