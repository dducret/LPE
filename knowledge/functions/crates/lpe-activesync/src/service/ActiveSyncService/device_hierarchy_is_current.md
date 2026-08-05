---
type: Rust Method
title: device_hierarchy_is_current
resource: crates/lpe-activesync/src/service.rs#L630-L645
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str
  - functions/crates/lpe-activesync/src/service/sync_helpers/hierarchy_generation_from_snapshot
  called_by:
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/sync_collection
  - functions/crates/lpe-activesync/src/service/ping/ActiveSyncService/ping_requires_folder_sync
---

# Signature

`async fn device_hierarchy_is_current( &self, account_id: Uuid, device_id: &str, current_hierarchy_generation: &str, ) -> Result<bool>`

# Calls

- [from_str](../../../../../../functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str.md)
- [hierarchy_generation_from_snapshot](../../../../../../functions/crates/lpe-activesync/src/service/sync_helpers/hierarchy_generation_from_snapshot.md)

# Called by

- [sync_collection](../../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/sync_collection.md)
- [ping_requires_folder_sync](../../../../../../functions/crates/lpe-activesync/src/service/ping/ActiveSyncService/ping_requires_folder_sync.md)