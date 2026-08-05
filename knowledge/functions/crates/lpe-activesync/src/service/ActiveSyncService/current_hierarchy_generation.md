---
type: Rust Method
title: current_hierarchy_generation
resource: crates/lpe-activesync/src/service.rs#L624-L628
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/service/sync_helpers/hierarchy_generation
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/folder_collections
  called_by:
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/sync_collection
  - functions/crates/lpe-activesync/src/service/ping/ActiveSyncService/ping_requires_folder_sync
---

# Signature

`async fn current_hierarchy_generation(&self, account_id: Uuid) -> Result<String>`

# Calls

- [hierarchy_generation](../../../../../../functions/crates/lpe-activesync/src/service/sync_helpers/hierarchy_generation.md)
- [folder_collections](../../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/folder_collections.md)

# Called by

- [sync_collection](../../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/sync_collection.md)
- [ping_requires_folder_sync](../../../../../../functions/crates/lpe-activesync/src/service/ping/ActiveSyncService/ping_requires_folder_sync.md)