---
type: Rust Function
title: drafts_collection
resource: crates/lpe-activesync/src/snapshot.rs#L619-L621
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/sync_collection
  - functions/crates/lpe-activesync/src/service/sync_helpers/sync_command_supported_for_collection
---

# Signature

`pub(crate) fn drafts_collection(collection: &CollectionDefinition) -> bool`

# Called by

- [sync_collection](../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/sync_collection.md)
- [sync_command_supported_for_collection](../../../../../functions/crates/lpe-activesync/src/service/sync_helpers/sync_command_supported_for_collection.md)