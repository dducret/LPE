---
type: Rust Function
title: sync_collection_has_unsupported_command
resource: crates/lpe-activesync/src/service/sync_helpers.rs#L60-L71
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/service/sync_helpers/sync_command_supported_for_collection
  called_by:
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/sync_collection
---

# Signature

`pub(super) fn sync_collection_has_unsupported_command( collection_node: &WbxmlNode, collection: &CollectionDefinition, ) -> bool`

# Calls

- [sync_command_supported_for_collection](../../../../../../functions/crates/lpe-activesync/src/service/sync_helpers/sync_command_supported_for_collection.md)

# Called by

- [sync_collection](../../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/sync_collection.md)