---
type: Rust Function
title: cleanup_blocker_labels
resource: crates/lpe-storage/src/storage_visibility.rs#L1009-L1036
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-storage/src/storage_visibility/Storage/cleanup_blockers_for_row
  - functions/crates/lpe-storage/src/storage_visibility/tests/cleanup_blockers_are_reported_without_internal_ids
---

# Signature

`fn cleanup_blocker_labels(state: CleanupBlockerState) -> Vec<String>`

# Calls

- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [cleanup_blockers_for_row](../../../../../functions/crates/lpe-storage/src/storage_visibility/Storage/cleanup_blockers_for_row.md)
- [cleanup_blockers_are_reported_without_internal_ids](../../../../../functions/crates/lpe-storage/src/storage_visibility/tests/cleanup_blockers_are_reported_without_internal_ids.md)