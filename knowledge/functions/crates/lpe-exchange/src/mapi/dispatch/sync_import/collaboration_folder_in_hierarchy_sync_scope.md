---
type: Rust Function
title: collaboration_folder_in_hierarchy_sync_scope
resource: crates/lpe-exchange/src/mapi/dispatch/sync_import.rs#L1116-L1119
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/sync_mailboxes_with_collaboration_counts
---

# Signature

`fn collaboration_folder_in_hierarchy_sync_scope(folder_id: u64, sync_root_folder_id: u64) -> bool`

# Called by

- [sync_mailboxes_with_collaboration_counts](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/sync_mailboxes_with_collaboration_counts.md)