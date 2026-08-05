---
type: Rust Function
title: special_folder_is_in_sync_scope
resource: crates/lpe-exchange/src/mapi/sync/scope.rs#L265-L294
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/sync/scope/sync_mailboxes_for_excluding_deleted
---

# Signature

`fn special_folder_is_in_sync_scope(special_folder_id: u64, sync_root_folder_id: u64) -> bool`

# Called by

- [sync_mailboxes_for_excluding_deleted](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/scope/sync_mailboxes_for_excluding_deleted.md)