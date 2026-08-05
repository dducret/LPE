---
type: Rust Function
title: sync_object_projected_to_folder
resource: crates/lpe-exchange/src/mapi/sync.rs#L402-L408
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/sync/special_sync_objects_for
---

# Signature

`fn sync_object_projected_to_folder( mut object: mapi_mailstore::SpecialMessageSyncFact, folder_id: u64, ) -> mapi_mailstore::SpecialMessageSyncFact`

# Called by

- [special_sync_objects_for](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/special_sync_objects_for.md)