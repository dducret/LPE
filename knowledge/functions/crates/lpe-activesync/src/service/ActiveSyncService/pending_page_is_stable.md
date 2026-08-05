---
type: Rust Method
title: pending_page_is_stable
resource: crates/lpe-activesync/src/service.rs#L818-L861
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/fetch_collection_states_by_ids
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/sync_collection
---

# Signature

`async fn pending_page_is_stable( &self, account_id: Uuid, collection: &CollectionDefinition, target_state: &[CollectionStateEntry], page_changes: &[SnapshotChange], ) -> Result<bool>`

# Calls

- [fetch_collection_states_by_ids](../../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/fetch_collection_states_by_ids.md)
- [get](../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [sync_collection](../../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/sync_collection.md)