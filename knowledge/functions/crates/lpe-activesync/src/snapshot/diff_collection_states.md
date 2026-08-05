---
type: Rust Function
title: diff_collection_states
resource: crates/lpe-activesync/src/snapshot.rs#L495-L534
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/sync_collection
  - functions/crates/lpe-activesync/src/service/get_item_estimate/ActiveSyncService/get_item_estimate_response
  - functions/crates/lpe-activesync/src/service/ping/ActiveSyncService/changed_ping_collections
---

# Signature

`pub(crate) fn diff_collection_states( previous: &[CollectionStateEntry], current: &[CollectionStateEntry], ) -> Vec<SnapshotChange>`

# Calls

- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [sync_collection](../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/sync_collection.md)
- [get_item_estimate_response](../../../../../functions/crates/lpe-activesync/src/service/get_item_estimate/ActiveSyncService/get_item_estimate_response.md)
- [changed_ping_collections](../../../../../functions/crates/lpe-activesync/src/service/ping/ActiveSyncService/changed_ping_collections.md)