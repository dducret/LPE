---
type: Rust Function
title: collaboration_sync_state_items
resource: crates/lpe-exchange/src/service/ews/sync_state.rs#L632-L668
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/sync_state/ExchangeService/sync_folder_items
  - functions/crates/lpe-exchange/src/service/ews/sync_state/public_folder_sync_state_items
---

# Signature

`pub(in crate::service) fn collaboration_sync_state_items( sync_state: &str, kind: &str, collection_id: &str, ) -> CollaborationSyncState`

# Called by

- [sync_folder_items](../../../../../../../functions/crates/lpe-exchange/src/service/ews/sync_state/ExchangeService/sync_folder_items.md)
- [public_folder_sync_state_items](../../../../../../../functions/crates/lpe-exchange/src/service/ews/sync_state/public_folder_sync_state_items.md)