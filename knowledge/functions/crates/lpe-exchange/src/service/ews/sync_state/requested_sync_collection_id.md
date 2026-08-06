---
type: Rust Function
title: requested_sync_collection_id
resource: crates/lpe-exchange/src/service/ews/sync_state.rs#L730-L744
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/request_ids/requested_collection_id_in
  - functions/crates/lpe-exchange/src/service/ews/sync_state/requested_sync_state
  - functions/crates/lpe-exchange/src/service/ews/sync_state/collaboration_sync_state_collection_id
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/sync_state/ExchangeService/sync_folder_items
---

# Signature

`pub(in crate::service) fn requested_sync_collection_id( request: &str, kind: &str, default_id: &str, ) -> String`

# Calls

- [requested_collection_id_in](../../../../../../../functions/crates/lpe-exchange/src/service/ews/request_ids/requested_collection_id_in.md)
- [requested_sync_state](../../../../../../../functions/crates/lpe-exchange/src/service/ews/sync_state/requested_sync_state.md)
- [collaboration_sync_state_collection_id](../../../../../../../functions/crates/lpe-exchange/src/service/ews/sync_state/collaboration_sync_state_collection_id.md)

# Called by

- [sync_folder_items](../../../../../../../functions/crates/lpe-exchange/src/service/ews/sync_state/ExchangeService/sync_folder_items.md)