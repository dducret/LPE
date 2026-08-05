---
type: Rust Function
title: collaboration_sync_state
resource: crates/lpe-exchange/src/service/ews/sync_state.rs#L513-L528
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/sync_state/ExchangeService/sync_folder_items
---

# Signature

`pub(in crate::service) fn collaboration_sync_state( kind: &str, collection_id: &str, items: &[(Uuid, String)], ) -> String`

# Called by

- [sync_folder_items](../../../../../../../functions/crates/lpe-exchange/src/service/ews/sync_state/ExchangeService/sync_folder_items.md)