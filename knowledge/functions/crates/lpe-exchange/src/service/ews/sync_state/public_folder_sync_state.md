---
type: Rust Function
title: public_folder_sync_state
resource: crates/lpe-exchange/src/service/ews/sync_state.rs#L602-L613
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/sync_state/ExchangeService/sync_folder_items
---

# Signature

`fn public_folder_sync_state(collection_id: &str, items: &[(Uuid, String, bool)]) -> String`

# Called by

- [sync_folder_items](../../../../../../../functions/crates/lpe-exchange/src/service/ews/sync_state/ExchangeService/sync_folder_items.md)