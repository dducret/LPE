---
type: Rust Function
title: sync_folder_items_response
resource: crates/lpe-exchange/src/service/ews/responses.rs#L276-L293
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/sync_state/ExchangeService/sync_folder_items
---

# Signature

`pub(in crate::service) fn sync_folder_items_response(sync_state: &str, changes: String) -> String`

# Called by

- [sync_folder_items](../../../../../../../functions/crates/lpe-exchange/src/service/ews/sync_state/ExchangeService/sync_folder_items.md)