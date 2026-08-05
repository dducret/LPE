---
type: Rust Function
title: recoverable_mapi_folder_id
resource: crates/lpe-exchange/src/mapi_store.rs#L955-L962
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_recoverable_items
---

# Signature

`pub(crate) fn recoverable_mapi_folder_id(folder: &str) -> Option<u64>`

# Called by

- [with_recoverable_items](../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_recoverable_items.md)