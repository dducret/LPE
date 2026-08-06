---
type: Rust Function
title: task_collection_matches
resource: crates/lpe-exchange/src/mapi_store.rs#L1064-L1066
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/build
---

# Signature

`fn task_collection_matches(task: &ClientTask, collection_id: &str) -> bool`

# Called by

- [build](../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/build.md)