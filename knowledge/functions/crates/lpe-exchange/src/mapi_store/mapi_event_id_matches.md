---
type: Rust Function
title: mapi_event_id_matches
resource: crates/lpe-exchange/src/mapi_store.rs#L949-L951
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/event_for_id
---

# Signature

`fn mapi_event_id_matches(event: &MapiEvent, object_id: u64) -> bool`

# Called by

- [event_for_id](../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/event_for_id.md)