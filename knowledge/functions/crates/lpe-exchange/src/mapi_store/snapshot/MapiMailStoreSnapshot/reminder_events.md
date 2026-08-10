---
type: Rust Method
title: reminder_events
resource: crates/lpe-exchange/src/mapi_store/snapshot.rs#L1110-L1126
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/search_folder_definition_for_role
  called_by:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/event_for_id
---

# Signature

`pub(crate) fn reminder_events(&self) -> Vec<&MapiEvent>`

# Calls

- [search_folder_definition_for_role](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/search_folder_definition_for_role.md)

# Called by

- [event_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/event_for_id.md)