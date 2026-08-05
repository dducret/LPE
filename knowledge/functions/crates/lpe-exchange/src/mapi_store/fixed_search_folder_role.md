---
type: Rust Function
title: fixed_search_folder_role
resource: crates/lpe-exchange/src/mapi_store.rs#L310-L318
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/search_folder_definition_for_folder_id
---

# Signature

`fn fixed_search_folder_role(folder_id: u64) -> Option<&'static str>`

# Called by

- [search_folder_definition_for_folder_id](../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/search_folder_definition_for_folder_id.md)