---
type: Rust Function
title: reserved_folder_id_for_role
resource: crates/lpe-exchange/src/mapi_store.rs#L1118-L1120
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/reserved_folder_counter_for_role
  called_by:
  - functions/crates/lpe-exchange/src/mapi_store/mapi_folder_id
  - functions/crates/lpe-exchange/src/mapi_store/mapi_folder_id_for_role
---

# Signature

`fn reserved_folder_id_for_role(role: &str) -> Option<u64>`

# Calls

- [reserved_folder_counter_for_role](../../../../../functions/crates/lpe-exchange/src/mapi_store/reserved_folder_counter_for_role.md)

# Called by

- [mapi_folder_id](../../../../../functions/crates/lpe-exchange/src/mapi_store/mapi_folder_id.md)
- [mapi_folder_id_for_role](../../../../../functions/crates/lpe-exchange/src/mapi_store/mapi_folder_id_for_role.md)