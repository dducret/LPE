---
type: Rust Function
title: mapi_folder_id_for_role
resource: crates/lpe-exchange/src/mapi_store.rs#L937-L939
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/reserved_folder_id_for_role
  called_by:
  - functions/crates/lpe-exchange/src/mapi_store/mapi_message_folder_id
---

# Signature

`fn mapi_folder_id_for_role(role: &str) -> u64`

# Calls

- [reserved_folder_id_for_role](../../../../../functions/crates/lpe-exchange/src/mapi_store/reserved_folder_id_for_role.md)

# Called by

- [mapi_message_folder_id](../../../../../functions/crates/lpe-exchange/src/mapi_store/mapi_message_folder_id.md)