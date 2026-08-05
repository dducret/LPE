---
type: Rust Function
title: try_mapi_folder_id_for_role
resource: crates/lpe-exchange/src/mapi/tables/folders.rs#L70-L111
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/folders/try_mapi_folder_id
  - functions/crates/lpe-exchange/src/mapi/tables/folders/mapi_folder_id_for_email
---

# Signature

`fn try_mapi_folder_id_for_role(role: &str) -> Option<u64>`

# Called by

- [try_mapi_folder_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/try_mapi_folder_id.md)
- [mapi_folder_id_for_email](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/mapi_folder_id_for_email.md)