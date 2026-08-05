---
type: Rust Function
title: email_role_folder_id
resource: crates/lpe-exchange/src/mapi/dispatch/folders.rs#L1439-L1442
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/reserved_folder_counter_for_role
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/snapshot_email_belongs_to_folder
---

# Signature

`fn email_role_folder_id(role: &str) -> Option<u64>`

# Calls

- [reserved_folder_counter_for_role](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/reserved_folder_counter_for_role.md)

# Called by

- [snapshot_email_belongs_to_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/snapshot_email_belongs_to_folder.md)