---
type: Rust Method
title: record_deleted_advertised_special_folder
resource: crates/lpe-exchange/src/mapi/session.rs#L1018-L1020
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/append_delete_folder_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/deleted_advertised_quick_step_create_can_reuse_existing_real_folder
  - functions/crates/lpe-exchange/src/mapi/session/tests/session_remembers_deleted_advertised_special_folder
---

# Signature

`pub(in crate::mapi) fn record_deleted_advertised_special_folder(&mut self, folder_id: u64)`

# Called by

- [append_delete_folder_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/append_delete_folder_response.md)
- [deleted_advertised_quick_step_create_can_reuse_existing_real_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/deleted_advertised_quick_step_create_can_reuse_existing_real_folder.md)
- [session_remembers_deleted_advertised_special_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/tests/session_remembers_deleted_advertised_special_folder.md)