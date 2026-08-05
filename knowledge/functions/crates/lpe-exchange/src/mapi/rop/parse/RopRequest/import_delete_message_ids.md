---
type: Rust Method
title: import_delete_message_ids
resource: crates/lpe-exchange/src/mapi/rop/parse.rs#L412-L417
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/import_delete_source_keys
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_deletes/append_synchronization_import_deletes_response
  - functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/extend_access_plan_for_request
---

# Signature

`pub(in crate::mapi) fn import_delete_message_ids(&self) -> Vec<u64>`

# Calls

- [import_delete_source_keys](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/import_delete_source_keys.md)

# Called by

- [append_synchronization_import_deletes_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_deletes/append_synchronization_import_deletes_response.md)
- [extend_access_plan_for_request](../../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/extend_access_plan_for_request.md)