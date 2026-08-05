---
type: Rust Method
title: import_message_id
resource: crates/lpe-exchange/src/mapi/rop/parse.rs#L359-L375
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/import_property_values
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message/append_synchronization_import_message_change_response
  - functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/extend_access_plan_for_request
---

# Signature

`pub(in crate::mapi) fn import_message_id(&self) -> Option<u64>`

# Calls

- [import_property_values](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/import_property_values.md)

# Called by

- [append_synchronization_import_message_change_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message/append_synchronization_import_message_change_response.md)
- [extend_access_plan_for_request](../../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/extend_access_plan_for_request.md)