---
type: Rust Function
title: task_input_from_mapi
resource: crates/lpe-exchange/src/mapi/properties/task.rs#L148-L198
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/streams/optional_pending_text_property
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/message_save/append_save_changes_message_route_response
  - functions/crates/lpe-exchange/src/mapi/properties/task/apply_canonical_task_property_values
  - functions/crates/lpe-exchange/src/mapi/tables/pending/serialize_pending_task_row
---

# Signature

`pub(in crate::mapi) fn task_input_from_mapi( account_id: Uuid, id: Option<Uuid>, existing: &ClientTask, collection_id: Option<&str>, properties: &HashMap<u32, MapiValue>, ) -> UpsertClientTaskInput`

# Calls

- [optional_pending_text_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/optional_pending_text_property.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [append_save_changes_message_route_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/message_save/append_save_changes_message_route_response.md)
- [apply_canonical_task_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/task/apply_canonical_task_property_values.md)
- [serialize_pending_task_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/pending/serialize_pending_task_row.md)