---
type: Rust Function
title: append_rop_delete_messages
resource: crates/lpe-exchange/src/tests/mod.rs#L15885-L15891
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/append_mapi_wire_id
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_advertised_calendar_update_delete_uses_default_collection_event
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_task_crud_uses_canonical_tasks
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_common_views_delete_messages_deletes_navigation_shortcut
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_contact_crud_uses_canonical_contacts
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_shared_contact_read_only_rights_reject_mutations
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/hierarchy/mapi_over_http_delete_folder_local_default_named_view_is_noop_success
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/public_folders/mapi_over_http_public_folder_delete_message_deletes_canonical_post_item
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/tasks/mapi_over_http_shared_task_read_only_rights_reject_mutations
---

# Signature

`fn append_rop_delete_messages(rops: &mut Vec<u8>, input: u8, message_ids: &[u64])`

# Calls

- [append_mapi_wire_id](../../../../../functions/crates/lpe-exchange/src/tests/append_mapi_wire_id.md)

# Called by

- [mapi_over_http_advertised_calendar_update_delete_uses_default_collection_event](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_advertised_calendar_update_delete_uses_default_collection_event.md)
- [mapi_over_http_task_crud_uses_canonical_tasks](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_task_crud_uses_canonical_tasks.md)
- [mapi_over_http_common_views_delete_messages_deletes_navigation_shortcut](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_common_views_delete_messages_deletes_navigation_shortcut.md)
- [mapi_over_http_contact_crud_uses_canonical_contacts](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_contact_crud_uses_canonical_contacts.md)
- [mapi_over_http_shared_contact_read_only_rights_reject_mutations](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_shared_contact_read_only_rights_reject_mutations.md)
- [mapi_over_http_delete_folder_local_default_named_view_is_noop_success](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/hierarchy/mapi_over_http_delete_folder_local_default_named_view_is_noop_success.md)
- [mapi_over_http_public_folder_delete_message_deletes_canonical_post_item](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/public_folders/mapi_over_http_public_folder_delete_message_deletes_canonical_post_item.md)
- [mapi_over_http_shared_task_read_only_rights_reject_mutations](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/tasks/mapi_over_http_shared_task_read_only_rights_reject_mutations.md)