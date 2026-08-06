---
type: Rust Method
title: create_item
resource: crates/lpe-exchange/src/service/ews/items.rs#L552-L732
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/xml/element_content
  - functions/crates/lpe-exchange/src/service/ews/sharing/ExchangeService/accept_sharing_invitation
  - functions/crates/lpe-exchange/src/service/ews/request_ids/requested_collection_id_in
  - functions/crates/lpe-exchange/src/service/ews/contacts/parse_create_contact_input
  - functions/crates/lpe-exchange/src/service/ews/sync_state/contact_change_keys
  - functions/crates/lpe-exchange/src/service/ews/contacts/create_contact_success_response
  - functions/crates/lpe-exchange/src/service/ews/sync_state/change_key_for
  - functions/crates/lpe-exchange/src/service/ews/calendar/parse_create_event_input
  - functions/crates/lpe-exchange/src/service/ews/sync_state/event_change_keys
  - functions/crates/lpe-exchange/src/service/ews/calendar/create_event_success_response
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/create_accessible_task
  - functions/crates/lpe-exchange/src/service/ews/tasks/parse_create_task_input
  - functions/crates/lpe-exchange/src/service/ews/sync_state/task_change_keys
  - functions/crates/lpe-exchange/src/service/ews/tasks/create_task_success_response
  - functions/crates/lpe-exchange/src/service/ews/mail/parse_create_message_input
  - functions/crates/lpe-exchange/src/service/ews/xml/attribute_value_after
  - functions/crates/lpe-exchange/src/service/ews/request_ids/requested_public_folder_ids
  - functions/crates/lpe-core/src/sieve/Parser/next
  - functions/crates/lpe-exchange/src/service/ews/public_folders/create_public_folder_item_success_response
  - functions/crates/lpe-exchange/src/service/ews/mail/imported_email_input
  - functions/crates/lpe-exchange/src/service/ews/mail/create_item_success_response
  - functions/crates/lpe-exchange/src/service/ews/responses/operation_error_response
  - functions/crates/lpe-exchange/src/service/ews/responses/ews_error_code_or
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle
---

# Signature

`pub(in crate::service) async fn create_item( &self, principal: &AccountPrincipal, request: &str, ) -> Result<String>`

# Calls

- [element_content](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/element_content.md)
- [accept_sharing_invitation](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/sharing/ExchangeService/accept_sharing_invitation.md)
- [requested_collection_id_in](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/request_ids/requested_collection_id_in.md)
- [parse_create_contact_input](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/contacts/parse_create_contact_input.md)
- [contact_change_keys](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/sync_state/contact_change_keys.md)
- [create_contact_success_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/contacts/create_contact_success_response.md)
- [change_key_for](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/sync_state/change_key_for.md)
- [parse_create_event_input](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/calendar/parse_create_event_input.md)
- [event_change_keys](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/sync_state/event_change_keys.md)
- [create_event_success_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/calendar/create_event_success_response.md)
- [create_accessible_task](../../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/create_accessible_task.md)
- [parse_create_task_input](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/tasks/parse_create_task_input.md)
- [task_change_keys](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/sync_state/task_change_keys.md)
- [create_task_success_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/tasks/create_task_success_response.md)
- [parse_create_message_input](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/mail/parse_create_message_input.md)
- [attribute_value_after](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/attribute_value_after.md)
- [requested_public_folder_ids](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/request_ids/requested_public_folder_ids.md)
- [next](../../../../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)
- [create_public_folder_item_success_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/public_folders/create_public_folder_item_success_response.md)
- [imported_email_input](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/mail/imported_email_input.md)
- [create_item_success_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/mail/create_item_success_response.md)
- [operation_error_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/responses/operation_error_response.md)
- [ews_error_code_or](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/responses/ews_error_code_or.md)

# Called by

- [handle](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle.md)