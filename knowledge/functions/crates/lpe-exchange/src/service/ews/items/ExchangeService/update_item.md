---
type: Rust Method
title: update_item
resource: crates/lpe-exchange/src/service/ews/items.rs#L280-L520
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/validate_mutating_item_change_keys
  - functions/crates/lpe-exchange/src/service/ews/request_ids/requested_item_references
  - functions/crates/lpe-exchange/src/service/ews/responses/operation_error_response
  - functions/crates/lpe-exchange/src/service/ews/mail/parse_update_message_flags
  - functions/crates/lpe-core/src/sieve/Parser/next
  - functions/crates/lpe-exchange/src/service/ews/items/validate_supplied_item_change_key
  - functions/crates/lpe-exchange/src/service/ews/mail/message_change_key
  - functions/crates/lpe-exchange/src/service/ews/mail/message_item_xml
  - functions/crates/lpe-exchange/src/service/ews/sync_state/contact_change_keys
  - functions/crates/lpe-exchange/src/service/ews/sync_state/change_key_for
  - functions/crates/lpe-exchange/src/service/ews/contacts/parse_update_contact_input
  - functions/crates/lpe-exchange/src/service/ews/contacts/contact_item_xml_with_change_key
  - functions/crates/lpe-exchange/src/service/ews/sync_state/event_change_keys
  - functions/crates/lpe-exchange/src/service/ews/calendar/parse_update_event_input
  - functions/crates/lpe-exchange/src/service/ews/calendar/calendar_item_xml_with_change_key
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_accessible_tasks_by_ids
  - functions/crates/lpe-exchange/src/service/ews/sync_state/task_change_keys
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/update_accessible_task
  - functions/crates/lpe-exchange/src/service/ews/tasks/parse_update_task_input
  - functions/crates/lpe-exchange/src/service/ews/tasks/task_item_xml_with_change_key
  - functions/crates/lpe-exchange/src/service/ews/public_folders/public_folder_item_change_key
  - functions/crates/lpe-exchange/src/service/ews/public_folders/parse_update_public_folder_item_input
  - functions/crates/lpe-exchange/src/service/ews/public_folders/public_folder_item_xml
  - functions/crates/lpe-exchange/src/service/ews/responses/update_item_success_response
  - functions/crates/lpe-exchange/src/service/ews/responses/ews_error_code_or
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle
---

# Signature

`pub(in crate::service) async fn update_item( &self, principal: &AccountPrincipal, request: &str, ) -> Result<String>`

# Calls

- [validate_mutating_item_change_keys](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/validate_mutating_item_change_keys.md)
- [requested_item_references](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/request_ids/requested_item_references.md)
- [operation_error_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/responses/operation_error_response.md)
- [parse_update_message_flags](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/mail/parse_update_message_flags.md)
- [next](../../../../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)
- [validate_supplied_item_change_key](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/items/validate_supplied_item_change_key.md)
- [message_change_key](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/mail/message_change_key.md)
- [message_item_xml](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/mail/message_item_xml.md)
- [contact_change_keys](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/sync_state/contact_change_keys.md)
- [change_key_for](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/sync_state/change_key_for.md)
- [parse_update_contact_input](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/contacts/parse_update_contact_input.md)
- [contact_item_xml_with_change_key](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/contacts/contact_item_xml_with_change_key.md)
- [event_change_keys](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/sync_state/event_change_keys.md)
- [parse_update_event_input](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/calendar/parse_update_event_input.md)
- [calendar_item_xml_with_change_key](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/calendar/calendar_item_xml_with_change_key.md)
- [fetch_accessible_tasks_by_ids](../../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_accessible_tasks_by_ids.md)
- [task_change_keys](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/sync_state/task_change_keys.md)
- [update_accessible_task](../../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/update_accessible_task.md)
- [parse_update_task_input](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/tasks/parse_update_task_input.md)
- [task_item_xml_with_change_key](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/tasks/task_item_xml_with_change_key.md)
- [public_folder_item_change_key](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/public_folders/public_folder_item_change_key.md)
- [parse_update_public_folder_item_input](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/public_folders/parse_update_public_folder_item_input.md)
- [public_folder_item_xml](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/public_folders/public_folder_item_xml.md)
- [update_item_success_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/responses/update_item_success_response.md)
- [ews_error_code_or](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/responses/ews_error_code_or.md)

# Called by

- [handle](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle.md)