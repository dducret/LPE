---
type: Rust Method
title: find_item
resource: crates/lpe-exchange/src/service/ews/items.rs#L150-L278
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/folders/requested_folder_kind
  - functions/crates/lpe-exchange/src/service/ews/responses/find_item_response
  - functions/crates/lpe-exchange/src/service/ews/request_ids/requested_collection_id
  - functions/crates/lpe-exchange/src/service/ews/sync_state/contact_change_keys
  - functions/crates/lpe-exchange/src/service/ews/contacts/contact_summary_xml_with_change_key
  - functions/crates/lpe-exchange/src/service/ews/sync_state/change_key_for
  - functions/crates/lpe-exchange/src/service/ews/sync_state/event_change_keys
  - functions/crates/lpe-exchange/src/service/ews/calendar/calendar_item_summary_xml_with_change_key
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_accessible_tasks_in_collection
  - functions/crates/lpe-exchange/src/service/ews/sync_state/task_change_keys
  - functions/crates/lpe-exchange/src/service/ews/tasks/task_item_summary_xml_with_change_key
  - functions/crates/lpe-core/src/sieve/Parser/next
  - functions/crates/lpe-exchange/src/service/ews/xml/attribute_value_after
  - functions/crates/lpe-exchange/src/service/ews/xml/ews_usize_attribute
  - functions/crates/lpe-exchange/src/service/ews/mail/message_summary_xml_for_mailbox
  - functions/crates/lpe-exchange/src/service/ews/responses/find_item_page_response
  - functions/crates/lpe-exchange/src/service/ews/request_ids/requested_public_folder_ids
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle
---

# Signature

`pub(in crate::service) async fn find_item( &self, principal: &AccountPrincipal, request: &str, ) -> Result<String>`

# Calls

- [requested_folder_kind](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/folders/requested_folder_kind.md)
- [find_item_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/responses/find_item_response.md)
- [requested_collection_id](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/request_ids/requested_collection_id.md)
- [contact_change_keys](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/sync_state/contact_change_keys.md)
- [contact_summary_xml_with_change_key](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/contacts/contact_summary_xml_with_change_key.md)
- [change_key_for](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/sync_state/change_key_for.md)
- [event_change_keys](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/sync_state/event_change_keys.md)
- [calendar_item_summary_xml_with_change_key](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/calendar/calendar_item_summary_xml_with_change_key.md)
- [fetch_accessible_tasks_in_collection](../../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_accessible_tasks_in_collection.md)
- [task_change_keys](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/sync_state/task_change_keys.md)
- [task_item_summary_xml_with_change_key](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/tasks/task_item_summary_xml_with_change_key.md)
- [next](../../../../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)
- [attribute_value_after](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/attribute_value_after.md)
- [ews_usize_attribute](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/ews_usize_attribute.md)
- [message_summary_xml_for_mailbox](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/mail/message_summary_xml_for_mailbox.md)
- [find_item_page_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/responses/find_item_page_response.md)
- [requested_public_folder_ids](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/request_ids/requested_public_folder_ids.md)

# Called by

- [handle](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle.md)