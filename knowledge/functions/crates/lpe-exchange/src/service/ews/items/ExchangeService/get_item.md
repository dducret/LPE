---
type: Rust Method
title: get_item
resource: crates/lpe-exchange/src/service/ews/items.rs#L8-L148
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/mail/requested_mime_content
  - functions/crates/lpe-exchange/src/service/ews/request_ids/requested_item_ids
  - functions/crates/lpe-exchange/src/service/ews/sync_state/contact_change_keys
  - functions/crates/lpe-exchange/src/service/ews/contacts/contact_item_xml_with_change_key
  - functions/crates/lpe-exchange/src/service/ews/sync_state/change_key_for
  - functions/crates/lpe-exchange/src/service/ews/sync_state/event_change_keys
  - functions/crates/lpe-exchange/src/service/ews/calendar/calendar_item_xml_with_change_key
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_accessible_tasks_by_ids
  - functions/crates/lpe-exchange/src/service/ews/sync_state/task_change_keys
  - functions/crates/lpe-exchange/src/service/ews/tasks/task_item_xml_with_change_key
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_message_attachments
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_attachment_content
  - functions/crates/lpe-exchange/src/service/ews/responses/get_item_error_response
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/service/ews/mail/message_item_xml_with_details
  - functions/crates/lpe-exchange/src/service/ews/public_folders/public_folder_item_xml
  - functions/crates/lpe-exchange/src/service/ews/xml/count_tag_occurrences
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle
---

# Signature

`pub(in crate::service) async fn get_item( &self, principal: &AccountPrincipal, request: &str, ) -> Result<String>`

# Calls

- [requested_mime_content](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/mail/requested_mime_content.md)
- [requested_item_ids](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/request_ids/requested_item_ids.md)
- [contact_change_keys](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/sync_state/contact_change_keys.md)
- [contact_item_xml_with_change_key](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/contacts/contact_item_xml_with_change_key.md)
- [change_key_for](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/sync_state/change_key_for.md)
- [event_change_keys](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/sync_state/event_change_keys.md)
- [calendar_item_xml_with_change_key](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/calendar/calendar_item_xml_with_change_key.md)
- [fetch_accessible_tasks_by_ids](../../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_accessible_tasks_by_ids.md)
- [task_change_keys](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/sync_state/task_change_keys.md)
- [task_item_xml_with_change_key](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/tasks/task_item_xml_with_change_key.md)
- [fetch_message_attachments](../../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_message_attachments.md)
- [fetch_attachment_content](../../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_attachment_content.md)
- [get_item_error_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/responses/get_item_error_response.md)
- [push](../../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [message_item_xml_with_details](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/mail/message_item_xml_with_details.md)
- [public_folder_item_xml](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/public_folders/public_folder_item_xml.md)
- [count_tag_occurrences](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/xml/count_tag_occurrences.md)

# Called by

- [handle](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle.md)