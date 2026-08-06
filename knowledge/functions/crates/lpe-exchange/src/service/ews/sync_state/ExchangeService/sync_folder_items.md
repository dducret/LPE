---
type: Rust Method
title: sync_folder_items
resource: crates/lpe-exchange/src/service/ews/sync_state.rs#L11-L442
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/folders/requested_folder_kind
  - functions/crates/lpe-exchange/src/service/ews/sync_state/requested_sync_collection_id
  - functions/crates/lpe-exchange/src/service/ews/sync_state/contact_change_keys
  - functions/crates/lpe-exchange/src/service/ews/sync_state/change_key_for
  - functions/crates/lpe-exchange/src/service/ews/sync_state/requested_sync_state
  - functions/crates/lpe-exchange/src/service/ews/sync_state/collaboration_sync_state_items
  - functions/crates/lpe-exchange/src/service/ews/sync_state/sync_state_items_by_id
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/service/ews/contacts/contact_item_xml_with_change_key
  - functions/crates/lpe-exchange/src/service/ews/sync_state/collaboration_sync_state
  - functions/crates/lpe-exchange/src/service/ews/sync_state/event_change_keys
  - functions/crates/lpe-exchange/src/service/ews/calendar/calendar_item_xml_with_change_key
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_accessible_tasks_in_collection
  - functions/crates/lpe-exchange/src/service/ews/sync_state/task_change_keys
  - functions/crates/lpe-exchange/src/service/ews/tasks/task_item_xml_with_change_key
  - functions/crates/lpe-core/src/sieve/Parser/next
  - functions/crates/lpe-exchange/src/service/ews/responses/sync_folder_items_response
  - functions/crates/lpe-exchange/src/service/ews/mail/message_change_key
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/service/ews/sync_state/requested_max_changes
  - functions/LPE-CT/web/app/smoke/test/MockClassList/remove
  - functions/crates/lpe-exchange/src/service/ews/request_ids/requested_public_folder_ids
  - functions/crates/lpe-exchange/src/service/ews/public_folders/public_folder_item_change_key
  - functions/crates/lpe-exchange/src/service/ews/sync_state/public_folder_sync_state_items
  - functions/crates/lpe-exchange/src/service/ews/public_folders/public_folder_item_summary_xml
  - functions/crates/lpe-exchange/src/service/ews/sync_state/public_folder_sync_state
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle
---

# Signature

`pub(in crate::service) async fn sync_folder_items( &self, principal: &AccountPrincipal, request: &str, ) -> Result<String>`

# Calls

- [requested_folder_kind](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/folders/requested_folder_kind.md)
- [requested_sync_collection_id](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/sync_state/requested_sync_collection_id.md)
- [contact_change_keys](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/sync_state/contact_change_keys.md)
- [change_key_for](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/sync_state/change_key_for.md)
- [requested_sync_state](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/sync_state/requested_sync_state.md)
- [collaboration_sync_state_items](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/sync_state/collaboration_sync_state_items.md)
- [sync_state_items_by_id](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/sync_state/sync_state_items_by_id.md)
- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [contact_item_xml_with_change_key](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/contacts/contact_item_xml_with_change_key.md)
- [collaboration_sync_state](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/sync_state/collaboration_sync_state.md)
- [event_change_keys](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/sync_state/event_change_keys.md)
- [calendar_item_xml_with_change_key](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/calendar/calendar_item_xml_with_change_key.md)
- [fetch_accessible_tasks_in_collection](../../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_accessible_tasks_in_collection.md)
- [task_change_keys](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/sync_state/task_change_keys.md)
- [task_item_xml_with_change_key](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/tasks/task_item_xml_with_change_key.md)
- [next](../../../../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)
- [sync_folder_items_response](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/responses/sync_folder_items_response.md)
- [message_change_key](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/mail/message_change_key.md)
- [push](../../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [requested_max_changes](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/sync_state/requested_max_changes.md)
- [remove](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockClassList/remove.md)
- [requested_public_folder_ids](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/request_ids/requested_public_folder_ids.md)
- [public_folder_item_change_key](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/public_folders/public_folder_item_change_key.md)
- [public_folder_sync_state_items](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/sync_state/public_folder_sync_state_items.md)
- [public_folder_item_summary_xml](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/public_folders/public_folder_item_summary_xml.md)
- [public_folder_sync_state](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/sync_state/public_folder_sync_state.md)

# Called by

- [handle](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/dispatch/ExchangeService/handle.md)