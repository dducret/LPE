---
type: Rust Method
title: validate_mutating_item_change_keys
resource: crates/lpe-exchange/src/service/ews/items.rs#L1421-L1536
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/request_ids/requested_item_references
  - functions/crates/lpe-core/src/sieve/Parser/next
  - functions/crates/lpe-exchange/src/service/ews/items/validate_supplied_item_change_key
  - functions/crates/lpe-exchange/src/service/ews/mail/message_change_key
  - functions/crates/lpe-exchange/src/service/ews/sync_state/contact_change_keys
  - functions/crates/lpe-exchange/src/service/ews/sync_state/change_key_for
  - functions/crates/lpe-exchange/src/service/ews/sync_state/event_change_keys
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_accessible_tasks_by_ids
  - functions/crates/lpe-exchange/src/service/ews/sync_state/task_change_keys
  - functions/crates/lpe-exchange/src/service/ews/public_folders/public_folder_item_change_key
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/attachments/ExchangeService/create_attachment
  - functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/update_item
  - functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/send_item
  - functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/archive_item
  - functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/copy_item
  - functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/move_item
  - functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/delete_item
  - functions/crates/lpe-exchange/src/service/ews/mail/ExchangeService/mark_as_junk
---

# Signature

`pub(in crate::service) async fn validate_mutating_item_change_keys( &self, principal: &AccountPrincipal, request: &str, ) -> Result<()>`

# Calls

- [requested_item_references](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/request_ids/requested_item_references.md)
- [next](../../../../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)
- [validate_supplied_item_change_key](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/items/validate_supplied_item_change_key.md)
- [message_change_key](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/mail/message_change_key.md)
- [contact_change_keys](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/sync_state/contact_change_keys.md)
- [change_key_for](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/sync_state/change_key_for.md)
- [event_change_keys](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/sync_state/event_change_keys.md)
- [fetch_accessible_tasks_by_ids](../../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_accessible_tasks_by_ids.md)
- [task_change_keys](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/sync_state/task_change_keys.md)
- [public_folder_item_change_key](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/public_folders/public_folder_item_change_key.md)

# Called by

- [create_attachment](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/attachments/ExchangeService/create_attachment.md)
- [update_item](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/update_item.md)
- [send_item](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/send_item.md)
- [archive_item](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/archive_item.md)
- [copy_item](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/copy_item.md)
- [move_item](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/move_item.md)
- [delete_item](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/delete_item.md)
- [mark_as_junk](../../../../../../../../functions/crates/lpe-exchange/src/service/ews/mail/ExchangeService/mark_as_junk.md)