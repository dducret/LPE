---
type: Rust Method
title: delete_accessible_task
resource: crates/lpe-exchange/src/tests/mod.rs#L9395-L9403
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_delete_messages_response
  - functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/delete_item
---

# Signature

`fn delete_accessible_task<'a>( &'a self, _principal_account_id: Uuid, task_id: Uuid, ) -> StoreFuture<'a, ()>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [append_delete_messages_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_delete_messages_response.md)
- [delete_item](../../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/delete_item.md)