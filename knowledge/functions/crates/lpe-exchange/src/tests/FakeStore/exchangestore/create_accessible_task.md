---
type: Rust Method
title: create_accessible_task
resource: crates/lpe-exchange/src/tests/mod.rs#L9278-L9309
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/FakeStore/rights
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/message_save/append_save_changes_message_route_response
  - functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/create_item
---

# Signature

`fn create_accessible_task<'a>( &'a self, principal_account_id: Uuid, input: UpsertClientTaskInput, ) -> StoreFuture<'a, ClientTask>`

# Calls

- [rights](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/rights.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [append_save_changes_message_route_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/message_save/append_save_changes_message_route_response.md)
- [create_item](../../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/create_item.md)