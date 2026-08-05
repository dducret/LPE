---
type: Rust Function
title: contact_change_keys
resource: crates/lpe-exchange/src/service/ews/sync_state.rs#L395-L427
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_contact_sync_versions
  - functions/crates/lpe-exchange/src/service/ews/contacts/contact_change_key
  - functions/crates/lpe-exchange/src/service/ews/sync_state/change_key_version
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/get_item
  - functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/find_item
  - functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/update_item
  - functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/create_item
  - functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/validate_mutating_item_change_keys
  - functions/crates/lpe-exchange/src/service/ews/sync_state/ExchangeService/sync_folder_items
---

# Signature

`pub(in crate::service) async fn contact_change_keys<S>( store: &S, principal_account_id: Uuid, contacts: &[AccessibleContact], ) -> Result<HashMap<Uuid, String>> where S: ExchangeStore + ?Sized,`

# Calls

- [fetch_contact_sync_versions](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_contact_sync_versions.md)
- [contact_change_key](../../../../../../../functions/crates/lpe-exchange/src/service/ews/contacts/contact_change_key.md)
- [change_key_version](../../../../../../../functions/crates/lpe-exchange/src/service/ews/sync_state/change_key_version.md)

# Called by

- [get_item](../../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/get_item.md)
- [find_item](../../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/find_item.md)
- [update_item](../../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/update_item.md)
- [create_item](../../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/create_item.md)
- [validate_mutating_item_change_keys](../../../../../../../functions/crates/lpe-exchange/src/service/ews/items/ExchangeService/validate_mutating_item_change_keys.md)
- [sync_folder_items](../../../../../../../functions/crates/lpe-exchange/src/service/ews/sync_state/ExchangeService/sync_folder_items.md)