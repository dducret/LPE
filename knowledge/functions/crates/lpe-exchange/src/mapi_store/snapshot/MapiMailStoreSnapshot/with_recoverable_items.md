---
type: Rust Method
title: with_recoverable_items
resource: crates/lpe-exchange/src/mapi_store/snapshot.rs#L203-L220
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/recoverable_mapi_folder_id
  - functions/crates/lpe-exchange/src/mapi_store/mapi_recoverable_item_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_store_for_access_plan
  - functions/crates/lpe-exchange/src/mapi_store/T/mapistore/load_mapi_mail_store
---

# Signature

`pub(crate) fn with_recoverable_items( mut self, recoverable_items: Vec<RecoverableItem>, ) -> Self`

# Calls

- [recoverable_mapi_folder_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/recoverable_mapi_folder_id.md)
- [mapi_recoverable_item_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/mapi_recoverable_item_id.md)

# Called by

- [load_mapi_store_for_access_plan](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_store_for_access_plan.md)
- [load_mapi_mail_store](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/T/mapistore/load_mapi_mail_store.md)