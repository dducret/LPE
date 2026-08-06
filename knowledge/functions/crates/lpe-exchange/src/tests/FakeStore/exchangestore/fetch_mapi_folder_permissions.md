---
type: Rust Method
title: fetch_mapi_folder_permissions
resource: crates/lpe-exchange/src/tests/mod.rs#L7231-L7254
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/permissions/owner_permission
  called_by:
  - functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_store_for_access_plan
  - functions/crates/lpe-exchange/src/mapi_store/T/mapistore/load_mapi_mail_store
---

# Signature

`fn fetch_mapi_folder_permissions<'a>( &'a self, account_id: Uuid, mailbox_ids: &'a [Uuid], ) -> StoreFuture<'a, Vec<MapiFolderPermission>>`

# Calls

- [owner_permission](../../../../../../../functions/crates/lpe-exchange/src/mapi/permissions/owner_permission.md)

# Called by

- [load_mapi_store_for_access_plan](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_store_for_access_plan.md)
- [load_mapi_mail_store](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/T/mapistore/load_mapi_mail_store.md)