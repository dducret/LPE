---
type: Rust Function
title: owner_permission
resource: crates/lpe-exchange/src/mapi/permissions.rs#L46-L56
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/permissions/owner_rights
  called_by:
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_folder_permissions
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/set_mapi_folder_permission
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/set_mapi_calendar_permission
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/set_mapi_calendar_collection_permission
---

# Signature

`pub(crate) fn owner_permission( mailbox_id: Uuid, principal: &AccountPrincipal, ) -> MapiFolderPermission`

# Calls

- [owner_rights](../../../../../../functions/crates/lpe-exchange/src/mapi/permissions/owner_rights.md)

# Called by

- [fetch_mapi_folder_permissions](../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_folder_permissions.md)
- [set_mapi_folder_permission](../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/set_mapi_folder_permission.md)
- [set_mapi_calendar_permission](../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/set_mapi_calendar_permission.md)
- [set_mapi_calendar_collection_permission](../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/set_mapi_calendar_collection_permission.md)