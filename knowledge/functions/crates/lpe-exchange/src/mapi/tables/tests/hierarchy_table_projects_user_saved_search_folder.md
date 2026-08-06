---
type: Rust Function
title: hierarchy_table_projects_user_saved_search_folder
resource: crates/lpe-exchange/src/mapi/tables/tests.rs#L3344-L3420
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_search_folder_definitions
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/mailboxes
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_rows
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_row_id
  - functions/crates/lpe-core/src/sieve/Parser/expect
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/serialize_hierarchy_row
---

# Signature

`fn hierarchy_table_projects_user_saved_search_folder()`

# Calls

- [remember_mapi_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity.md)
- [with_search_folder_definitions](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_search_folder_definitions.md)
- [mailboxes](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/mailboxes.md)
- [hierarchy_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_rows.md)
- [hierarchy_row_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_row_id.md)
- [expect](../../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)
- [serialize_hierarchy_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/serialize_hierarchy_row.md)