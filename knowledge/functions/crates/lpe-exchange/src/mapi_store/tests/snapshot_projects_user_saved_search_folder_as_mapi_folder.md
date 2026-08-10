---
type: Rust Function
title: snapshot_projects_user_saved_search_folder_as_mapi_folder
resource: crates/lpe-exchange/src/mapi_store/tests.rs#L2772-L2809
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_search_folder_definitions
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/folders
  - functions/crates/lpe-core/src/sieve/Parser/expect
---

# Signature

`fn snapshot_projects_user_saved_search_folder_as_mapi_folder()`

# Calls

- [remember_mapi_identity](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity.md)
- [with_search_folder_definitions](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_search_folder_definitions.md)
- [folders](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/folders.md)
- [expect](../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)