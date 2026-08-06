---
type: Rust Function
title: snapshot_carries_persisted_search_folder_definitions
resource: crates/lpe-exchange/src/mapi_store/tests.rs#L2684-L2728
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_search_folder_definitions
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/search_folder_definition_for_role
  - functions/crates/lpe-core/src/sieve/Parser/expect
---

# Signature

`fn snapshot_carries_persisted_search_folder_definitions()`

# Calls

- [remember_mapi_identity](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity.md)
- [with_search_folder_definitions](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_search_folder_definitions.md)
- [search_folder_definition_for_role](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/search_folder_definition_for_role.md)
- [expect](../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)