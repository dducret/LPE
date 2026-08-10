---
type: Rust Function
title: snapshot_resolves_tracked_mail_processing_by_advertised_folder_id
resource: crates/lpe-exchange/src/mapi_store/tests.rs#L2734-L2769
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_search_folder_definitions
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/search_folder_definition_for_folder_id
  - functions/crates/lpe-core/src/sieve/Parser/expect
---

# Signature

`fn snapshot_resolves_tracked_mail_processing_by_advertised_folder_id()`

# Calls

- [with_search_folder_definitions](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_search_folder_definitions.md)
- [search_folder_definition_for_folder_id](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/search_folder_definition_for_folder_id.md)
- [expect](../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)