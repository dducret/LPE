---
type: Rust Function
title: inbox_default_named_view_is_materialized_for_the_advertised_entry_id
resource: crates/lpe-exchange/src/mapi_store/tests.rs#L1430-L1451
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/empty
  - functions/crates/lpe-exchange/src/mapi_store/associated_config/outlook_default_folder_named_view_id
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/default_folder_named_view_message
  - functions/crates/lpe-core/src/sieve/Parser/expect
---

# Signature

`fn inbox_default_named_view_is_materialized_for_the_advertised_entry_id()`

# Calls

- [empty](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/empty.md)
- [outlook_default_folder_named_view_id](../../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/outlook_default_folder_named_view_id.md)
- [default_folder_named_view_message](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/default_folder_named_view_message.md)
- [expect](../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)