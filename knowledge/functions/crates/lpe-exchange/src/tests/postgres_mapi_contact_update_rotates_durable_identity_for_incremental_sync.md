---
type: Rust Function
title: postgres_mapi_contact_update_rotates_durable_identity_for_incremental_sync
resource: crates/lpe-exchange/src/tests/mod.rs#L566-L631
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/postgres_mapi_calendar_fixture
  - functions/crates/lpe-exchange/src/mapi_store/T/mapistore/load_mapi_mail_store
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/contacts_for_folder
  - functions/crates/lpe-core/src/sieve/Parser/expect
---

# Signature

`async fn postgres_mapi_contact_update_rotates_durable_identity_for_incremental_sync()`

# Calls

- [postgres_mapi_calendar_fixture](../../../../../functions/crates/lpe-exchange/src/tests/postgres_mapi_calendar_fixture.md)
- [load_mapi_mail_store](../../../../../functions/crates/lpe-exchange/src/mapi_store/T/mapistore/load_mapi_mail_store.md)
- [contacts_for_folder](../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/contacts_for_folder.md)
- [expect](../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)