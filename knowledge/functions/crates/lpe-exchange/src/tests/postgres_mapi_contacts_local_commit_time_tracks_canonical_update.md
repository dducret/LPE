---
type: Rust Function
title: postgres_mapi_contacts_local_commit_time_tracks_canonical_update
resource: crates/lpe-exchange/src/tests/mod.rs#L496-L563
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/postgres_mapi_calendar_fixture
  - functions/crates/lpe-exchange/src/mapi_store/T/mapistore/load_mapi_mail_store
  - functions/crates/lpe-exchange/src/mapi_store/folder_commit_time/MapiMailStoreSnapshot/folder_local_commit_time_max
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/mailboxes
  - functions/crates/lpe-core/src/sieve/Parser/expect
---

# Signature

`async fn postgres_mapi_contacts_local_commit_time_tracks_canonical_update()`

# Calls

- [postgres_mapi_calendar_fixture](../../../../../functions/crates/lpe-exchange/src/tests/postgres_mapi_calendar_fixture.md)
- [load_mapi_mail_store](../../../../../functions/crates/lpe-exchange/src/mapi_store/T/mapistore/load_mapi_mail_store.md)
- [folder_local_commit_time_max](../../../../../functions/crates/lpe-exchange/src/mapi_store/folder_commit_time/MapiMailStoreSnapshot/folder_local_commit_time_max.md)
- [mailboxes](../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/mailboxes.md)
- [expect](../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)