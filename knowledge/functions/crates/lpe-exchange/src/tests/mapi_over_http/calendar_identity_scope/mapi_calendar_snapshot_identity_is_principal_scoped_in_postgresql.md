---
type: Rust Function
title: mapi_calendar_snapshot_identity_is_principal_scoped_in_postgresql
resource: crates/lpe-exchange/src/tests/mapi_over_http/calendar_identity_scope.rs#L58-L192
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/postgres_mapi_calendar_fixture
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar_identity_scope/insert_calendar_identity_account
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar_identity_scope/scoped_identity_event_input
  - functions/crates/lpe-exchange/src/mapi_store/T/mapistore/load_mapi_mail_store
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/collaboration_folders
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/events_for_folder
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_or_allocate_mapi_identities
  - functions/crates/lpe-core/src/sieve/Parser/next
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/event_for_id
---

# Signature

`async fn mapi_calendar_snapshot_identity_is_principal_scoped_in_postgresql() -> anyhow::Result<()>`

# Calls

- [postgres_mapi_calendar_fixture](../../../../../../../functions/crates/lpe-exchange/src/tests/postgres_mapi_calendar_fixture.md)
- [insert_calendar_identity_account](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar_identity_scope/insert_calendar_identity_account.md)
- [scoped_identity_event_input](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar_identity_scope/scoped_identity_event_input.md)
- [load_mapi_mail_store](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/T/mapistore/load_mapi_mail_store.md)
- [collaboration_folders](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/collaboration_folders.md)
- [events_for_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/events_for_folder.md)
- [fetch_or_allocate_mapi_identities](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_or_allocate_mapi_identities.md)
- [next](../../../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)
- [event_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/event_for_id.md)