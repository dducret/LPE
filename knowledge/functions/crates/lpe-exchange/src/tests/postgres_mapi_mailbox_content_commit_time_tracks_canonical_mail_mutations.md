---
type: Rust Function
title: postgres_mapi_mailbox_content_commit_time_tracks_canonical_mail_mutations
resource: crates/lpe-exchange/src/tests/mod.rs#L332-L492
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/postgres_mapi_calendar_fixture
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/ensure_jmap_system_mailboxes
  - functions/crates/lpe-exchange/src/tests/postgres_mapi_audit
  - functions/crates/lpe-exchange/src/tests/postgres_mapi_mailbox_commit_time
---

# Signature

`async fn postgres_mapi_mailbox_content_commit_time_tracks_canonical_mail_mutations()`

# Calls

- [postgres_mapi_calendar_fixture](../../../../../functions/crates/lpe-exchange/src/tests/postgres_mapi_calendar_fixture.md)
- [ensure_jmap_system_mailboxes](../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/ensure_jmap_system_mailboxes.md)
- [postgres_mapi_audit](../../../../../functions/crates/lpe-exchange/src/tests/postgres_mapi_audit.md)
- [postgres_mapi_mailbox_commit_time](../../../../../functions/crates/lpe-exchange/src/tests/postgres_mapi_mailbox_commit_time.md)