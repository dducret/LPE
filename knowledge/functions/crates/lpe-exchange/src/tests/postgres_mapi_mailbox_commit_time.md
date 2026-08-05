---
type: Rust Function
title: postgres_mapi_mailbox_commit_time
resource: crates/lpe-exchange/src/tests/mod.rs#L187-L199
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_mailbox_content_commit_times
  - functions/crates/lpe-core/src/sieve/Parser/next
  called_by:
  - functions/crates/lpe-exchange/src/tests/postgres_mapi_mailbox_content_commit_time_tracks_canonical_mail_mutations
---

# Signature

`async fn postgres_mapi_mailbox_commit_time( storage: &Storage, account_id: Uuid, mailbox_id: Uuid, ) -> Option<u64>`

# Calls

- [fetch_mapi_mailbox_content_commit_times](../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_mailbox_content_commit_times.md)
- [next](../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)

# Called by

- [postgres_mapi_mailbox_content_commit_time_tracks_canonical_mail_mutations](../../../../../functions/crates/lpe-exchange/src/tests/postgres_mapi_mailbox_content_commit_time_tracks_canonical_mail_mutations.md)