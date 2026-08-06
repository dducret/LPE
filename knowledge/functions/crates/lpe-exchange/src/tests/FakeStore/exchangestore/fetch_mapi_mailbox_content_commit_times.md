---
type: Rust Method
title: fetch_mapi_mailbox_content_commit_times
resource: crates/lpe-exchange/src/tests/mod.rs#L9646-L9665
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_store_for_access_plan
  - functions/crates/lpe-exchange/src/mapi_store/T/mapistore/load_mapi_mail_store
  - functions/crates/lpe-exchange/src/tests/postgres_mapi_mailbox_commit_time
---

# Signature

`fn fetch_mapi_mailbox_content_commit_times<'a>( &'a self, _account_id: Uuid, mailbox_ids: &'a [Uuid], ) -> StoreFuture<'a, Vec<MapiMailboxContentCommitTime>>`

# Called by

- [load_mapi_store_for_access_plan](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_store_for_access_plan.md)
- [load_mapi_mail_store](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/T/mapistore/load_mapi_mail_store.md)
- [postgres_mapi_mailbox_commit_time](../../../../../../../functions/crates/lpe-exchange/src/tests/postgres_mapi_mailbox_commit_time.md)