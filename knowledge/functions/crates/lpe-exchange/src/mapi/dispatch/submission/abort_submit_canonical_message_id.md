---
type: Rust Function
title: abort_submit_canonical_message_id
resource: crates/lpe-exchange/src/mapi/dispatch/submission.rs#L245-L266
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/submission/abort_submit_source_is_sent
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_identities_by_object_ids
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/submission/append_abort_submit_response
---

# Signature

`pub(super) async fn abort_submit_canonical_message_id<S>( store: &S, account_id: Uuid, folder_id: u64, message_id: u64, mailboxes: &[JmapMailbox], emails: &[JmapEmail], ) -> Option<Uuid> where S: ExchangeStore,`

# Calls

- [abort_submit_source_is_sent](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/submission/abort_submit_source_is_sent.md)
- [fetch_mapi_identities_by_object_ids](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_identities_by_object_ids.md)

# Called by

- [append_abort_submit_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/submission/append_abort_submit_response.md)