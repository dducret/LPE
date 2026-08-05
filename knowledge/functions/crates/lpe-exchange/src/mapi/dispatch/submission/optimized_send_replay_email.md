---
type: Rust Function
title: optimized_send_replay_email
resource: crates/lpe-exchange/src/mapi/dispatch/submission.rs#L38-L68
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_identities_by_object_ids
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/submission/append_submit_message_response
---

# Signature

`async fn optimized_send_replay_email<S>( store: &S, account_id: Uuid, outbox_mailbox_id: Uuid, target: &OptimizedSendTarget, ) -> Result<Option<JmapEmail>> where S: ExchangeStore,`

# Calls

- [fetch_mapi_identities_by_object_ids](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_identities_by_object_ids.md)

# Called by

- [append_submit_message_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/submission/append_submit_message_response.md)