---
type: Rust Function
title: sync_attachment_facts_for_with_embedded_content
resource: crates/lpe-exchange/src/mapi/dispatch/attachments.rs#L1212-L1234
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/sync/sync_attachment_facts_for
  - functions/crates/lpe-exchange/src/mapi_mailstore/attachment_sync_fact_is_embedded_message
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_attachment_content
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_configure/append_synchronization_configure_response
---

# Signature

`pub(super) async fn sync_attachment_facts_for_with_embedded_content<S: ExchangeStore>( store: &S, account_id: Uuid, folder_id: u64, emails: &[JmapEmail], snapshot: &MapiMailStoreSnapshot, ) -> Vec<mapi_mailstore::MessageAttachmentSyncFacts>`

# Calls

- [sync_attachment_facts_for](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/sync_attachment_facts_for.md)
- [attachment_sync_fact_is_embedded_message](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/attachment_sync_fact_is_embedded_message.md)
- [fetch_attachment_content](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_attachment_content.md)

# Called by

- [append_synchronization_configure_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_configure/append_synchronization_configure_response.md)