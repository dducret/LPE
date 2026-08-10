---
type: Rust Function
title: mapi_over_http_contacts_search_content_sync_uses_search_folder_parent
resource: crates/lpe-exchange/src/tests/mapi_over_http/contacts.rs#L1428-L1509
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/FakeStore/collection
  - functions/crates/lpe-exchange/src/tests/content_sync_response_rops_for_store
  - functions/crates/lpe-exchange/src/tests/strict_content_sync_transfer_from_response
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_sync_checkpoint
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/virtual_special_mailbox
  - functions/crates/lpe-core/src/sieve/Parser/expect
---

# Signature

`async fn mapi_over_http_contacts_search_content_sync_uses_search_folder_parent()`

# Calls

- [collection](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/collection.md)
- [content_sync_response_rops_for_store](../../../../../../../functions/crates/lpe-exchange/src/tests/content_sync_response_rops_for_store.md)
- [strict_content_sync_transfer_from_response](../../../../../../../functions/crates/lpe-exchange/src/tests/strict_content_sync_transfer_from_response.md)
- [fetch_mapi_sync_checkpoint](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_sync_checkpoint.md)
- [virtual_special_mailbox](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/virtual_special_mailbox.md)
- [expect](../../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)