---
type: Rust Function
title: mapi_over_http_contact_content_sync_exports_deletes
resource: crates/lpe-exchange/src/tests/mapi_over_http/contacts.rs#L1144-L1200
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/virtual_special_mailbox
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/store_mapi_sync_checkpoint
  - functions/crates/lpe-exchange/src/mapi/identity/legacy_migration_object_id
  - functions/crates/lpe-exchange/src/tests/outlook_content_sync_response_rops_for_store
  - functions/crates/lpe-exchange/src/tests/strict_content_sync_transfer_from_response
---

# Signature

`async fn mapi_over_http_contact_content_sync_exports_deletes()`

# Calls

- [virtual_special_mailbox](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/virtual_special_mailbox.md)
- [store_mapi_sync_checkpoint](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/store_mapi_sync_checkpoint.md)
- [legacy_migration_object_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/legacy_migration_object_id.md)
- [outlook_content_sync_response_rops_for_store](../../../../../../../functions/crates/lpe-exchange/src/tests/outlook_content_sync_response_rops_for_store.md)
- [strict_content_sync_transfer_from_response](../../../../../../../functions/crates/lpe-exchange/src/tests/strict_content_sync_transfer_from_response.md)