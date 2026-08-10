---
type: Rust Function
title: mapi_over_http_virtual_calendar_content_sync_stores_virtual_checkpoint
resource: crates/lpe-exchange/src/tests/mapi_over_http/calendar.rs#L6722-L6833
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/FakeStore/collection
  - functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_identity_codec_for_test
  - functions/crates/lpe-exchange/src/tests/content_sync_response_rops
  - functions/crates/lpe-exchange/src/tests/strict_content_sync_transfer_from_response
  - functions/crates/lpe-exchange/src/tests/test_mapi_uuid_id
  - functions/crates/lpe-exchange/src/mapi/identity/with_current_mapi_identity_codec
  - functions/crates/lpe-exchange/src/mapi/identity/generated_message_search_key
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_rfc3339_utc
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_sync_checkpoint
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/virtual_special_mailbox
---

# Signature

`async fn mapi_over_http_virtual_calendar_content_sync_stores_virtual_checkpoint()`

# Calls

- [collection](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/collection.md)
- [load_mapi_identity_codec_for_test](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_identity_codec_for_test.md)
- [content_sync_response_rops](../../../../../../../functions/crates/lpe-exchange/src/tests/content_sync_response_rops.md)
- [strict_content_sync_transfer_from_response](../../../../../../../functions/crates/lpe-exchange/src/tests/strict_content_sync_transfer_from_response.md)
- [test_mapi_uuid_id](../../../../../../../functions/crates/lpe-exchange/src/tests/test_mapi_uuid_id.md)
- [with_current_mapi_identity_codec](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/with_current_mapi_identity_codec.md)
- [generated_message_search_key](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/generated_message_search_key.md)
- [filetime_from_rfc3339_utc](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_rfc3339_utc.md)
- [fetch_mapi_sync_checkpoint](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_sync_checkpoint.md)
- [virtual_special_mailbox](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/virtual_special_mailbox.md)