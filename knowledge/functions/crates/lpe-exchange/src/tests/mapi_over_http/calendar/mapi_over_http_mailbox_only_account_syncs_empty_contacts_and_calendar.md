---
type: Rust Function
title: mapi_over_http_mailbox_only_account_syncs_empty_contacts_and_calendar
resource: crates/lpe-exchange/src/tests/mapi_over_http/calendar.rs#L8445-L8555
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/mapi_headers
  - functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str
  - functions/crates/lpe-exchange/src/tests/mapi_cookie_header
  - functions/crates/lpe-exchange/src/tests/append_rop_open_folder
  - functions/crates/lpe-exchange/src/tests/append_rop_outlook_hierarchy_sync_manifest_get_buffer
  - functions/crates/lpe-exchange/src/tests/execute_body
  - functions/crates/lpe-exchange/src/tests/response_rops_from_execute_response
  - functions/crates/lpe-exchange/src/tests/strict_hierarchy_sync_transfer_from_response
  - functions/crates/lpe-core/src/sieve/Parser/expect
  - functions/crates/lpe-exchange/src/tests/content_sync_response_rops_for_store
  - functions/crates/lpe-exchange/src/tests/strict_content_sync_transfer_from_response
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/virtual_special_mailbox
---

# Signature

`async fn mapi_over_http_mailbox_only_account_syncs_empty_contacts_and_calendar()`

# Calls

- [mapi_headers](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_headers.md)
- [from_str](../../../../../../../functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str.md)
- [mapi_cookie_header](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_cookie_header.md)
- [append_rop_open_folder](../../../../../../../functions/crates/lpe-exchange/src/tests/append_rop_open_folder.md)
- [append_rop_outlook_hierarchy_sync_manifest_get_buffer](../../../../../../../functions/crates/lpe-exchange/src/tests/append_rop_outlook_hierarchy_sync_manifest_get_buffer.md)
- [execute_body](../../../../../../../functions/crates/lpe-exchange/src/tests/execute_body.md)
- [response_rops_from_execute_response](../../../../../../../functions/crates/lpe-exchange/src/tests/response_rops_from_execute_response.md)
- [strict_hierarchy_sync_transfer_from_response](../../../../../../../functions/crates/lpe-exchange/src/tests/strict_hierarchy_sync_transfer_from_response.md)
- [expect](../../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)
- [content_sync_response_rops_for_store](../../../../../../../functions/crates/lpe-exchange/src/tests/content_sync_response_rops_for_store.md)
- [strict_content_sync_transfer_from_response](../../../../../../../functions/crates/lpe-exchange/src/tests/strict_content_sync_transfer_from_response.md)
- [virtual_special_mailbox](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/virtual_special_mailbox.md)