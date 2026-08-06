---
type: Rust Function
title: query_rows_ignores_incomplete_windowed_content_table_rows
resource: crates/lpe-exchange/src/mapi/tables/tests.rs#L1576-L1672
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_content_windows
  - functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/emails
  - functions/crates/lpe-exchange/src/mapi/tables/tests/assert_response_contains_utf16
  - functions/crates/lpe-exchange/src/mapi/tables/controls/rop_query_position_response
---

# Signature

`fn query_rows_ignores_incomplete_windowed_content_table_rows()`

# Calls

- [remember_mapi_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity.md)
- [with_content_windows](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_content_windows.md)
- [rop_query_rows_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response.md)
- [emails](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/emails.md)
- [assert_response_contains_utf16](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/assert_response_contains_utf16.md)
- [rop_query_position_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/controls/rop_query_position_response.md)