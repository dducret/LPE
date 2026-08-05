---
type: Rust Function
title: inbox_associated_find_row_followup_uses_the_original_rowset
resource: crates/lpe-exchange/src/mapi/tables/tests.rs#L6376-L6511
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/empty
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_associated_configs
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/write_utf16z
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response
  - functions/crates/lpe-exchange/src/mapi/tables/tests/assert_response_contains_utf16
  - functions/crates/lpe-exchange/src/mapi/tables/controls/rop_seek_row_response
  - functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response
---

# Signature

`fn inbox_associated_find_row_followup_uses_the_original_rowset()`

# Calls

- [remember_mapi_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity.md)
- [empty](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/empty.md)
- [with_associated_configs](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_associated_configs.md)
- [write_utf16z](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/write_utf16z.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [rop_find_row_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response.md)
- [assert_response_contains_utf16](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/assert_response_contains_utf16.md)
- [rop_seek_row_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/controls/rop_seek_row_response.md)
- [rop_query_rows_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response.md)