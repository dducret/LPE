---
type: Rust Function
title: bookmark_seek_does_not_mark_sparse_window_unknown_row_deleted
resource: crates/lpe-exchange/src/mapi/tables/tests.rs#L1818-L1917
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity
  - functions/crates/lpe-exchange/src/mapi/tables/tests/test_table_email
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_content_windows
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/tables/controls/rop_seek_row_bookmark_response
---

# Signature

`fn bookmark_seek_does_not_mark_sparse_window_unknown_row_deleted()`

# Calls

- [remember_mapi_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity.md)
- [test_table_email](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/test_table_email.md)
- [with_content_windows](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_content_windows.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [rop_seek_row_bookmark_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/controls/rop_seek_row_bookmark_response.md)