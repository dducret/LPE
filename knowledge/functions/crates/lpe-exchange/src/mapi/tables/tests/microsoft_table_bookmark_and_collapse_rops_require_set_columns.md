---
type: Rust Function
title: microsoft_table_bookmark_and_collapse_rops_require_set_columns
resource: crates/lpe-exchange/src/mapi/tables/tests.rs#L10118-L10232
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/tables/controls/rop_seek_row_bookmark_response
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/empty
  - functions/crates/lpe-exchange/src/mapi/tables/controls/rop_free_bookmark_response
  - functions/crates/lpe-exchange/src/mapi/tables/collapse/rop_get_collapse_state_response
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/write_u64
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/write_u16
  - functions/crates/lpe-exchange/src/mapi/tables/collapse/rop_set_collapse_state_response
---

# Signature

`fn microsoft_table_bookmark_and_collapse_rops_require_set_columns()`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [rop_seek_row_bookmark_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/controls/rop_seek_row_bookmark_response.md)
- [empty](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/calendar_identity/MapiMailStoreSnapshot/empty.md)
- [rop_free_bookmark_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/controls/rop_free_bookmark_response.md)
- [rop_get_collapse_state_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/collapse/rop_get_collapse_state_response.md)
- [write_u64](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/write_u64.md)
- [write_u16](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/write_u16.md)
- [rop_set_collapse_state_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/collapse/rop_set_collapse_state_response.md)