---
type: Rust Function
title: table_bookmark_state_mut
resource: crates/lpe-exchange/src/mapi/tables/state.rs#L94-L112
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/controls/rop_create_bookmark_response
  - functions/crates/lpe-exchange/src/mapi/tables/controls/rop_seek_row_bookmark_response
  - functions/crates/lpe-exchange/src/mapi/tables/controls/rop_free_bookmark_response
---

# Signature

`pub(in crate::mapi) fn table_bookmark_state_mut( object: &mut MapiObject, ) -> Option<(&mut usize, &mut HashMap<Vec<u8>, TableBookmark>, &mut u32)>`

# Called by

- [rop_create_bookmark_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/controls/rop_create_bookmark_response.md)
- [rop_seek_row_bookmark_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/controls/rop_seek_row_bookmark_response.md)
- [rop_free_bookmark_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/controls/rop_free_bookmark_response.md)