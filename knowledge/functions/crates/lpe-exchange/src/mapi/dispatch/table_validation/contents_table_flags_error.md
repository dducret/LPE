---
type: Rust Function
title: contents_table_flags_error
resource: crates/lpe-exchange/src/mapi/dispatch/table_validation.rs#L101-L121
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_open/append_open_table_response
---

# Signature

`pub(in crate::mapi::dispatch) fn contents_table_flags_error( flags: u8, folder_id: u64, is_public_folder: bool, ) -> Option<u32>`

# Called by

- [append_open_table_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_open/append_open_table_response.md)