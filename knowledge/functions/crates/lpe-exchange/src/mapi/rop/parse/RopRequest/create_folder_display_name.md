---
type: Rust Method
title: create_folder_display_name
resource: crates/lpe-exchange/src/mapi/rop/parse.rs#L814-L816
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/read_u16_prefixed_string
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/folder_create/append_create_folder_response
---

# Signature

`pub(in crate::mapi) fn create_folder_display_name(&self) -> String`

# Calls

- [read_u16_prefixed_string](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/read_u16_prefixed_string.md)

# Called by

- [append_create_folder_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folder_create/append_create_folder_response.md)