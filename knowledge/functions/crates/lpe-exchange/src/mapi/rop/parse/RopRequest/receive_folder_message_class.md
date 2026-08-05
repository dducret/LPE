---
type: Rust Method
title: receive_folder_message_class
resource: crates/lpe-exchange/src/mapi/rop/parse.rs#L656-L659
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/append_get_receive_folder_response
---

# Signature

`pub(in crate::mapi) fn receive_folder_message_class(&self) -> Option<&str>`

# Called by

- [append_get_receive_folder_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/append_get_receive_folder_response.md)