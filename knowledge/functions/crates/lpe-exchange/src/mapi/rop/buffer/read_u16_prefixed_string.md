---
type: Rust Function
title: read_u16_prefixed_string
resource: crates/lpe-exchange/src/mapi/rop/buffer.rs#L105-L110
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/create_folder_display_name
---

# Signature

`pub(in crate::mapi) fn read_u16_prefixed_string(bytes: &[u8], offset: usize) -> Option<String>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [create_folder_display_name](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/create_folder_display_name.md)