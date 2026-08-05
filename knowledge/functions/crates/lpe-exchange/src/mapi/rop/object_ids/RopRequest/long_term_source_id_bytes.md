---
type: Rust Method
title: long_term_source_id_bytes
resource: crates/lpe-exchange/src/mapi/rop/object_ids.rs#L54-L56
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/object_ids/append_long_term_id_from_id_response
---

# Signature

`pub(in crate::mapi) fn long_term_source_id_bytes(&self) -> Option<&[u8]>`

# Calls

- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [append_long_term_id_from_id_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/object_ids/append_long_term_id_from_id_response.md)