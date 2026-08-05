---
type: Rust Method
title: long_term_id
resource: crates/lpe-exchange/src/mapi/rop/parse.rs#L684-L686
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/object_ids/append_id_from_long_term_id_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/public_folders/append_get_per_user_guid_response
  - functions/crates/lpe-exchange/src/mapi/rop/object_ids/rop_id_from_long_term_id_response
---

# Signature

`pub(in crate::mapi) fn long_term_id(&self) -> Option<&[u8]>`

# Calls

- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [append_id_from_long_term_id_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/object_ids/append_id_from_long_term_id_response.md)
- [append_get_per_user_guid_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/public_folders/append_get_per_user_guid_response.md)
- [rop_id_from_long_term_id_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/object_ids/rop_id_from_long_term_id_response.md)