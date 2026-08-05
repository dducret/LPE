---
type: Rust Method
title: long_term_source_object_id
resource: crates/lpe-exchange/src/mapi/rop/object_ids.rs#L44-L52
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/rop/object_ids/stale_special_folder_object_id_from_short_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/object_ids/append_long_term_id_from_id_response
  - functions/crates/lpe-exchange/src/mapi/rop/object_ids/rop_long_term_id_from_id_response
  - functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/extend_access_plan_for_request
---

# Signature

`pub(in crate::mapi) fn long_term_source_object_id(&self) -> Option<u64>`

# Calls

- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [stale_special_folder_object_id_from_short_id](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/object_ids/stale_special_folder_object_id_from_short_id.md)

# Called by

- [append_long_term_id_from_id_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/object_ids/append_long_term_id_from_id_response.md)
- [rop_long_term_id_from_id_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/object_ids/rop_long_term_id_from_id_response.md)
- [extend_access_plan_for_request](../../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/extend_access_plan_for_request.md)