---
type: Rust Method
title: public_folder_probe_object_id
resource: crates/lpe-exchange/src/mapi/rop/parse.rs#L111-L121
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/public_folders/append_get_owning_servers_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/public_folders/append_public_folder_is_ghosted_response
---

# Signature

`pub(in crate::mapi) fn public_folder_probe_object_id(&self) -> Option<u64>`

# Calls

- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [append_get_owning_servers_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/public_folders/append_get_owning_servers_response.md)
- [append_public_folder_is_ghosted_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/public_folders/append_public_folder_is_ghosted_response.md)