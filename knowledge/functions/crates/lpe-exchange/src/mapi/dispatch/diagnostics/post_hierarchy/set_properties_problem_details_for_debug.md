---
type: Rust Function
title: set_properties_problem_details_for_debug
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/post_hierarchy.rs#L309-L336
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_u16
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/post_hierarchy/post_hierarchy_setprops_contract
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/post_hierarchy/log_set_properties_default_folder_response_debug
---

# Signature

`fn set_properties_problem_details_for_debug(response: &[u8]) -> String`

# Calls

- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [read_u16](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_u16.md)
- [push](../../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [post_hierarchy_setprops_contract](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/post_hierarchy/post_hierarchy_setprops_contract.md)
- [log_set_properties_default_folder_response_debug](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/post_hierarchy/log_set_properties_default_folder_response_debug.md)